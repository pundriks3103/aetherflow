# AetherFlow

A local Kafka, Spark, Delta Lake, SeaweedFS, and Airflow data platform.

## Current pipeline

Kafka events are continuously written by `bronze-stream`; topic, storage paths,
checkpoint, decimal policy, schedule, and retry defaults live in
`config/aetherflow.yml`. The Kafka topic and production storage/checkpoint paths
retain their established values.

The hourly `aetherflow_silver_transform` Airflow DAG launches the existing
Delta-enabled Spark image. It reads the full Bronze snapshot, validates the
known event schema, deduplicates legacy records by `event_id` and Binance trades
by `source:symbol:trade_id`, and deterministically overwrites Silver and
quarantine outputs. Binance prices and quantities use the configured decimal
type, which covers the observed eight fractional digits while leaving headroom.
Invalid records are retained in quarantine with semicolon-
delimited reasons. The task allows one five-second retry and limits active DAG
runs to one. Silver remains a full-snapshot job until an incremental merge and
checkpoint strategy is tested.

`make crypto-analytics` rebuilds a separate hourly Delta snapshot from Silver
at `s3a://aetherflow-data/analytics/crypto-hourly`. Windows use the Binance
trade execution `event_time` in UTC and are start-inclusive/end-exclusive;
exchange notification time is not used for window assignment. Stable
`trade_identity` values are deduplicated before grouping, choosing the latest
available ingestion/Kafka provenance and deterministic payload tie-breakers.
The output includes symbol, window boundaries, distinct trade count, base
quantity volume, quote notional, VWAP, first/last price, and high/low price.
First/last use earliest/latest trade `event_time`; ties use ascending/descending
`trade_identity` respectively. Quote notional is the sum of exact price ×
quantity products, and VWAP is total quote notional ÷ total base quantity.

Price and quantity are cast exactly to Decimal(25,8) and Decimal(12,8), based
on observed Binance values using eight fractional digits. The product and
quote notional use Decimal(38,16); VWAP is quote notional divided by base
volume, rounded to Decimal(30,8). The chosen operands support prices below
10^17 and quantities below 10^4. The job rejects values that cannot round-trip
exactly through those types before writing. It recomputes the full snapshot
from Silver on each run, leaving Bronze, Silver, quarantine, and the Bronze
checkpoint untouched.

Example query from a Spark session:

```python
hourly = spark.read.format("delta").load("s3a://aetherflow-data/analytics/crypto-hourly")
hourly.createOrReplaceTempView("crypto_hourly")
spark.sql("""
  SELECT symbol, window_start, trade_count, base_volume, quote_notional,
         vwap, first_price, last_price, low_price, high_price
  FROM crypto_hourly
  ORDER BY symbol, window_start
""").show(truncate=False)
```

## Crypto Analytics dashboard

Start the read-only dashboard with:

```bash
make dashboard
```

Open [http://localhost:8501](http://localhost:8501). It reads the existing
hourly Delta snapshot and operational metrics; it does not trigger analytics
or Silver processing. The page refreshes every 60 seconds by default and also
has a manual refresh button. The latest recorded price is from the most recent
hourly aggregate window, not a live quote. Exchange event freshness uses the
Binance notification timestamp, with stale data called out after 15 minutes.
Kafka and pipeline probes refresh every five minutes to avoid repeatedly
scanning the full Bronze snapshot.

Silver logs a run identifier, duration, counts, maximum Bronze Kafka offset,
and quarantine counts by reason. The isolated recovery harness can be run from
the Spark container after copying `tests/bronze_recovery_harness.py`; it creates
unique Kafka and S3 prefixes and never uses the production Bronze checkpoint.

`make metrics` prints a read-only snapshot of per-partition Kafka beginning/end
offsets versus the highest offsets stored in Bronze, the latest valid Binance
exchange-event freshness in Bronze and Silver, the latest successful Silver
task end time from Airflow metadata, quarantine counts by reason, and the most
recent Bronze progress log when available. Offset lag is an offset-position
difference, not necessarily a count of unprocessed records. Unknown values
(such as a partition with no Bronze offset yet) are reported as unknown rather
than zero. Freshness uses exchange event time; Kafka record and ingestion
timestamps are not treated as Bronze commit time. The command does not write to
Kafka, Delta tables, checkpoints, or Airflow metadata.

The Airflow scheduler uses the Docker provider to launch Spark. The scheduler
therefore needs access to the Docker socket. On this local Docker Desktop setup,
the default socket path is `/Users/pundriksharma/.docker/run/docker.sock`; set
`DOCKER_SOCKET_PATH` in `.env` when using a different path.

Application configuration uses `AETHERFLOW_CONTEXT=host` for host tools
(`localhost` published ports) and `AETHERFLOW_CONTEXT=compose` for container
tools (Compose service DNS). Compose services set the latter explicitly. The
host-run generic event command delegates publishing to the Kafka container and
selects Compose context for that operation. An
optional `AETHERFLOW_CONFIG` selects another YAML file. CLI arguments override
environment values, which override YAML defaults. Legacy `KAFKA_TOPIC`,
`KAFKA_BOOTSTRAP_SERVERS`, `BINANCE_SYMBOL`, and `S3_ENDPOINT` environment
overrides remain supported; prefixed `AETHERFLOW_*` overrides are preferred.
Credentials remain environment-only. Install `requirements.txt` for host-run
Python tools; Spark images include the same YAML parser.

## Useful commands

```bash
make up
make bronze-stream
make bronze-logs
make silver-transform
make silver-logs
make metrics
make events COUNT=10
make crypto-trades DURATION=30 SYMBOL=btcusdt
```

`make crypto-trades` connects to Binance's public Spot `@trade` stream and
publishes normalized live events to the configured events topic without restarting any
Compose service. Prices and quantities remain decimal strings, and the original
Binance payload is retained inside each Kafka value.

Airflow is available at `http://localhost:8085`, Kafka UI at
`http://localhost:8086`, and the Spark master UI at `http://localhost:8080`.

`make test` builds a temporary Spark test container and runs the unit suite.
`make recovery-test` runs the Kafka/Spark restart harness with a unique topic
and unique S3 output/checkpoint prefixes; it also verifies known offset-lag and
exchange-event freshness calculations against that isolated input.
