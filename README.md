# AetherFlow

A local Kafka, Spark, Delta Lake, SeaweedFS, and Airflow data platform.

## Current pipeline

Kafka events are continuously written by the `bronze-stream` Compose service to:

- Bronze Delta: `s3a://aetherflow-data/bronze/events`
- Bronze checkpoint: `s3a://aetherflow-checkpoints/bronze-events/`
- Silver Delta: `s3a://aetherflow-data/silver/events`
- Silver quarantine: `s3a://aetherflow-data/quarantine/silver-events`

The hourly `aetherflow_silver_transform` Airflow DAG launches the existing
Delta-enabled Spark image. It reads the full Bronze snapshot, validates the
known event schema, deduplicates legacy records by `event_id` and Binance trades
by `source:symbol:trade_id`, and deterministically overwrites Silver and
quarantine outputs. Binance prices and quantities are stored as
`DecimalType(38,18)`; this covers the observed eight fractional digits while
leaving headroom. Invalid records are retained in quarantine with semicolon-
delimited reasons. The task allows one five-second retry and limits active DAG
runs to one. Silver remains a full-snapshot job until an incremental merge and
checkpoint strategy is tested.

The Airflow scheduler uses the Docker provider to launch Spark. The scheduler
therefore needs access to the Docker socket. On this local Docker Desktop setup,
the default socket path is `/Users/pundriksharma/.docker/run/docker.sock`; set
`DOCKER_SOCKET_PATH` in `.env` when using a different path.

## Useful commands

```bash
make up
make bronze-stream
make bronze-logs
make silver-transform
make silver-logs
make events COUNT=10
make crypto-trades DURATION=30 SYMBOL=btcusdt
```

`make crypto-trades` connects to Binance's public Spot `@trade` stream and
publishes normalized live events to `aetherflow-events` without restarting any
Compose service. Prices and quantities remain decimal strings, and the original
Binance payload is retained inside each Kafka value.

Airflow is available at `http://localhost:8085`, Kafka UI at
`http://localhost:8086`, and the Spark master UI at `http://localhost:8080`.
