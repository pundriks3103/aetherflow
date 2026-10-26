"""Exercise Bronze restart recovery only in unique test prefixes.

Run inside the Spark image. It creates a unique Kafka topic and S3 output/checkpoint
prefix, interrupts one bounded Bronze process, resumes it with the same checkpoint,
and verifies that the isolated Delta output contains the published records.
"""
import json
import os
import signal
import sys
import subprocess
import tempfile
import time
import uuid
from pathlib import Path

sys.path.insert(0, "/opt/aetherflow/scripts")
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

from kafka import KafkaConsumer, KafkaProducer, TopicPartition
from pyspark.sql import SparkSession, functions as F
from aetherflow_config import configure_s3a, load_config
from metrics_calculations import partition_offset_metrics, timestamp_freshness
from silver_transform import transform_bronze


EXCHANGE_EVENT_TIME = "2026-01-01T00:00:00Z"
METRICS_AS_OF = "2026-01-01T00:01:00Z"


def trade_event(index):
    symbol = "btcusdt"
    return {
        "source": "binance",
        "event_type": "crypto_trade",
        "event_id": f"binance:{symbol}:recovery-{index}",
        "trade_id": f"recovery-{index}",
        "symbol": symbol,
        "price": "100.25",
        "quantity": "0.01000000",
        "event_time": EXCHANGE_EVENT_TIME,
        "exchange_event_time": EXCHANGE_EVENT_TIME,
        "exchange_event_time_ms": 1767225600000,
        "trade_time_ms": 1767225600000,
        "is_buyer_maker": False,
    }


def main():
    config = load_config(context="compose")
    run = uuid.uuid4().hex[:10]
    topic = f"aetherflow-recovery-test-{run}"
    root = config["tests"]["recovery_root"].rstrip("/")
    output = f"{root}/{run}/bronze"
    checkpoint = f"{root}/{run}/checkpoint"
    command = ["/opt/spark/bin/spark-submit", "--master", config["spark"]["master"], "/opt/aetherflow/scripts/bronze_stream.py", "--topic", topic, "--bootstrap-servers", config["kafka"]["bootstrap_servers"], "--output", output, "--checkpoint", checkpoint]
    producer = KafkaProducer(bootstrap_servers=config["kafka"]["bootstrap_servers"], value_serializer=lambda value: json.dumps(value).encode())
    for i in range(4):
        producer.send(topic, trade_event(i))
    producer.flush()
    first_log_file = tempfile.NamedTemporaryFile(prefix="aetherflow-bronze-first-", suffix=".log", delete=False)
    first_log_path = first_log_file.name
    first = subprocess.Popen(command, env=os.environ.copy(), stdout=first_log_file, stderr=subprocess.STDOUT, start_new_session=True)
    time.sleep(25)
    os.killpg(first.pid, signal.SIGTERM)
    first.wait(timeout=30)
    first_log_file.close()
    for i in range(4, 8):
        producer.send(topic, trade_event(i))
    producer.flush()
    producer.close()
    second_log_file = tempfile.NamedTemporaryFile(prefix="aetherflow-bronze-second-", suffix=".log", delete=False)
    second_log_path = second_log_file.name
    second = subprocess.Popen(command, env=os.environ.copy(), stdout=second_log_file, stderr=subprocess.STDOUT, start_new_session=True)
    time.sleep(30)
    os.killpg(second.pid, signal.SIGTERM)
    second.wait(timeout=30)
    second_log_file.close()
    first_log_text = Path(first_log_path).read_text(errors="replace")
    second_log_text = Path(second_log_path).read_text(errors="replace")
    first_progress = first_log_text.count("BRONZE_PROGRESS ")
    second_progress = second_log_text.count("BRONZE_PROGRESS ")
    if first_progress < 1 or second_progress < 1:
        first_tail = "\n".join(first_log_text.splitlines()[-12:])
        second_tail = "\n".join(second_log_text.splitlines()[-12:])
        raise AssertionError(f"expected Bronze progress logs before and after test restart, got first={first_progress} second={second_progress}; first_exit={first.returncode} second_exit={second.returncode}\nfirst log tail:\n{first_tail}\nsecond log tail:\n{second_tail}")
    Path(first_log_path).unlink(missing_ok=True)
    Path(second_log_path).unlink(missing_ok=True)
    spark_builder = (SparkSession.builder.master("local[2]").appName("recovery-verification")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"))
    spark = configure_s3a(spark_builder, config).getOrCreate()
    try:
        recovered = spark.read.format("delta").load(output)
        count = recovered.count()
        config = load_config(context="compose")
        _, _, valid = transform_bronze(recovered, config)
        latest_event = (valid.where("source = 'binance' AND event_type = 'crypto_trade'")
            .agg(F.max("exchange_event_time").alias("latest"))
            .first()["latest"])
        event_freshness = timestamp_freshness(latest_event, METRICS_AS_OF)
        bronze_offsets = {
            int(row["partition"]): int(row["max_offset"])
            for row in recovered.groupBy("partition").agg(F.max("offset").alias("max_offset")).collect()
        }
    finally:
        spark.stop()
    if count != 8:
        raise AssertionError(f"expected exactly 8 isolated records after restart, got {count}")
    consumer = KafkaConsumer(
        bootstrap_servers=config["kafka"]["bootstrap_servers"],
        enable_auto_commit=False,
        request_timeout_ms=10000,
        api_version_auto_timeout_ms=5000,
    )
    try:
        partition_ids = consumer.partitions_for_topic(topic)
        topic_partitions = [TopicPartition(topic, partition) for partition in sorted(partition_ids or [])]
        beginnings = consumer.beginning_offsets(topic_partitions)
        endings = consumer.end_offsets(topic_partitions)
    finally:
        consumer.close()
    if len(topic_partitions) != 1:
        raise AssertionError(f"expected one isolated test partition, got {len(topic_partitions)}")
    tp = topic_partitions[0]
    offsets = partition_offset_metrics(beginnings[tp], endings[tp], bronze_offsets.get(tp.partition))
    if offsets["offset_position_lag"] != 0 or offsets["retention_gap"]:
        raise AssertionError(f"isolated recovered offsets did not reach Kafka end: {offsets}")
    if event_freshness["status"] != "available" or event_freshness["age_seconds"] != 60:
        raise AssertionError(f"exchange-event freshness calculation mismatch: {event_freshness}")
    print(f"RECOVERY_VERIFIED topic={topic} output={output} checkpoint={checkpoint} rows={count} first_exit={first.returncode} second_exit={second.returncode}")
    print(f"BRONZE_PROGRESS_INTEGRATION_VERIFIED first_batches={first_progress} resumed_batches={second_progress}")
    print(f"METRICS_INTEGRATION_VERIFIED partition={tp.partition} beginning={beginnings[tp]} end_exclusive={endings[tp]} bronze_max={bronze_offsets.get(tp.partition)} offset_position_lag={offsets['offset_position_lag']} retention_gap={offsets['retention_gap']} exchange_event_age_seconds={event_freshness['age_seconds']}")


if __name__ == "__main__":
    main()
