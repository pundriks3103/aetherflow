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
import time
import uuid
from pathlib import Path

sys.path.insert(0, "/opt/aetherflow/scripts")
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

from kafka import KafkaProducer
from pyspark.sql import SparkSession
from aetherflow_config import configure_s3a, load_config


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
        producer.send(topic, {"event_id": f"recovery-{i}", "event_time": "2026-10-26T06:00:00Z", "entity_id": "recovery", "event_type": "test", "value": i})
    producer.flush()
    first = subprocess.Popen(command, env=os.environ.copy(), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True)
    time.sleep(25)
    os.killpg(first.pid, signal.SIGTERM)
    first.wait(timeout=30)
    for i in range(4, 8):
        producer.send(topic, {"event_id": f"recovery-{i}", "event_time": "2026-10-26T06:00:00Z", "entity_id": "recovery", "event_type": "test", "value": i})
    producer.flush()
    second = subprocess.Popen(command, env=os.environ.copy(), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True)
    time.sleep(30)
    os.killpg(second.pid, signal.SIGTERM)
    second.wait(timeout=30)
    spark_builder = (SparkSession.builder.master("local[2]").appName("recovery-verification")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"))
    spark = configure_s3a(spark_builder, config).getOrCreate()
    try:
        count = spark.read.format("delta").load(output).count()
    finally:
        spark.stop()
    if count < 8:
        raise AssertionError(f"expected at least 8 isolated records after restart, got {count}")
    print(f"RECOVERY_VERIFIED topic={topic} output={output} checkpoint={checkpoint} rows={count} first_exit={first.returncode} second_exit={second.returncode}")


if __name__ == "__main__":
    main()
