"""Exercise Bronze restart recovery only in unique test prefixes.

Run inside the Spark image. It creates a unique Kafka topic and S3 output/checkpoint
prefix, interrupts one bounded Bronze process, resumes it with the same checkpoint,
and verifies that the isolated Delta output contains the published records.
"""
import json
import os
import signal
import subprocess
import time
import uuid

from kafka import KafkaProducer
from pyspark.sql import SparkSession


def main():
    run = uuid.uuid4().hex[:10]
    topic = f"aetherflow-recovery-test-{run}"
    output = f"s3a://aetherflow-data/recovery-tests/{run}/bronze"
    checkpoint = f"s3a://aetherflow-data/recovery-tests/{run}/checkpoint"
    command = ["/opt/spark/bin/spark-submit", "--master", "spark://spark-master:7077", "/opt/aetherflow/scripts/bronze_stream.py", "--topic", topic, "--output", output, "--checkpoint", checkpoint]
    producer = KafkaProducer(bootstrap_servers="kafka:29092", value_serializer=lambda value: json.dumps(value).encode())
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
    spark = (SparkSession.builder.master("local[2]").appName("recovery-verification")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.endpoint", os.environ.get("S3_ENDPOINT", "http://seaweedfs:8333"))
        .config("spark.hadoop.fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"])
        .config("spark.hadoop.fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])
        .config("spark.hadoop.fs.s3a.path.style.access", "true").config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem").getOrCreate())
    try:
        count = spark.read.format("delta").load(output).count()
    finally:
        spark.stop()
    if count < 8:
        raise AssertionError(f"expected at least 8 isolated records after restart, got {count}")
    print(f"RECOVERY_VERIFIED topic={topic} output={output} checkpoint={checkpoint} rows={count} first_exit={first.returncode} second_exit={second.returncode}")


if __name__ == "__main__":
    main()
