#!/usr/bin/env python3
"""Stream Kafka records into a Bronze Delta table on SeaweedFS."""

import argparse
import os

from pyspark.sql import SparkSession, functions as F


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--topic", default="aetherflow-events")
    parser.add_argument("--bootstrap-servers", default="kafka:29092")
    parser.add_argument("--output", default="s3a://aetherflow-data/bronze/events")
    parser.add_argument(
        "--checkpoint",
        default="s3a://aetherflow-checkpoints/bronze-events/",
    )
    parser.add_argument(
        "--run-seconds",
        type=int,
        default=0,
        help="Stop gracefully after this many seconds; 0 runs until interrupted.",
    )
    args = parser.parse_args()
    if args.run_seconds < 0:
        parser.error("--run-seconds cannot be negative")

    endpoint = os.environ.get("S3_ENDPOINT", "http://seaweedfs:8333")
    access_key = os.environ.get("AWS_ACCESS_KEY_ID")
    secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
    if not access_key or not secret_key:
        raise RuntimeError("AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY are missing")

    spark = (
        SparkSession.builder.appName("AetherFlow-Kafka-Bronze-Stream")
        .master("spark://spark-master:7077")
        .config("spark.cores.max", "2")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.endpoint", endpoint)
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        kafka_records = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", args.bootstrap_servers)
            .option("subscribe", args.topic)
            .option("startingOffsets", "earliest")
            .load()
        )
        bronze = kafka_records.select(
            F.col("key").cast("string").alias("key"),
            F.col("value").cast("string").alias("raw_event"),
            F.col("timestamp").alias("kafka_timestamp"),
            F.col("topic"),
            F.col("partition"),
            F.col("offset"),
        )

        query = (
            bronze.writeStream.format("delta")
            .outputMode("append")
            .option("checkpointLocation", args.checkpoint)
            .trigger(processingTime="10 seconds")
            .start(args.output)
        )
        print(f"Streaming topic {args.topic} to {args.output}; checkpoint={args.checkpoint}")
        timeout_ms = args.run_seconds * 1000 if args.run_seconds else None
        terminated = query.awaitTermination(timeout_ms)
        if not terminated:
            query.stop()
            query.awaitTermination()
        if query.exception() is not None:
            raise RuntimeError(f"Streaming query failed: {query.exception()}")
        print(f"Stream stopped; latest progress: {query.lastProgress}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
