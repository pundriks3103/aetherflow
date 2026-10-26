#!/usr/bin/env python3
"""Stream Kafka records into a Bronze Delta table on SeaweedFS."""

import argparse

from pyspark.sql import SparkSession, functions as F

from aetherflow_config import configure_s3a, load_config


def main():
    config = load_config()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--topic", default=config["kafka"]["topics"]["events"])
    parser.add_argument("--bootstrap-servers", default=config["kafka"]["bootstrap_servers"])
    parser.add_argument("--output", default=config["bronze"]["output"])
    parser.add_argument("--checkpoint", default=config["bronze"]["checkpoint"])
    parser.add_argument("--master", default=config["spark"]["master"])
    parser.add_argument("--trigger-seconds", type=int, default=config["bronze"]["trigger_seconds"])
    parser.add_argument("--run-seconds", type=int, default=0, help="Stop gracefully after this many seconds; 0 runs until interrupted.")
    args = parser.parse_args()
    if args.run_seconds < 0:
        parser.error("--run-seconds cannot be negative")
    if args.trigger_seconds < 1:
        parser.error("--trigger-seconds must be positive")

    builder = (SparkSession.builder.appName("AetherFlow-Kafka-Bronze-Stream").master(args.master)
        .config("spark.cores.max", str(config["spark"]["cores_max"]))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"))
    spark = configure_s3a(builder, config).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        kafka_records = (spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", args.bootstrap_servers)
            .option("subscribe", args.topic).option("startingOffsets", "earliest").load())
        bronze = kafka_records.select(
            F.col("key").cast("string").alias("key"), F.col("value").cast("string").alias("raw_event"),
            F.col("timestamp").alias("kafka_timestamp"), F.col("topic"), F.col("partition"), F.col("offset"))
        query = (bronze.writeStream.format("delta").outputMode("append")
            .option("checkpointLocation", args.checkpoint)
            .trigger(processingTime=f"{args.trigger_seconds} seconds").start(args.output))
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
