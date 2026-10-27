#!/usr/bin/env python3
"""Stream Kafka records into a Bronze Delta table on SeaweedFS."""

import argparse
import json
import logging
import os
import time

from pyspark.sql import SparkSession, functions as F

from aetherflow_config import configure_s3a, load_config


LOG = logging.getLogger("aetherflow.bronze")


def _offset_json(value):
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError:
            pass
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def format_progress(progress):
    """Format one Spark streaming progress object as a compact log record."""
    source_offsets = [
        f"start={_offset_json(source.get('startOffset'))};end={_offset_json(source.get('endOffset'))}"
        for source in progress.get("sources", [])
    ]
    durations = progress.get("durationMs", {})
    return (
        f"BRONZE_PROGRESS batch_id={progress.get('batchId')} "
        f"input_rows={progress.get('numInputRows')} "
        f"input_rows_per_second={progress.get('inputRowsPerSecond')} "
        f"processed_rows_per_second={progress.get('processedRowsPerSecond')} "
        f"trigger_execution_ms={durations.get('triggerExecution')} "
        f"source_offsets={';'.join(source_offsets) or 'unavailable'}"
    )


def main():
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(message)s", force=True)
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
        
        LOG.info("Streaming topic=%s output=%s checkpoint=%s trigger_seconds=%s", args.topic, args.output, args.checkpoint, args.trigger_seconds)
        deadline = time.monotonic() + args.run_seconds if args.run_seconds else None
        last_batch_id = None
        
        while query.isActive:
            wait_seconds = min(1, args.trigger_seconds)
            if deadline is not None:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                wait_seconds = min(wait_seconds, max(0.1, remaining))
            terminated = query.awaitTermination(wait_seconds)
            for progress in query.recentProgress:
                batch_id = progress.get("batchId")
                if last_batch_id is None or batch_id > last_batch_id:
                    LOG.info(format_progress(progress))
                    last_batch_id = batch_id
            if terminated:
                break
        if query.isActive:
            query.stop()
            query.awaitTermination()
        if query.exception() is not None:
            raise RuntimeError(f"Streaming query failed: {query.exception()}")
        LOG.info("Stream stopped topic=%s latest_batch_id=%s", args.topic, last_batch_id)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
