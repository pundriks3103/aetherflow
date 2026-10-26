#!/usr/bin/env python3
"""Build a validated, deduplicated Silver Delta snapshot from Bronze."""

import argparse
import os

from pyspark.sql import SparkSession, Window, functions as F, types as T


EVENT_SCHEMA = T.StructType(
    [
        T.StructField("event_id", T.StringType()),
        T.StructField("event_time", T.StringType()),
        T.StructField("entity_id", T.StringType()),
        T.StructField("event_type", T.StringType()),
        T.StructField("value", T.LongType()),
    ]
)


def spark_session(master):
    endpoint = os.environ.get("S3_ENDPOINT", "http://seaweedfs:8333")
    access_key = os.environ.get("AWS_ACCESS_KEY_ID")
    secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
    if not access_key or not secret_key:
        raise RuntimeError("AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY are missing")

    return (
        SparkSession.builder.appName("AetherFlow-Silver-Transform")
        .master(master)
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


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--master", default="spark://spark-master:7077")
    parser.add_argument("--bronze", default="s3a://aetherflow-data/bronze/events")
    parser.add_argument("--silver", default="s3a://aetherflow-data/silver/events")
    parser.add_argument(
        "--quarantine",
        default="s3a://aetherflow-data/quarantine/silver-events",
    )
    args = parser.parse_args()

    spark = spark_session(args.master)
    spark.sparkContext.setLogLevel("WARN")
    try:
        bronze = spark.read.format("delta").load(args.bronze)
        input_count = bronze.count()

        parsed = bronze.withColumn("event", F.from_json("raw_event", EVENT_SCHEMA))
        normalized = parsed.select(
            "raw_event",
            "kafka_timestamp",
            "topic",
            "partition",
            "offset",
            F.col("event.event_id").alias("event_id"),
            F.col("event.event_time").alias("event_time_text"),
            F.to_timestamp("event.event_time").alias("event_time"),
            F.col("event.entity_id").alias("entity_id"),
            F.col("event.event_type").alias("event_type"),
            F.col("event.value").alias("value"),
        )

        reasons = F.concat_ws(
            ";",
            F.when(F.col("raw_event").isNull(), F.lit("missing_raw_event")),
            F.when(F.col("event_id").isNull() | (F.trim("event_id") == ""), F.lit("missing_event_id")),
            F.when(F.col("event_time").isNull(), F.lit("invalid_event_time")),
            F.when(F.col("entity_id").isNull() | (F.trim("entity_id") == ""), F.lit("missing_entity_id")),
            F.when(F.col("event_type").isNull() | (F.trim("event_type") == ""), F.lit("missing_event_type")),
            F.when(F.col("value").isNull(), F.lit("missing_value")),
        )
        classified = normalized.withColumn("reject_reason", reasons)
        invalid = classified.where(F.col("reject_reason") != "")
        valid = classified.where(F.col("reject_reason") == "")

        valid_count = valid.count()
        invalid_count = invalid.count()
        ranking = Window.partitionBy("event_id").orderBy(
            F.col("kafka_timestamp").desc_nulls_last(),
            F.col("partition").desc_nulls_last(),
            F.col("offset").desc_nulls_last(),
        )
        silver = (
            valid.withColumn("event_rank", F.row_number().over(ranking))
            .where(F.col("event_rank") == 1)
            .drop("event_rank", "event_time_text", "reject_reason", "raw_event")
        )
        silver_count = silver.count()
        duplicate_count = valid_count - silver_count

        silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(args.silver)
        invalid.select(
            "raw_event",
            "reject_reason",
            "kafka_timestamp",
            "topic",
            "partition",
            "offset",
        ).write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(args.quarantine)

        print(
            "SILVER_METRICS "
            f"input={input_count} valid={valid_count} invalid={invalid_count} "
            f"duplicates_removed={duplicate_count} output={silver_count}"
        )
        print(f"SILVER_PATH {args.silver}")
        print(f"QUARANTINE_PATH {args.quarantine}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
