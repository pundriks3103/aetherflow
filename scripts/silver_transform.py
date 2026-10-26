#!/usr/bin/env python3
"""Build a validated, deduplicated Silver Delta snapshot from Bronze."""

import argparse
import logging
import os
import time
import uuid

from pyspark.sql import SparkSession, Window, functions as F, types as T

from aetherflow_config import configure_s3a, load_config


LOG = logging.getLogger("aetherflow.silver")


DECIMAL_TEXT = r"^-?[0-9]+(?:\.[0-9]+)?$"
EVENT_SCHEMA = T.StructType([T.StructField(n, t) for n, t in [
    ("event_id", T.StringType()), ("event_time", T.StringType()), ("entity_id", T.StringType()),
    ("event_type", T.StringType()), ("value", T.LongType()), ("source", T.StringType()),
    ("trade_id", T.StringType()), ("symbol", T.StringType()), ("price", T.StringType()),
    ("quantity", T.StringType()), ("exchange_event_time", T.StringType()),
    ("exchange_event_time_ms", T.LongType()), ("trade_time_ms", T.LongType()),
    ("is_buyer_maker", T.BooleanType()),
]])
SILVER_COLUMNS = [
    "kafka_timestamp", "topic", "partition", "offset", "event_id", "event_time",
    "entity_id", "event_type", "value", "ingestion_timestamp", "source", "trade_id",
    "symbol", "exchange_event_time", "exchange_event_time_ms", "trade_time_ms",
    "is_buyer_maker", "price", "quantity", "trade_identity",
]


def spark_session(master, config):
    builder = (
        SparkSession.builder.appName("AetherFlow-Silver-Transform").master(master)
        .config("spark.cores.max", str(config["spark"]["cores_max"]))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    return configure_s3a(builder, config).getOrCreate()


def _decimal_reason(text_col, value_col, prefix, policy):
    """Return independent, null-safe validation expressions for a decimal string."""
    has_value = text_col.isNotNull()
    significant_integer = F.regexp_replace(F.regexp_extract(text_col, r"^-?([0-9]+)", 1), r"^0+", "")
    integer_digits = F.when(F.length(significant_integer) == 0, F.lit(1)).otherwise(F.length(significant_integer))
    scale = F.when(F.instr(text_col, ".") > 0, F.length(F.substring_index(text_col, ".", -1))).otherwise(F.lit(0))
    decimal_text = DECIMAL_TEXT if policy["allow_scientific_notation"] is False else r"^-?[0-9]+(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?$"
    non_positive = F.when(value_col <= 0, F.lit(True)).otherwise(F.lit(False)) if policy["positive_only"] else F.lit(False)
    return [
        F.when(~has_value, f"missing_{prefix}"),
        F.when(has_value & ~text_col.rlike(decimal_text), f"malformed_{prefix}"),
        F.when(has_value & text_col.rlike(decimal_text) & (scale > policy["scale"]), f"excessive_scale_{prefix}"),
        F.when(has_value & text_col.rlike(decimal_text) & (integer_digits > policy["max_integer_digits"]), f"precision_overflow_{prefix}"),
        F.when(has_value & text_col.rlike(decimal_text) & (scale <= policy["scale"]) & (integer_digits <= policy["max_integer_digits"]) & value_col.isNull(), f"precision_overflow_{prefix}"),
        F.when(has_value & text_col.rlike(decimal_text) & value_col.isNotNull() & non_positive, f"non_positive_{prefix}"),
    ]


def transform_bronze(bronze, config=None):
    config = config or load_config()
    policy = config["decimal"]
    decimal_type = T.DecimalType(policy["precision"], policy["scale"])
    parsed = bronze.withColumn("event", F.from_json("raw_event", EVENT_SCHEMA))
    normalized = parsed.select(
        "raw_event", "kafka_timestamp", "topic", "partition", "offset",
        (F.col("raw_event").isNotNull() & F.get_json_object("raw_event", "$").isNull()).alias("malformed_json"),
        F.col("event.event_id").alias("event_id"), F.col("event.event_time").alias("event_time_text"),
        F.to_timestamp("event.event_time").alias("event_time"), F.col("event.entity_id").alias("entity_id"),
        F.col("event.event_type").alias("event_type"), F.col("event.value").alias("value"),
        F.lower(F.col("event.source")).alias("source"), F.col("event.trade_id").alias("trade_id"),
        F.lower(F.col("event.symbol")).alias("symbol"), F.col("event.price").alias("price_text"),
        F.col("event.quantity").alias("quantity_text"), F.col("event.exchange_event_time").alias("exchange_event_time_text"),
        F.col("event.exchange_event_time_ms").alias("exchange_event_time_ms"), F.col("event.trade_time_ms").alias("trade_time_ms"),
        F.col("event.is_buyer_maker").alias("is_buyer_maker"),
    )
    normalized = (normalized
        .withColumn("exchange_event_time", F.to_timestamp("exchange_event_time_text"))
        .withColumn("price", F.expr(f"try_cast(price_text AS DECIMAL({policy['precision']},{policy['scale']}))"))
        .withColumn("quantity", F.expr(f"try_cast(quantity_text AS DECIMAL({policy['precision']},{policy['scale']}))"))
        .withColumn("is_crypto", (F.col("source") == "binance") & (F.col("event_type") == "crypto_trade"))
        .withColumn("is_legacy", F.col("source").isNull() & F.col("event_type").isNotNull()))
    malformed = F.col("raw_event").isNotNull() & F.col("malformed_json")
    reasons = [
        F.when(F.col("raw_event").isNull(), "missing_raw_event"),
        F.when(malformed, "malformed_json"),
        F.when(~malformed & ~(F.col("is_crypto") | F.col("is_legacy")), "unsupported_source_or_event_type"),
        F.when(F.col("event_id").isNull() | (F.trim("event_id") == ""), "missing_event_id"),
        F.when(F.col("event_time").isNull(), "invalid_event_time"),
        F.when(F.col("is_legacy") & (F.col("entity_id").isNull() | (F.trim("entity_id") == "")), "missing_entity_id"),
        F.when(F.col("is_legacy") & F.col("value").isNull(), "missing_value"),
        F.when(F.col("is_crypto") & (F.col("trade_id").isNull() | (F.trim("trade_id") == "")), "missing_trade_id"),
        F.when(F.col("is_crypto") & (F.col("symbol").isNull() | (F.trim("symbol") == "")), "missing_symbol"),
        F.when(F.col("is_crypto") & F.col("exchange_event_time").isNull(), "invalid_exchange_event_time"),
    ]
    reasons.extend(F.when(F.col("is_crypto"), reason) for reason in _decimal_reason(F.col("price_text"), F.col("price"), "price", policy))
    reasons.extend(F.when(F.col("is_crypto"), reason) for reason in _decimal_reason(F.col("quantity_text"), F.col("quantity"), "quantity", policy))
    classified = normalized.withColumn("reject_reason", F.concat_ws(";", *reasons))
    classified = classified.withColumn("trade_identity", F.when(F.col("is_crypto"), F.concat_ws(":", F.lit("binance"), F.col("symbol"), F.col("trade_id"))).otherwise(F.col("event_id")))
    invalid = classified.where(F.col("reject_reason") != "")
    valid = classified.where(F.col("reject_reason") == "")
    ranking = Window.partitionBy("trade_identity").orderBy(F.col("kafka_timestamp").desc_nulls_last(), F.col("partition").desc_nulls_last(), F.col("offset").desc_nulls_last())
    silver = (valid.withColumn("event_rank", F.row_number().over(ranking)).where(F.col("event_rank") == 1)
        .withColumn("ingestion_timestamp", F.col("kafka_timestamp")).select(*SILVER_COLUMNS))
    return silver, invalid, valid


def main():
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(message)s")
    config = load_config()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--master", default=config["spark"]["master"])
    parser.add_argument("--bronze", default=config["bronze"]["output"])
    parser.add_argument("--silver", default=config["silver"]["output"])
    parser.add_argument("--quarantine", default=config["silver"]["quarantine"])
    args = parser.parse_args()
    run_id, started = uuid.uuid4().hex[:12], time.monotonic()
    spark = spark_session(args.master, config)
    spark.sparkContext.setLogLevel("WARN")
    try:
        bronze = spark.read.format("delta").load(args.bronze)
        silver, invalid, valid = transform_bronze(bronze, config)
        bronze = bronze.persist(); valid = valid.persist(); invalid = invalid.persist(); silver = silver.persist()
        input_count = bronze.count(); valid_count = valid.count(); invalid_count = invalid.count(); silver_count = silver.count()
        max_offset = bronze.agg(F.max("offset").alias("max_offset")).first()["max_offset"]
        reason_counts = {row["reject_reason"]: row["count"] for row in invalid.groupBy("reject_reason").count().collect()}
        silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(args.silver)
        invalid.select("raw_event", "reject_reason", "kafka_timestamp", "topic", "partition", "offset").write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(args.quarantine)
        elapsed = time.monotonic() - started
        LOG.info("SILVER_METRICS run_id=%s duration_seconds=%.3f input=%s valid=%s invalid=%s duplicates_removed=%s output=%s max_kafka_offset=%s", run_id, elapsed, input_count, valid_count, invalid_count, valid_count-silver_count, silver_count, max_offset)
        LOG.info("SILVER_QUARANTINE_REASONS run_id=%s reasons=%s", run_id, reason_counts)
        LOG.info("SILVER_PATH %s QUARANTINE_PATH %s", args.silver, args.quarantine)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
