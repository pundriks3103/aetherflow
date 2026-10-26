#!/usr/bin/env python3
"""Rebuild deterministic UTC hourly Binance trade aggregates from Silver."""

import argparse
import logging
import os
import time
import uuid

from pyspark.sql import SparkSession, Window, functions as F

from aetherflow_config import configure_s3a, load_config


LOG = logging.getLogger("aetherflow.crypto_analytics")


def spark_session(master, config):
    builder = (
        SparkSession.builder.appName("AetherFlow-Crypto-Hourly-Analytics").master(master)
        .config("spark.cores.max", str(config["spark"]["cores_max"]))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.session.timeZone", "UTC")
    )
    return configure_s3a(builder, config).getOrCreate()


def _decimal_types(config):
    settings = config["analytics"]
    price_type = f"DECIMAL({settings['price_precision']},{settings['price_scale']})"
    quantity_type = f"DECIMAL({settings['quantity_precision']},{settings['quantity_scale']})"
    vwap_type = f"DECIMAL({settings['vwap_precision']},{settings['vwap_scale']})"
    return price_type, quantity_type, vwap_type


def prepare_trades(silver, config):
    """Filter to valid Binance trades and select one deterministic row per trade identity."""
    required = {"source", "event_type", "trade_identity", "symbol", "event_time", "price", "quantity"}
    missing = sorted(required - set(silver.columns))
    if missing:
        raise ValueError(f"Silver input is missing required columns: {', '.join(missing)}")
    trades = silver.where((F.col("source") == "binance") & (F.col("event_type") == "crypto_trade"))
    required_values = ["trade_identity", "symbol", "event_time", "price", "quantity"]
    invalid = trades.where(F.expr(" OR ".join(f"`{name}` IS NULL" for name in required_values)))
    if invalid.limit(1).count():
        raise ValueError("Silver contains Binance trades with missing identity, symbol, event_time, price, or quantity")

    price_type, quantity_type, _ = _decimal_types(config)
    typed = (trades
        .withColumn("_analytics_price", F.expr(f"try_cast(price AS {price_type})"))
        .withColumn("_analytics_quantity", F.expr(f"try_cast(quantity AS {quantity_type})"))
        .withColumn("_price_roundtrip", F.expr("try_cast(_analytics_price AS DECIMAL(38,18))"))
        .withColumn("_quantity_roundtrip", F.expr("try_cast(_analytics_quantity AS DECIMAL(38,18))")))
    out_of_range = typed.where(
        F.col("_analytics_price").isNull() | F.col("_analytics_quantity").isNull()
        | (F.col("_price_roundtrip") != F.col("price"))
        | (F.col("_quantity_roundtrip") != F.col("quantity"))
    )
    if out_of_range.limit(1).count():
        raise ValueError(
            "Silver price/quantity exceeds the configured exact analytics decimal range "
            f"(price {price_type}, quantity {quantity_type}); no output was written"
        )

    # A replayed stable Binance identity may have a later Kafka record. Select that
    # record; remaining columns settle ties deterministically if provenance also ties.
    ordering = []
    for name in ("ingestion_timestamp", "kafka_timestamp", "partition", "offset"):
        if name in typed.columns:
            ordering.append(F.col(name).desc_nulls_last())
    ordering.extend([F.col("event_time").desc(), F.col("price").desc(), F.col("quantity").desc()])
    ranked = typed.withColumn("_identity_rank", F.row_number().over(Window.partitionBy("trade_identity").orderBy(*ordering)))
    return (ranked.where(F.col("_identity_rank") == 1)
        .select("trade_identity", "symbol", "event_time", "_analytics_price", "_analytics_quantity")
        .withColumnRenamed("_analytics_price", "price")
        .withColumnRenamed("_analytics_quantity", "quantity"))


def aggregate_hourly(silver, config):
    """Return UTC hour aggregates; each window is inclusive-start/exclusive-end."""
    trades = prepare_trades(silver, config)
    window_minutes = config["analytics"]["window_minutes"]
    duration = f"{window_minutes} minutes"
    _, _, vwap_type = _decimal_types(config)
    # The configured operand precision makes price * quantity Decimal(38,16)
    # for current defaults (25,8) × (12,8), with no floating-point conversion.
    product = F.col("price") * F.col("quantity")
    aggregates = (trades
        .withColumn("_window", F.window("event_time", duration))
        .groupBy("symbol", "_window")
        .agg(
            F.countDistinct("trade_identity").cast("long").alias("trade_count"),
            F.sum("quantity").alias("base_volume"),
            F.sum(product).alias("quote_notional"),
            F.min_by("price", F.struct("event_time", "trade_identity")).alias("first_price"),
            F.max_by("price", F.struct("event_time", "trade_identity")).alias("last_price"),
            F.max("price").alias("high_price"),
            F.min("price").alias("low_price"),
        )
        .withColumn("vwap", F.expr(f"try_cast(quote_notional / base_volume AS {vwap_type})"))
        .select(
            "symbol", F.col("_window.start").alias("window_start"), F.col("_window.end").alias("window_end"),
            "trade_count", "base_volume", "quote_notional", "vwap", "first_price", "last_price", "high_price", "low_price",
        ))
    return trades, aggregates


def main():
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(message)s")
    config = load_config()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--master", default=config["spark"]["master"])
    parser.add_argument("--silver", default=config["silver"]["output"])
    parser.add_argument("--output", default=config["analytics"]["output"])
    args = parser.parse_args()

    run_id, started = uuid.uuid4().hex[:12], time.monotonic()
    spark = spark_session(args.master, config)
    spark.sparkContext.setLogLevel("WARN")
    try:
        source = spark.read.format("delta").load(args.silver)
        source_count = source.count()
        crypto_input_count = source.where((F.col("source") == "binance") & (F.col("event_type") == "crypto_trade")).count()
        trades, aggregates = aggregate_hourly(source, config)
        trades = trades.persist()
        aggregates = aggregates.persist()
        trade_count = trades.count()
        output_count = aggregates.count()
        if aggregates.where(F.col("vwap").isNull() | F.col("base_volume").isNull() | F.col("quote_notional").isNull()).limit(1).count():
            raise ValueError("Aggregate arithmetic produced a null value; output was not written")
        aggregates.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(args.output)
        LOG.info(
            "CRYPTO_ANALYTICS_METRICS run_id=%s duration_seconds=%.3f silver_rows=%d unique_trades=%d duplicate_rows_removed=%d output_windows=%d",
            run_id, time.monotonic() - started, source_count, trade_count, crypto_input_count - trade_count, output_count,
        )
        LOG.info("CRYPTO_ANALYTICS_PATH %s; window_minutes=%d; event_time=trade execution time UTC", args.output, config["analytics"]["window_minutes"])
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
