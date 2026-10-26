#!/usr/bin/env python3
"""Build a validated, deduplicated Silver Delta snapshot from Bronze."""
import argparse, os
from pyspark.sql import SparkSession, Window, functions as F, types as T

DECIMAL_TYPE = T.DecimalType(38, 18)
DECIMAL_TEXT = r"^[0-9]+(?:\.[0-9]{1,18})?$"
EVENT_SCHEMA = T.StructType([T.StructField(n, t) for n, t in [
    ("event_id", T.StringType()), ("event_time", T.StringType()), ("entity_id", T.StringType()),
    ("event_type", T.StringType()), ("value", T.LongType()), ("source", T.StringType()),
    ("trade_id", T.StringType()), ("symbol", T.StringType()), ("price", T.StringType()),
    ("quantity", T.StringType()), ("exchange_event_time", T.StringType()),
    ("exchange_event_time_ms", T.LongType()), ("trade_time_ms", T.LongType()),
    ("is_buyer_maker", T.BooleanType())]])
SILVER_COLUMNS = ["kafka_timestamp", "topic", "partition", "offset", "event_id", "event_time", "entity_id", "event_type", "value", "ingestion_timestamp", "source", "trade_id", "symbol", "exchange_event_time", "exchange_event_time_ms", "trade_time_ms", "is_buyer_maker", "price", "quantity", "trade_identity"]

def spark_session(master):
    endpoint = os.environ.get("S3_ENDPOINT", "http://seaweedfs:8333")
    access_key, secret_key = os.environ.get("AWS_ACCESS_KEY_ID"), os.environ.get("AWS_SECRET_ACCESS_KEY")
    if not access_key or not secret_key: raise RuntimeError("AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY are missing")
    return (SparkSession.builder.appName("AetherFlow-Silver-Transform").master(master)
        .config("spark.cores.max", "2").config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.endpoint", endpoint).config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key).config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false").config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider").getOrCreate())

def transform_bronze(bronze):
    p = bronze.withColumn("event", F.from_json("raw_event", EVENT_SCHEMA))
    n = p.select("raw_event", "kafka_timestamp", "topic", "partition", "offset", (F.col("raw_event").isNotNull() & F.get_json_object("raw_event", "$").isNull()).alias("malformed_json"),
        F.col("event.event_id").alias("event_id"), F.col("event.event_time").alias("event_time_text"), F.to_timestamp("event.event_time").alias("event_time"),
        F.col("event.entity_id").alias("entity_id"), F.col("event.event_type").alias("event_type"), F.col("event.value").alias("value"),
        F.lower(F.col("event.source")).alias("source"), F.col("event.trade_id").alias("trade_id"), F.lower(F.col("event.symbol")).alias("symbol"),
        F.col("event.price").alias("price_text"), F.col("event.quantity").alias("quantity_text"), F.col("event.exchange_event_time").alias("exchange_event_time_text"),
        F.col("event.exchange_event_time_ms").alias("exchange_event_time_ms"), F.col("event.trade_time_ms").alias("trade_time_ms"), F.col("event.is_buyer_maker").alias("is_buyer_maker"))
    n = (n.withColumn("exchange_event_time", F.to_timestamp("exchange_event_time_text")).withColumn("price", F.col("price_text").cast(DECIMAL_TYPE)).withColumn("quantity", F.col("quantity_text").cast(DECIMAL_TYPE))
        .withColumn("is_crypto", (F.col("source") == "binance") & (F.col("event_type") == "crypto_trade")).withColumn("is_legacy", F.col("source").isNull() & F.col("event_type").isNotNull()))
    malformed = F.col("raw_event").isNotNull() & F.col("malformed_json")
    reasons = F.concat_ws(";", F.when(F.col("raw_event").isNull(), "missing_raw_event"), F.when(malformed, "malformed_json"), F.when(~malformed & ~(F.col("is_crypto") | F.col("is_legacy")), "unsupported_source_or_event_type"), F.when(F.col("event_id").isNull() | (F.trim("event_id") == ""), "missing_event_id"), F.when(F.col("event_time").isNull(), "invalid_event_time"), F.when(F.col("is_legacy") & (F.col("entity_id").isNull() | (F.trim("entity_id") == "")), "missing_entity_id"), F.when(F.col("is_legacy") & F.col("value").isNull(), "missing_value"), F.when(F.col("is_crypto") & (F.col("trade_id").isNull() | (F.trim("trade_id") == "")), "missing_trade_id"), F.when(F.col("is_crypto") & (F.col("symbol").isNull() | (F.trim("symbol") == "")), "missing_symbol"), F.when(F.col("is_crypto") & F.col("exchange_event_time").isNull(), "invalid_exchange_event_time"), F.when(F.col("is_crypto") & (F.col("price_text").isNull() | ~F.col("price_text").rlike(DECIMAL_TEXT) | (F.col("price") <= 0)), "invalid_price"), F.when(F.col("is_crypto") & (F.col("quantity_text").isNull() | ~F.col("quantity_text").rlike(DECIMAL_TEXT) | (F.col("quantity") <= 0)), "invalid_quantity"))
    c = n.withColumn("reject_reason", reasons).withColumn("trade_identity", F.when(F.col("is_crypto"), F.concat_ws(":", F.lit("binance"), F.col("symbol"), F.col("trade_id"))).otherwise(F.col("event_id")))
    invalid, valid = c.where(F.col("reject_reason") != ""), c.where(F.col("reject_reason") == "")
    w = Window.partitionBy("trade_identity").orderBy(F.col("kafka_timestamp").desc_nulls_last(), F.col("partition").desc_nulls_last(), F.col("offset").desc_nulls_last())
    silver = valid.withColumn("event_rank", F.row_number().over(w)).where(F.col("event_rank") == 1).withColumn("ingestion_timestamp", F.col("kafka_timestamp")).select(*SILVER_COLUMNS)
    return silver, invalid, valid

def main():
    parser = argparse.ArgumentParser(description=__doc__); parser.add_argument("--master", default="spark://spark-master:7077"); parser.add_argument("--bronze", default="s3a://aetherflow-data/bronze/events"); parser.add_argument("--silver", default="s3a://aetherflow-data/silver/events"); parser.add_argument("--quarantine", default="s3a://aetherflow-data/quarantine/silver-events"); args = parser.parse_args()
    spark = spark_session(args.master); spark.sparkContext.setLogLevel("WARN")
    try:
        bronze = spark.read.format("delta").load(args.bronze); input_count = bronze.count(); silver, invalid, valid = transform_bronze(bronze); valid_count, invalid_count, silver_count = valid.count(), invalid.count(), silver.count()
        silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(args.silver); invalid.select("raw_event", "reject_reason", "kafka_timestamp", "topic", "partition", "offset").write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(args.quarantine)
        print(f"SILVER_METRICS input={input_count} valid={valid_count} invalid={invalid_count} duplicates_removed={valid_count-silver_count} output={silver_count}"); print(f"SILVER_PATH {args.silver}\nQUARANTINE_PATH {args.quarantine}")
    finally: spark.stop()
if __name__ == "__main__": main()
