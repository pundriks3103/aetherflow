#!/usr/bin/env python3
"""Delta Lake + SeaweedFS S3A smoke test. Run with spark-submit in the worker."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent / "scripts"))

from pyspark.sql import SparkSession

from aetherflow_config import configure_s3a, load_config


config = load_config()
target = config["smoke_test"]["output"]
spark = (SparkSession.builder.appName("AetherFlow-Delta-S3-SmokeTest")
    .master(config["spark"]["master"])
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"))
spark = configure_s3a(spark, config).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

try:
    rows = [(1, "alpha"), (2, "beta"), (3, "gamma")]
    expected = len(rows)
    df = spark.createDataFrame(rows, ["id", "value"])
    print(f"Writing {expected} rows to {target}")
    df.write.format("delta").mode("overwrite").save(target)
    result = spark.read.format("delta").load(target)
    actual = result.count()
    result.orderBy("id").show(truncate=False)
    if actual != expected:
        raise RuntimeError(f"Read-back count mismatch: expected {expected}, got {actual}")
    print(f"PASS: Delta write/read succeeded; rows={actual}; path={target}")
finally:
    spark.stop()
