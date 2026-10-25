#!/usr/bin/env python3
"""Delta Lake + SeaweedFS S3A smoke test. Run with spark-submit in the worker."""

import os
from pyspark.sql import SparkSession

endpoint = os.environ.get("S3_ENDPOINT", "http://seaweedfs:8333")
access_key = os.environ.get("AWS_ACCESS_KEY_ID")
secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
bucket = os.environ.get("S3_BUCKET_NAME", "aetherflow-data")

if not access_key or not secret_key:
    raise RuntimeError("AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY are missing in the Spark container")

target = f"s3a://{bucket}/bronze/smoke-test"

spark = (
    SparkSession.builder
    .appName("AetherFlow-Delta-S3-SmokeTest")
    .master("spark://spark-master:7077")
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
