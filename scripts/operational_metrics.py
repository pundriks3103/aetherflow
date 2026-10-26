#!/usr/bin/env python3
"""Read-only AetherFlow freshness, Kafka offset, and run metrics snapshot."""

import argparse
import json
import subprocess
import sys
from datetime import datetime, timezone

from metrics_calculations import calculate_partition_offsets, parse_utc, timestamp_freshness


RESULT_PREFIX = "AETHERFLOW_METRICS_RESULT="


def _iso_utc(value):
    parsed = parse_utc(value)
    return None if parsed is None else parsed.isoformat().replace("+00:00", "Z")


def _emit_probe_result(result):
    print(RESULT_PREFIX + json.dumps(result, sort_keys=True, separators=(",", ":")))


def _data_probe():
    from kafka.admin import KafkaAdminClient
    from kafka import KafkaConsumer, TopicPartition
    from pyspark.sql import SparkSession, functions as F

    from aetherflow_config import configure_s3a, load_config
    from silver_transform import transform_bronze

    config = load_config(context="compose")
    topic = config["kafka"]["topics"]["events"]
    result = {
        "topic": topic,
        "kafka": {},
        "bronze": {"status": "unavailable"},
        "silver": {"status": "unavailable"},
        "analytics": {"status": "unavailable"},
        "quarantine": {"status": "unavailable"},
    }

    admin = None
    consumer = None
    try:
        admin = KafkaAdminClient(bootstrap_servers=config["kafka"]["bootstrap_servers"])
        topic_info = admin.describe_topics([topic])[0]
        if topic_info.get("error_code"):
            result["kafka"] = {
                "status": "topic_unavailable",
                "error": f"Kafka topic metadata error code {topic_info['error_code']}",
                "partitions": {},
            }
        else:
            partition_errors = [
                partition for partition in topic_info.get("partitions", [])
                if partition.get("error_code", 0) != 0
            ]
            if partition_errors:
                result["kafka"] = {
                    "status": "partition_metadata_unavailable",
                    "error": ",".join(
                        f"partition {partition['partition']} error code {partition['error_code']}"
                        for partition in partition_errors
                    ),
                    "partitions": {},
                }
            else:
                partition_ids = sorted(partition["partition"] for partition in topic_info.get("partitions", []))
                topic_partitions = [TopicPartition(topic, partition) for partition in partition_ids]
                consumer = KafkaConsumer(
                    bootstrap_servers=config["kafka"]["bootstrap_servers"],
                    enable_auto_commit=False,
                    request_timeout_ms=10000,
                    api_version_auto_timeout_ms=5000,
                )
                beginning = consumer.beginning_offsets(topic_partitions)
                ending = consumer.end_offsets(topic_partitions)
                result["kafka"] = {
                    "status": "available",
                    "partitions": {
                        str(partition.partition): {
                            "beginning_offset": beginning.get(partition),
                            "end_offset_exclusive": ending.get(partition),
                        }
                        for partition in topic_partitions
                    },
                }
    except Exception as exc:
        result["kafka"] = {"status": "unavailable", "error": f"{type(exc).__name__}: {exc}", "partitions": {}}
    finally:
        if consumer is not None:
            consumer.close()
        if admin is not None:
            admin.close()

    spark = None
    try:
        builder = (
            SparkSession.builder.master("local[2]")
            .appName("AetherFlow-Operational-Metrics-ReadOnly")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        )
        spark = configure_s3a(builder, config).getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")

        try:
            bronze = spark.read.format("delta").load(config["bronze"]["output"])
            bronze = bronze.where(F.col("topic") == topic)
            partition_rows = bronze.groupBy("partition").agg(
                F.max("offset").alias("max_offset"), F.count("*").alias("rows")
            ).collect()
            bronze_offsets = {
                int(row["partition"]): int(row["max_offset"])
                for row in partition_rows
                if row["max_offset"] is not None
            }
            _, _, valid = transform_bronze(bronze, config)
            valid_crypto = valid.where((F.col("source") == "binance") & (F.col("event_type") == "crypto_trade"))
            latest_bronze_event = (
                valid_crypto
                .agg(F.max("exchange_event_time").alias("latest"))
                .first()["latest"]
            )
            bronze_count = sum(int(row["rows"]) for row in partition_rows)
            result["bronze"] = {
                "status": "available",
                "row_count": bronze_count,
                "offsets": {str(partition): value for partition, value in bronze_offsets.items()},
                "latest_valid_exchange_event_time_utc": _iso_utc(latest_bronze_event),
                "freshness_source_status": "empty_table" if bronze_count == 0 else "no_valid_events",
            }
        except Exception as exc:
            result["bronze"] = {"status": "unavailable", "error": f"{type(exc).__name__}: {exc}", "offsets": {}}

        try:
            silver = spark.read.format("delta").load(config["silver"]["output"])
            silver_stats = silver.agg(
                F.count("*").alias("rows"),
                F.max(F.when((F.col("source") == "binance") & (F.col("event_type") == "crypto_trade"), F.col("exchange_event_time"))).alias("latest"),
            ).first()
            latest_silver_event = silver_stats["latest"]
            silver_count = int(silver_stats["rows"])
            result["silver"] = {
                "status": "available",
                "row_count": silver_count,
                "latest_valid_exchange_event_time_utc": _iso_utc(latest_silver_event),
                "freshness_source_status": "empty_table" if silver_count == 0 else "no_valid_events",
            }
        except Exception as exc:
            result["silver"] = {"status": "unavailable", "error": f"{type(exc).__name__}: {exc}"}

        try:
            analytics = spark.read.format("delta").load(config["analytics"]["output"])
            analytics_stats = analytics.agg(
                F.count("*").alias("rows"), F.max("window_end").alias("latest_window_end")
            ).first()
            analytics_count = int(analytics_stats["rows"])
            result["analytics"] = {
                "status": "available",
                "row_count": analytics_count,
                "latest_window_end_utc": _iso_utc(analytics_stats["latest_window_end"]),
                "table_status": "empty_table" if analytics_count == 0 else "available",
            }
        except Exception as exc:
            result["analytics"] = {"status": "unavailable", "error": f"{type(exc).__name__}: {exc}"}

        try:
            quarantine = spark.read.format("delta").load(config["silver"]["quarantine"])
            reason_rows = quarantine.groupBy("reject_reason").count().collect()
            result["quarantine"] = {
                "status": "available",
                "by_reason": {str(row["reject_reason"]): int(row["count"]) for row in reason_rows},
                "total_records": sum(int(row["count"]) for row in reason_rows),
            }
        except Exception as exc:
            result["quarantine"] = {"status": "unavailable", "error": f"{type(exc).__name__}: {exc}"}
    except Exception as exc:
        detail = f"{type(exc).__name__}: {exc}"
        result["bronze"] = {"status": "unavailable", "error": detail}
        result["silver"] = {"status": "unavailable", "error": detail}
        result["analytics"] = {"status": "unavailable", "error": detail}
        result["quarantine"] = {"status": "unavailable", "error": detail}
    finally:
        if spark is not None:
            spark.stop()

    return result


def _airflow_probe():
    from airflow.models import TaskInstance
    from airflow.utils.session import create_session
    from airflow.utils.state import TaskInstanceState

    from aetherflow_config import load_config

    config = load_config(context="compose")
    with create_session() as session:
        instance = (
            session.query(TaskInstance.run_id, TaskInstance.end_date)
            .filter(
                TaskInstance.dag_id == config["airflow"]["dag_id"],
                TaskInstance.task_id == "silver_transform",
                TaskInstance.state == TaskInstanceState.SUCCESS,
                TaskInstance.end_date.isnot(None),
            )
            .order_by(TaskInstance.end_date.desc())
            .first()
        )
    if instance is None:
        return {"status": "no_successful_task", "run_id": None, "end_time_utc": None}
    return {
        "status": "available",
        "run_id": instance.run_id,
        "end_time_utc": _iso_utc(instance.end_date),
    }


def _probe_mode(args):
    try:
        if args.data_probe:
            result = _data_probe()
        else:
            result = _airflow_probe()
    except Exception as exc:
        result = {"status": "unavailable", "error": f"{type(exc).__name__}: {exc}"}
    _emit_probe_result(result)
    return 0


def _compose_probe(service, mode, as_of):
    command = [
        "docker", "compose", "exec", "-T", service,
        "python3", "/opt/aetherflow/scripts/operational_metrics.py", mode, "--as-of", as_of,
    ]
    try:
        proc = subprocess.run(command, text=True, capture_output=True, timeout=180)
    except (OSError, subprocess.TimeoutExpired) as exc:
        return {"status": "unavailable", "error": f"{type(exc).__name__}: {exc}"}
    for line in proc.stdout.splitlines():
        if line.startswith(RESULT_PREFIX):
            try:
                return json.loads(line[len(RESULT_PREFIX):])
            except json.JSONDecodeError:
                break
    detail = (proc.stderr or proc.stdout).strip().splitlines()
    return {"status": "unavailable", "error": detail[-1] if detail else f"probe exited {proc.returncode}"}


def _latest_bronze_progress(measured_at):
    try:
        proc = subprocess.run(
            ["docker", "compose", "logs", "--no-color", "--timestamps", "--tail=500", "bronze-stream"],
            text=True, capture_output=True, timeout=30,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        return {"status": "unavailable", "error": f"{type(exc).__name__}: {exc}"}
    candidates = [line.strip() for line in proc.stdout.splitlines() if "BRONZE_PROGRESS " in line]
    if proc.returncode != 0:
        lines = (proc.stderr or proc.stdout).strip().splitlines()
        return {"status": "unavailable", "error": lines[-1] if lines else "docker compose logs failed"}
    if not candidates:
        return {"status": "no_recent_progress", "message": "No BRONZE_PROGRESS entries in the last 500 service log lines."}
    line = candidates[-1]
    # `docker compose logs` prefixes the service name, then the container timestamp.
    content = line.split("|", 1)[-1].strip()
    emitted_at = content.split(maxsplit=1)[0]
    progress_text = content.split("BRONZE_PROGRESS ", 1)[1]
    return {
        "status": "available",
        "logged_at_utc": emitted_at,
        "freshness": timestamp_freshness(emitted_at, measured_at),
        "progress": "BRONZE_PROGRESS " + progress_text,
    }


def _value(value):
    return "unknown" if value is None else str(value)


def _format_freshness(label, metric):
    timestamp = metric.get("timestamp_utc")
    age = metric.get("age_seconds")
    skew = metric.get("clock_skew_seconds")
    if metric["status"] == "future_clock_skew":
        return f"{label}: timestamp_utc={timestamp} age_seconds=unknown clock_skew_seconds={skew} status=future_clock_skew"
    return f"{label}: timestamp_utc={_value(timestamp)} age_seconds={_value(age)} status={metric['status']}"


def render_snapshot(measured_at, data, airflow, progress):
    lines = [f"AETHERFLOW_OPERATIONAL_METRICS measured_at_utc={measured_at}"]
    topic = data.get("topic", "unknown")
    kafka = data.get("kafka", {})
    lines.append(f"KAFKA topic={topic} status={kafka.get('status', 'unavailable')}")
    kafka_offsets = {
        int(partition): offsets
        for partition, offsets in kafka.get("partitions", {}).items()
    }
    bronze_offsets = {int(partition): value for partition, value in data.get("bronze", {}).get("offsets", {}).items()}
    for partition, metric in calculate_partition_offsets(kafka_offsets, bronze_offsets).items():
        lines.append(
            "KAFKA_PARTITION "
            f"partition={partition} beginning_offset={_value(metric['beginning_offset'])} "
            f"end_offset_exclusive={_value(metric['end_offset_exclusive'])} "
            f"bronze_highest_offset={_value(metric['highest_bronze_offset'])} "
            f"bronze_next_offset={_value(metric['bronze_next_offset'])} "
            f"offset_position_lag={_value(metric['offset_position_lag'])} "
            f"retention_gap={_value(metric['retention_gap'])} status={metric['status']}"
        )
    if kafka.get("error"):
        lines.append(f"KAFKA_ERROR {kafka['error']}")
    lines.append("OFFSET_LAG_SEMANTICS offset_position_lag is an offset-position difference, not necessarily an exact count of unprocessed records.")

    bronze = data.get("bronze", {})
    lines.append(f"BRONZE status={bronze.get('status', 'unavailable')} row_count={_value(bronze.get('row_count'))}")
    if "latest_valid_exchange_event_time_utc" in bronze:
        metric = timestamp_freshness(
            bronze["latest_valid_exchange_event_time_utc"], measured_at,
            bronze.get("freshness_source_status", "available"),
        )
        lines.append(_format_freshness("BRONZE_LATEST_VALID_BINANCE_EXCHANGE_EVENT", metric))
    if bronze.get("error"):
        lines.append(f"BRONZE_ERROR {bronze['error']}")

    silver = data.get("silver", {})
    lines.append(f"SILVER status={silver.get('status', 'unavailable')} row_count={_value(silver.get('row_count'))}")
    if "latest_valid_exchange_event_time_utc" in silver:
        metric = timestamp_freshness(
            silver["latest_valid_exchange_event_time_utc"], measured_at,
            silver.get("freshness_source_status", "available"),
        )
        lines.append(_format_freshness("SILVER_LATEST_VALID_BINANCE_EXCHANGE_EVENT", metric))
    if silver.get("error"):
        lines.append(f"SILVER_ERROR {silver['error']}")

    lines.append(f"AIRFLOW_SILVER_LAST_SUCCESS status={airflow.get('status', 'unavailable')} run_id={_value(airflow.get('run_id'))}")
    if airflow.get("status") == "no_successful_task":
        metric = timestamp_freshness(None, measured_at, "no_successful_run")
        lines.append(_format_freshness("AIRFLOW_SILVER_TASK_END", metric))
    elif airflow.get("end_time_utc"):
        metric = timestamp_freshness(airflow["end_time_utc"], measured_at)
        lines.append(_format_freshness("AIRFLOW_SILVER_TASK_END", metric))
    if airflow.get("error"):
        lines.append(f"AIRFLOW_ERROR {airflow['error']}")

    quarantine = data.get("quarantine", {})
    reason_counts = quarantine.get("by_reason")
    lines.append(
        f"QUARANTINE status={quarantine.get('status', 'unavailable')} "
        f"total_records={_value(quarantine.get('total_records'))} by_reason="
        f"{json.dumps(reason_counts, sort_keys=True) if reason_counts is not None else 'unknown'}"
    )
    if quarantine.get("error"):
        lines.append(f"QUARANTINE_ERROR {quarantine['error']}")

    lines.append(f"BRONZE_LATEST_PROGRESS status={progress.get('status', 'unavailable')}")
    if progress.get("status") == "available":
        lines.append(
            f"BRONZE_PROGRESS_ACTIVITY logged_at_utc={progress.get('logged_at_utc')} "
            f"age_seconds={progress.get('freshness', {}).get('age_seconds')} {progress.get('progress')}"
        )
    elif progress.get("message"):
        lines.append(progress["message"])
    if progress.get("error"):
        lines.append(f"BRONZE_PROGRESS_ERROR {progress['error']}")
    return "\n".join(lines)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--as-of", help="Use a fixed ISO-8601 UTC time (primarily for verification).")
    parser.add_argument("--data-probe", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--airflow-probe", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args(argv)
    if args.data_probe or args.airflow_probe:
        if args.data_probe == args.airflow_probe:
            parser.error("select exactly one internal probe mode")
        return _probe_mode(args)

    probe_as_of = _iso_utc(args.as_of or datetime.now(timezone.utc))
    data = _compose_probe("bronze-stream", "--data-probe", probe_as_of)
    airflow = _compose_probe("airflow-scheduler", "--airflow-probe", probe_as_of)
    measured_at = _iso_utc(args.as_of or datetime.now(timezone.utc))
    progress = _latest_bronze_progress(measured_at)
    print(render_snapshot(measured_at, data, airflow, progress))
    return 0


if __name__ == "__main__":
    sys.exit(main())
