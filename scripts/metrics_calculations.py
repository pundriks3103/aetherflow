"""Pure calculations used by the read-only operational metrics snapshot."""

from datetime import datetime, timezone
import re


def parse_utc(value):
    if value is None:
        return None
    if isinstance(value, datetime):
        parsed = value
    else:
        text = str(value).strip()
        if not text:
            return None
        try:
            normalized = text.replace("Z", "+00:00")
            fraction = re.search(r"\.(\d+)", normalized)
            if fraction and len(fraction.group(1)) > 6:
                start, end = fraction.span(1)
                normalized = normalized[:start + 6] + normalized[end:]
            parsed = datetime.fromisoformat(normalized)
        except ValueError:
            return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def timestamp_freshness(timestamp, measured_at, source_status="available"):
    """Return nonnegative age, or a distinct missing/future-clock status."""
    event_time = parse_utc(timestamp)
    measured = parse_utc(measured_at)
    if measured is None:
        raise ValueError("measured_at must be a valid timestamp")
    if event_time is None:
        status = source_status if source_status in ("empty_table", "no_valid_events", "no_successful_run") else "missing_timestamp"
        return {"timestamp_utc": None, "age_seconds": None, "clock_skew_seconds": None, "status": status}
    difference = (measured - event_time).total_seconds()
    if difference < 0:
        return {
            "timestamp_utc": event_time.isoformat().replace("+00:00", "Z"),
            "age_seconds": None,
            "clock_skew_seconds": round(-difference, 3),
            "status": "future_clock_skew",
        }
    return {
        "timestamp_utc": event_time.isoformat().replace("+00:00", "Z"),
        "age_seconds": round(difference, 3),
        "clock_skew_seconds": None,
        "status": "available",
    }


def partition_offset_metrics(beginning_offset, end_offset, highest_bronze_offset):
    """Compare Kafka's exclusive end with the latest offset stored in Bronze."""
    if beginning_offset is None or end_offset is None:
        return {
            "beginning_offset": beginning_offset,
            "end_offset_exclusive": end_offset,
            "highest_bronze_offset": highest_bronze_offset,
            "bronze_next_offset": None if highest_bronze_offset is None else highest_bronze_offset + 1,
            "offset_position_lag": None,
            "retention_gap": None,
            "status": "kafka_offsets_unavailable",
        }
    next_offset = None if highest_bronze_offset is None else highest_bronze_offset + 1
    if next_offset is None:
        return {
            "beginning_offset": beginning_offset,
            "end_offset_exclusive": end_offset,
            "highest_bronze_offset": None,
            "bronze_next_offset": None,
            "offset_position_lag": None,
            "retention_gap": None,
            "status": "no_bronze_offset",
        }
    if end_offset < beginning_offset:
        status = "inconsistent_kafka_offsets"
    elif next_offset > end_offset:
        status = "bronze_ahead_of_kafka_snapshot"
    else:
        status = "available"
    return {
        "beginning_offset": beginning_offset,
        "end_offset_exclusive": end_offset,
        "highest_bronze_offset": highest_bronze_offset,
        "bronze_next_offset": next_offset,
        "offset_position_lag": end_offset - next_offset,
        "retention_gap": next_offset < beginning_offset,
        "status": status,
    }


def calculate_partition_offsets(kafka_offsets, bronze_offsets):
    """Build partition results from maps keyed by Kafka partition number."""
    partitions = sorted(set(kafka_offsets) | set(bronze_offsets))
    results = {}
    for partition in partitions:
        kafka = kafka_offsets.get(partition)
        bronze_max = bronze_offsets.get(partition)
        if kafka is None:
            results[partition] = {
                "beginning_offset": None,
                "end_offset_exclusive": None,
                "highest_bronze_offset": bronze_max,
                "bronze_next_offset": None if bronze_max is None else bronze_max + 1,
                "offset_position_lag": None,
                "retention_gap": None,
                "status": "partition_not_in_kafka_snapshot",
            }
        else:
            results[partition] = partition_offset_metrics(
                kafka.get("beginning_offset"), kafka.get("end_offset_exclusive"), bronze_max
            )
    return results
