import sys
import unittest
from pathlib import Path

REPOSITORY = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY / "scripts"))

from metrics_calculations import calculate_partition_offsets, partition_offset_metrics, timestamp_freshness
from operational_metrics import render_snapshot


class OperationalMetricsCalculationTest(unittest.TestCase):
    def test_normal_single_partition(self):
        metric = partition_offset_metrics(0, 10, 7)
        self.assertEqual(metric["bronze_next_offset"], 8)
        self.assertEqual(metric["offset_position_lag"], 2)
        self.assertFalse(metric["retention_gap"])
        self.assertEqual(metric["status"], "available")

    def test_multiple_partitions_are_calculated_independently(self):
        metrics = calculate_partition_offsets(
            {
                0: {"beginning_offset": 0, "end_offset_exclusive": 5},
                1: {"beginning_offset": 2, "end_offset_exclusive": 9},
            },
            {0: 4, 1: 5},
        )
        self.assertEqual(metrics[0]["offset_position_lag"], 0)
        self.assertEqual(metrics[1]["offset_position_lag"], 3)
        self.assertFalse(metrics[0]["retention_gap"])
        self.assertFalse(metrics[1]["retention_gap"])

    def test_empty_partition_is_unknown_until_bronze_has_an_offset(self):
        metric = partition_offset_metrics(0, 0, None)
        self.assertIsNone(metric["offset_position_lag"])
        self.assertIsNone(metric["retention_gap"])
        self.assertEqual(metric["status"], "no_bronze_offset")

    def test_offset_gaps_are_positions_not_record_counts(self):
        metric = partition_offset_metrics(0, 8, 3)
        self.assertEqual(metric["offset_position_lag"], 4)
        self.assertFalse(metric["retention_gap"])

    def test_retention_gap_is_explicit(self):
        metric = partition_offset_metrics(10, 15, 7)
        self.assertEqual(metric["bronze_next_offset"], 8)
        self.assertEqual(metric["offset_position_lag"], 7)
        self.assertTrue(metric["retention_gap"])

    def test_empty_and_missing_timestamps_are_not_zero_freshness(self):
        measured_at = "2026-10-26T08:00:00Z"
        empty = timestamp_freshness(None, measured_at, "empty_table")
        missing = timestamp_freshness(None, measured_at)
        no_valid = timestamp_freshness(None, measured_at, "no_valid_events")
        self.assertEqual(empty["status"], "empty_table")
        self.assertEqual(missing["status"], "missing_timestamp")
        self.assertEqual(no_valid["status"], "no_valid_events")
        self.assertIsNone(empty["age_seconds"])
        self.assertIsNone(missing["age_seconds"])

    def test_future_timestamp_reports_clock_skew_without_negative_age(self):
        metric = timestamp_freshness("2026-10-26T08:00:05Z", "2026-10-26T08:00:00Z")
        self.assertEqual(metric["status"], "future_clock_skew")
        self.assertIsNone(metric["age_seconds"])
        self.assertEqual(metric["clock_skew_seconds"], 5)

    def test_compose_nanosecond_log_timestamp_is_parsed(self):
        metric = timestamp_freshness("2026-10-26T08:00:00.123456789Z", "2026-10-26T08:00:01Z")
        self.assertEqual(metric["status"], "available")
        self.assertEqual(metric["age_seconds"], 0.877)

    def test_offset_metrics_do_not_turn_missing_kafka_data_into_zero(self):
        metric = partition_offset_metrics(None, None, 7)
        self.assertIsNone(metric["offset_position_lag"])
        self.assertIsNone(metric["retention_gap"])
        self.assertEqual(metric["status"], "kafka_offsets_unavailable")

    def test_unavailable_services_are_printed_as_unknown(self):
        snapshot = render_snapshot(
            "2026-10-26T08:00:00Z",
            {
                "topic": "aetherflow-events",
                "kafka": {"status": "unavailable", "partitions": {}, "error": "broker unavailable"},
                "bronze": {"status": "unavailable", "error": "storage unavailable"},
                "silver": {"status": "unavailable", "error": "table unavailable"},
                "quarantine": {"status": "unavailable", "error": "table unavailable"},
            },
            {"status": "no_successful_task", "run_id": None},
            {"status": "no_recent_progress", "message": "No recent progress."},
        )
        self.assertIn("KAFKA topic=aetherflow-events status=unavailable", snapshot)
        self.assertNotIn("KAFKA_PARTITION", snapshot)
        self.assertIn("AIRFLOW_SILVER_LAST_SUCCESS status=no_successful_task", snapshot)
        self.assertIn("age_seconds=unknown status=no_successful_run", snapshot)
        self.assertIn("QUARANTINE status=unavailable total_records=unknown by_reason=unknown", snapshot)
        self.assertNotIn("offset_position_lag=0", snapshot)


if __name__ == "__main__":
    unittest.main()
