import sys
import unittest
from pathlib import Path

REPOSITORY = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY / "scripts"))

from bronze_stream import format_progress


class BronzeProgressMetricsTest(unittest.TestCase):
    def test_formats_batch_rates_duration_and_partition_offsets(self):
        progress = {
            "batchId": 12,
            "numInputRows": 25,
            "inputRowsPerSecond": 2.5,
            "processedRowsPerSecond": 5.0,
            "durationMs": {"triggerExecution": 5012},
            "sources": [{
                "startOffset": '{"aetherflow-events":{"0":10,"1":20}}',
                "endOffset": '{"aetherflow-events":{"0":15,"1":40}}',
            }],
        }

        line = format_progress(progress)

        self.assertIn("BRONZE_PROGRESS batch_id=12", line)
        self.assertIn("input_rows=25", line)
        self.assertIn("input_rows_per_second=2.5", line)
        self.assertIn("processed_rows_per_second=5.0", line)
        self.assertIn("trigger_execution_ms=5012", line)
        self.assertIn('start={"aetherflow-events":{"0":10,"1":20}}', line)
        self.assertIn('end={"aetherflow-events":{"0":15,"1":40}}', line)

    def test_missing_progress_fields_are_reported_as_unavailable(self):
        line = format_progress({"sources": []})
        self.assertIn("batch_id=None", line)
        self.assertIn("input_rows=None", line)
        self.assertIn("trigger_execution_ms=None", line)
        self.assertIn("source_offsets=unavailable", line)


if __name__ == "__main__":
    unittest.main()
