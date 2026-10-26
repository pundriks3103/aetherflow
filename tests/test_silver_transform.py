import json
import sys
import unittest
from datetime import datetime

from pyspark.sql import SparkSession, types as T

sys.path.insert(0, "/opt/aetherflow/scripts")
sys.path.insert(0, "scripts")
from silver_transform import DECIMAL_TYPE, transform_bronze


class SilverTransformTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master("local[2]").appName("silver-tests").getOrCreate()
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def bronze(self, payloads):
        schema = T.StructType([
            T.StructField("raw_event", T.StringType()), T.StructField("kafka_timestamp", T.TimestampType()),
            T.StructField("topic", T.StringType()), T.StructField("partition", T.IntegerType()), T.StructField("offset", T.LongType()),
        ])
        return self.spark.createDataFrame(payloads, schema)

    def trade(self, trade_id, offset, price="80158.70000000", quantity="0.00168000", event_id=None):
        return (json.dumps({"source": "binance", "event_type": "crypto_trade", "event_id": event_id or f"binance:btcusdt:{trade_id}", "trade_id": str(trade_id), "symbol": "BTCUSDT", "price": price, "quantity": quantity, "event_time": "2026-10-26T06:06:12.450000+00:00", "exchange_event_time": "2026-10-26T06:06:12.450000+00:00"}), datetime(2026, 10, 26, 6, 6, 12), "aetherflow-events", 0, offset)

    def result(self, rows):
        silver, invalid, valid = transform_bronze(self.bronze(rows))
        return silver.collect(), invalid.select("offset", "reject_reason").orderBy("offset").collect(), valid.count()

    def test_supported_boundaries_and_contents(self):
        silver, invalid, _ = self.result([self.trade("boundary", 1, "12345678901234567890.123456789012345678", "0.123456789012345678")])
        self.assertEqual(invalid, [])
        self.assertEqual(str(silver[0].price), "12345678901234567890.123456789012345678")
        self.assertEqual(str(silver[0].quantity), "0.123456789012345678")
        self.assertEqual(silver[0].price.as_tuple().exponent, -18)

    def test_invalid_decimal_reasons_and_failed_cast_null_bug(self):
        rows = [
            self.trade("missing", 1, price=None), self.trade("malformed", 2, price="abc"),
            self.trade("scale", 3, price="1.1234567890123456789"), self.trade("overflow", 4, price="123456789012345678901"),
            self.trade("zero", 5, price="0"), self.trade("negative", 6, price="-1"),
            self.trade("qty-null", 7, quantity=None), self.trade("qty-scale", 8, quantity="1.1234567890123456789"),
        ]
        _, invalid, _ = self.result(rows)
        reasons = {row.offset: row.reject_reason for row in invalid}
        self.assertIn("missing_price", reasons[1]); self.assertIn("malformed_price", reasons[2])
        self.assertIn("excessive_scale_price", reasons[3]); self.assertIn("precision_overflow_price", reasons[4])
        self.assertIn("non_positive_price", reasons[5]); self.assertIn("non_positive_price", reasons[6])
        self.assertIn("missing_quantity", reasons[7]); self.assertIn("excessive_scale_quantity", reasons[8])

    def test_separate_batch_duplicate_late_payload_and_legacy(self):
        first = self.bronze([self.trade("same", 10, price="10.00")])
        second = self.bronze([self.trade("same", 11, price="11.00"), (json.dumps({"event_id": "legacy-1", "event_time": "2026-10-26T06:00:00Z", "entity_id": "sensor-1", "event_type": "demo", "value": 1}), datetime(2026, 10, 26, 6), "aetherflow-events", 0, 12)])
        combined = first.unionByName(second)
        silver, invalid, _ = transform_bronze(combined)
        self.assertEqual(invalid.count(), 0)
        rows = {row.trade_identity: row for row in silver.collect()}
        self.assertEqual(len(rows), 2)
        self.assertEqual(str(rows["binance:btcusdt:same"].price), "11.000000000000000000")
        self.assertEqual(rows["legacy-1"].value, 1)

    def test_malformed_and_repeat_are_deterministic(self):
        bronze = self.bronze([("not-json", datetime(2026, 10, 26, 6), "aetherflow-events", 0, 20)])
        a = transform_bronze(bronze); b = transform_bronze(bronze)
        self.assertEqual(a[0].collect(), b[0].collect())
        self.assertEqual(a[1].select("reject_reason").collect(), b[1].select("reject_reason").collect())
        self.assertIn("malformed_json", a[1].first().reject_reason)
        self.assertEqual(a[0].schema["price"].dataType, DECIMAL_TYPE)


if __name__ == "__main__":
    unittest.main()
