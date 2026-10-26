import json
import sys
import unittest
from datetime import datetime

from pyspark.sql import SparkSession, types as T

sys.path.insert(0, "/opt/aetherflow/scripts")
sys.path.insert(0, "scripts")
from silver_transform import transform_bronze, DECIMAL_TYPE


class SilverTransformTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master("local[2]").appName("silver-tests").getOrCreate()
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_crypto_legacy_invalid_duplicate_and_repeat(self):
        def crypto(offset, price="80158.70000000", quantity="0.00168000"):
            return json.dumps({"source": "binance", "event_type": "crypto_trade", "event_id": "binance:btcusdt:1", "trade_id": "1", "symbol": "BTCUSDT", "price": price, "quantity": quantity, "event_time": "2026-10-26T06:06:12.450000+00:00", "exchange_event_time": "2026-10-26T06:06:12.450000+00:00"}), datetime(2026, 10, 26, 6, 6, 12), "aetherflow-events", 0, offset
        rows = [
            (json.dumps({"event_id": "legacy-1", "event_time": "2026-10-26T06:00:00Z", "entity_id": "sensor-1", "event_type": "demo", "value": 1}), datetime(2026, 10, 26, 6, 0), "aetherflow-events", 0, 1),
            crypto(2), crypto(3), ("not-json", datetime(2026, 10, 26, 6, 0), "aetherflow-events", 0, 4),
        ]
        schema = T.StructType([T.StructField("raw_event", T.StringType()), T.StructField("kafka_timestamp", T.TimestampType()), T.StructField("topic", T.StringType()), T.StructField("partition", T.IntegerType()), T.StructField("offset", T.LongType())])
        bronze = self.spark.createDataFrame(rows, schema)
        silver, invalid, valid = transform_bronze(bronze)
        self.assertEqual(valid.count(), 3)
        self.assertEqual(invalid.count(), 1)
        self.assertEqual(silver.count(), 2)
        self.assertEqual(silver.schema["price"].dataType, DECIMAL_TYPE)
        self.assertEqual(silver.where("source = 'binance'").count(), 1)
        self.assertEqual(silver.where("source is null").count(), 1)
        self.assertIn("malformed_json", invalid.first().reject_reason)
        self.assertEqual(silver.collect(), transform_bronze(bronze)[0].collect())


if __name__ == "__main__":
    unittest.main()
