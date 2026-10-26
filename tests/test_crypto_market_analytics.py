import unittest
from datetime import datetime
from decimal import Decimal

from pyspark.sql import SparkSession, types as T

from aetherflow_config import load_config
from crypto_market_analytics import aggregate_hourly


class CryptoMarketAnalyticsTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master("local[2]").appName("crypto-market-analytics-tests").getOrCreate()
        cls.spark.sparkContext.setLogLevel("ERROR")
        cls.spark.conf.set("spark.sql.session.timeZone", "UTC")
        cls.config = load_config(environ={})

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def frame(self, rows):
        schema = T.StructType([
            T.StructField("source", T.StringType()), T.StructField("event_type", T.StringType()),
            T.StructField("trade_identity", T.StringType()), T.StructField("symbol", T.StringType()),
            T.StructField("event_time", T.TimestampType()), T.StructField("exchange_event_time", T.TimestampType()),
            T.StructField("ingestion_timestamp", T.TimestampType()), T.StructField("kafka_timestamp", T.TimestampType()),
            T.StructField("partition", T.IntegerType()), T.StructField("offset", T.LongType()),
            T.StructField("price", T.DecimalType(38, 18)), T.StructField("quantity", T.DecimalType(38, 18)),
        ])
        return self.spark.createDataFrame(rows, schema)

    def row(self, identity, event_time, price, quantity, offset, *, source="binance", event_type="crypto_trade", exchange_time=None):
        stamp = datetime.fromisoformat(event_time)
        provenance_time = datetime(2026, 1, 1, 0, 0, offset)
        return (
            source, event_type, identity, "btcusdt", stamp,
            datetime.fromisoformat(exchange_time) if exchange_time else stamp,
            provenance_time, provenance_time, 0, offset, Decimal(price), Decimal(quantity),
        )

    def test_hour_windows_dedup_decimal_metrics_and_legacy_filter(self):
        # The later Kafka record wins for trade t2 even though its payload differs.
        data = self.frame([
            self.row("binance:btcusdt:t1", "2026-01-01T00:59:59", "100.00000000", "0.50000000", 1),
            self.row("binance:btcusdt:t2", "2026-01-01T00:30:00", "110.00000000", "0.25000000", 2),
            self.row("binance:btcusdt:t2", "2026-01-01T00:30:00", "120.00000000", "0.25000000", 3),
            # event_time assigns this to the next UTC hour; exchange time intentionally differs.
            self.row("binance:btcusdt:t3", "2026-01-01T01:00:00", "200.00000000", "0.10000000", 4, exchange_time="2026-01-01T00:01:00"),
            self.row("legacy-event", "2026-01-01T00:15:00", "999.00000000", "9.00000000", 5, source="legacy", event_type="demo"),
        ])
        unique, result = aggregate_hourly(data, self.config)
        self.assertEqual(unique.count(), 3)
        rows = result.orderBy("window_start").collect()
        self.assertEqual(len(rows), 2)
        first, second = rows
        self.assertEqual(first.window_start, datetime(2026, 1, 1, 0, 0))
        self.assertEqual(first.window_end, datetime(2026, 1, 1, 1, 0))
        self.assertEqual(first.trade_count, 2)
        self.assertEqual(first.base_volume, Decimal("0.75000000"))
        self.assertEqual(first.quote_notional, Decimal("80.0000000000000000"))
        self.assertEqual(first.vwap, Decimal("106.66666667"))
        self.assertEqual((first.first_price, first.last_price, first.low_price, first.high_price), tuple(map(Decimal, ["120.00000000", "100.00000000", "100.00000000", "120.00000000"])))
        self.assertEqual(second.window_start, datetime(2026, 1, 1, 1, 0))
        self.assertEqual(second.trade_count, 1)
        self.assertEqual(second.quote_notional, Decimal("20.0000000000000000"))
        self.assertEqual(second.vwap, Decimal("200.00000000"))
        self.assertEqual(str(result.schema["quote_notional"].dataType), "DecimalType(38,16)")

    def test_utc_window_alignment_repeat_execution_and_empty_crypto_input(self):
        data = self.frame([self.row("x", "2026-01-01T00:00:00", "1.00000000", "1.00000000", 1)])
        _, first = aggregate_hourly(data, self.config)
        _, second = aggregate_hourly(data, self.config)
        self.assertEqual(first.collect(), second.collect())
        legacy = self.frame([self.row("legacy", "2026-01-01T00:00:00", "1.00000000", "1.00000000", 1, source="legacy", event_type="demo")])
        _, empty = aggregate_hourly(legacy, self.config)
        self.assertEqual(empty.count(), 0)

    def test_decimal_range_failure_is_explicit(self):
        data = self.frame([self.row("too-precise", "2026-01-01T00:00:00", "1.123456789", "1.00000000", 1)])
        with self.assertRaisesRegex(ValueError, "exact analytics decimal range"):
            aggregate_hourly(data, self.config)


if __name__ == "__main__":
    unittest.main()
