import ast
import os
import sys
import tempfile
import unittest
from pathlib import Path

import yaml

REPOSITORY = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY / "scripts"))

from aetherflow_config import ConfigError, default_config_path, load_config
from produce_crypto_trades import parse_args


class AetherFlowConfigTest(unittest.TestCase):
    def test_default_config_is_resolved_independent_of_cwd(self):
        original = Path.cwd()
        with tempfile.TemporaryDirectory() as other_dir:
            try:
                os.chdir(other_dir)
                config = load_config(environ={})
            finally:
                os.chdir(original)
        self.assertEqual(default_config_path(), REPOSITORY / "config" / "aetherflow.yml")
        self.assertEqual(config["context"], "host")
        self.assertEqual(config["kafka"]["bootstrap_servers"], "localhost:9092")
        self.assertEqual(config["s3a"]["endpoint"], "http://localhost:8333")

    def test_compose_context_and_environment_then_cli_precedence(self):
        config = load_config(environ={
            "AETHERFLOW_CONTEXT": "compose", "KAFKA_TOPIC": "environment-topic",
            "KAFKA_BOOTSTRAP_SERVERS": "environment-broker", "BINANCE_SYMBOL": "ethusdt",
        })
        self.assertEqual(config["kafka"]["bootstrap_servers"], "environment-broker")
        self.assertEqual(config["context"], "compose")
        args = parse_args(config, ["--topic", "cli-topic", "--bootstrap-servers", "cli-broker", "--symbol", "xrpusdt"])
        self.assertEqual((args.topic, args.bootstrap_servers, args.symbol), ("cli-topic", "cli-broker", "xrpusdt"))
        self.assertEqual(parse_args(config, []).topic, "environment-topic")

    def test_context_endpoints_and_explicit_environment_endpoint_override(self):
        host = load_config(environ={"AETHERFLOW_CONTEXT": "host"})
        compose = load_config(environ={"AETHERFLOW_CONTEXT": "compose"})
        self.assertEqual((host["kafka"]["bootstrap_servers"], host["s3a"]["endpoint"], host["spark"]["master"]), ("localhost:9092", "http://localhost:8333", "spark://localhost:7077"))
        self.assertEqual((compose["kafka"]["bootstrap_servers"], compose["s3a"]["endpoint"], compose["spark"]["master"]), ("kafka:29092", "http://seaweedfs:8333", "spark://spark-master:7077"))
        override = load_config(environ={"AETHERFLOW_CONTEXT": "compose", "S3_ENDPOINT": "http://storage-proxy:8333", "AETHERFLOW_S3_ENDPOINT": "http://preferred-proxy:8333"})
        self.assertEqual(override["s3a"]["endpoint"], "http://preferred-proxy:8333")

    def test_explicit_config_path_and_environment_override(self):
        original = load_config(environ={})
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "custom.yml"
            custom = yaml.safe_load(default_config_path().read_text())
            custom["kafka"]["topics"]["events"] = "custom-events"
            path.write_text(yaml.safe_dump(custom))
            loaded = load_config(config_path=path, environ={"AETHERFLOW_CONTEXT": "compose", "AETHERFLOW_KAFKA_TOPIC": "env-events"})
        self.assertEqual(original["kafka"]["topics"]["events"], "aetherflow-events")
        self.assertEqual(loaded["kafka"]["topics"]["events"], "env-events")

    def test_aetherflow_config_environment_path_override(self):
        custom = yaml.safe_load(default_config_path().read_text())
        custom["kafka"]["topics"]["events"] = "selected-by-environment"
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "alternate.yml"
            path.write_text(yaml.safe_dump(custom))
            loaded = load_config(environ={"AETHERFLOW_CONTEXT": "host", "AETHERFLOW_CONFIG": str(path)})
        self.assertEqual(loaded["kafka"]["topics"]["events"], "selected-by-environment")

    def test_missing_key_and_invalid_type_have_actionable_errors(self):
        config = yaml.safe_load(default_config_path().read_text())
        del config["bronze"]["checkpoint"]
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "missing.yml"
            path.write_text(yaml.safe_dump(config))
            with self.assertRaisesRegex(ConfigError, "bronze.checkpoint"):
                load_config(config_path=path, environ={})
            config["bronze"]["checkpoint"] = "s3a://example/checkpoint"
            config["decimal"]["scale"] = "18"
            path.write_text(yaml.safe_dump(config))
            with self.assertRaisesRegex(ConfigError, "decimal.scale must be int"):
                load_config(config_path=path, environ={})

    def test_environment_override_type_and_context_validation(self):
        with self.assertRaisesRegex(ConfigError, "AETHERFLOW_CONTEXT"):
            load_config(environ={"AETHERFLOW_CONTEXT": "somewhere"})
        with self.assertRaisesRegex(ConfigError, "AETHERFLOW_AIRFLOW_RETRIES"):
            load_config(environ={"AETHERFLOW_AIRFLOW_RETRIES": "many"})

    def test_exact_production_and_policy_defaults(self):
        config = load_config(environ={})
        self.assertEqual(config["kafka"]["topics"]["events"], "aetherflow-events")
        self.assertEqual(config["bronze"]["output"], "s3a://aetherflow-data/bronze/events")
        self.assertEqual(config["bronze"]["checkpoint"], "s3a://aetherflow-checkpoints/bronze-events/")
        self.assertEqual(config["silver"]["output"], "s3a://aetherflow-data/silver/events")
        self.assertEqual(config["silver"]["quarantine"], "s3a://aetherflow-data/quarantine/silver-events")
        self.assertEqual((config["decimal"]["precision"], config["decimal"]["scale"], config["decimal"]["max_integer_digits"]), (38, 18, 20))
        self.assertTrue(config["decimal"]["positive_only"])
        self.assertFalse(config["decimal"]["allow_scientific_notation"])
        self.assertEqual((config["crypto_producer"]["symbol"], config["crypto_producer"]["duration_seconds"], config["crypto_producer"]["max_messages"], config["crypto_producer"]["reconnect_initial_seconds"], config["crypto_producer"]["reconnect_max_seconds"]), ("btcusdt", 0, 0, 1, 30))
        self.assertEqual((config["airflow"]["schedule"], config["airflow"]["catchup"], config["airflow"]["max_active_runs"], config["airflow"]["retries"], config["airflow"]["retry_delay_seconds"]), ("0 * * * *", False, 1, 1, 5))

    def test_all_spark_s3a_callers_use_shared_helper(self):
        callers = [
            REPOSITORY / "scripts/bronze_stream.py",
            REPOSITORY / "scripts/silver_transform.py",
            REPOSITORY / "aetherflow_delta_smoke_test.py",
            REPOSITORY / "tests/bronze_recovery_harness.py",
        ]
        for path in callers:
            source = path.read_text()
            tree = ast.parse(source)
            calls = [node for node in ast.walk(tree) if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "configure_s3a"]
            self.assertTrue(calls, str(path))
            self.assertNotIn('"spark.hadoop.fs.s3a.endpoint"', source, str(path))


if __name__ == "__main__":
    unittest.main()
