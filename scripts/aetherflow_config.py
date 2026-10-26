"""Load and validate AetherFlow's shared, non-secret YAML configuration."""

import copy
import os
import sys
from pathlib import Path

import yaml


class ConfigError(ValueError):
    """Raised when the application configuration is missing or invalid."""


def default_config_path():
    return Path(__file__).resolve().parent.parent / "config" / "aetherflow.yml"


def _required(config, dotted_path, expected):
    value = config
    for part in dotted_path.split("."):
        if not isinstance(value, dict) or part not in value:
            raise ConfigError(f"Missing required configuration key: {dotted_path}")
        value = value[part]
    if not isinstance(value, expected) or (expected is int and isinstance(value, bool)):
        expected_name = " or ".join(t.__name__ for t in expected) if isinstance(expected, tuple) else expected.__name__
        raise ConfigError(f"Configuration key {dotted_path} must be {expected_name}")
    if isinstance(value, str) and not value.strip():
        raise ConfigError(f"Configuration key {dotted_path} cannot be empty")
    return value


def _set(config, dotted_path, value):
    target = config
    parts = dotted_path.split(".")
    for part in parts[:-1]:
        target = target[part]
    target[parts[-1]] = value


def _apply_env(config, environ):
    overrides = {
        "AETHERFLOW_KAFKA_TOPIC": ("kafka.topics.events", str),
        "KAFKA_TOPIC": ("kafka.topics.events", str),
        "AETHERFLOW_KAFKA_BOOTSTRAP_SERVERS": ("contexts.{context}.kafka_bootstrap_servers", str),
        "KAFKA_BOOTSTRAP_SERVERS": ("contexts.{context}.kafka_bootstrap_servers", str),
        "AETHERFLOW_BINANCE_SYMBOL": ("crypto_producer.symbol", str),
        "BINANCE_SYMBOL": ("crypto_producer.symbol", str),
        "AETHERFLOW_BINANCE_DURATION_SECONDS": ("crypto_producer.duration_seconds", int),
        "AETHERFLOW_BINANCE_MAX_MESSAGES": ("crypto_producer.max_messages", int),
        "AETHERFLOW_BINANCE_RECONNECT_INITIAL_SECONDS": ("crypto_producer.reconnect_initial_seconds", int),
        "AETHERFLOW_BINANCE_RECONNECT_MAX_SECONDS": ("crypto_producer.reconnect_max_seconds", int),
        "AETHERFLOW_BRONZE_OUTPUT": ("bronze.output", str),
        "AETHERFLOW_BRONZE_CHECKPOINT": ("bronze.checkpoint", str),
        "AETHERFLOW_BRONZE_TRIGGER_SECONDS": ("bronze.trigger_seconds", int),
        "AETHERFLOW_SILVER_OUTPUT": ("silver.output", str),
        "AETHERFLOW_SILVER_QUARANTINE": ("silver.quarantine", str),
        "AETHERFLOW_ANALYTICS_OUTPUT": ("analytics.output", str),
        "AETHERFLOW_ANALYTICS_WINDOW_MINUTES": ("analytics.window_minutes", int),
        "AETHERFLOW_DECIMAL_PRECISION": ("decimal.precision", int),
        "AETHERFLOW_DECIMAL_SCALE": ("decimal.scale", int),
        "AETHERFLOW_DECIMAL_MAX_INTEGER_DIGITS": ("decimal.max_integer_digits", int),
        "AETHERFLOW_AIRFLOW_SCHEDULE": ("airflow.schedule", str),
        "AETHERFLOW_AIRFLOW_CATCHUP": ("airflow.catchup", "bool"),
        "AETHERFLOW_AIRFLOW_MAX_ACTIVE_RUNS": ("airflow.max_active_runs", int),
        "AETHERFLOW_AIRFLOW_RETRIES": ("airflow.retries", int),
        "AETHERFLOW_AIRFLOW_RETRY_DELAY_SECONDS": ("airflow.retry_delay_seconds", int),
        "AETHERFLOW_S3_ENDPOINT": ("contexts.{context}.s3_endpoint", str),
        "S3_ENDPOINT": ("contexts.{context}.s3_endpoint", str),
        "AETHERFLOW_SPARK_MASTER": ("contexts.{context}.spark_master", str),
        "AETHERFLOW_MAKE_CRYPTO_DURATION_SECONDS": ("make.crypto_duration_seconds", int),
        "AETHERFLOW_SMOKE_TEST_OUTPUT": ("smoke_test.output", str),
        "AETHERFLOW_GENERIC_EVENT_COUNT": ("generic_producer.count", int),
    }
    for name, (path, converter) in reversed(list(overrides.items())):
        if name not in environ:
            continue
        actual_path = path.format(context=config["context"])
        raw = environ[name]
        try:
            if converter == "bool":
                lowered = raw.lower()
                if lowered not in ("true", "false"):
                    raise ValueError("expected true or false")
                value = lowered == "true"
            else:
                value = converter(raw)
        except (TypeError, ValueError) as exc:
            raise ConfigError(f"Environment variable {name} has invalid value: {exc}") from exc
        _set(config, actual_path, value)
    if "S3_BUCKET_NAME" in environ and "AETHERFLOW_SMOKE_TEST_OUTPUT" not in environ:
        config["smoke_test"]["output"] = f"s3a://{environ['S3_BUCKET_NAME']}/bronze/smoke-test"


def validate_config(config):
    if not isinstance(config, dict):
        raise ConfigError("Configuration root must be a YAML mapping")
    for context in ("host", "compose"):
        for key in ("kafka_bootstrap_servers", "s3_endpoint", "spark_master"):
            _required(config, f"contexts.{context}.{key}", str)
    _required(config, "spark.cores_max", int)
    _required(config, "kafka.topics.events", str)
    _required(config, "crypto_producer.symbol", str)
    _required(config, "crypto_producer.duration_seconds", int)
    _required(config, "crypto_producer.max_messages", int)
    _required(config, "crypto_producer.reconnect_initial_seconds", int)
    _required(config, "crypto_producer.reconnect_max_seconds", int)
    _required(config, "crypto_producer.websocket_url", str)
    for key in ("output", "checkpoint"):
        _required(config, f"bronze.{key}", str)
    for key in ("output", "quarantine"):
        _required(config, f"silver.{key}", str)
    _required(config, "analytics.output", str)
    for key in ("window_minutes", "price_precision", "price_scale", "quantity_precision", "quantity_scale", "vwap_precision", "vwap_scale"):
        _required(config, f"analytics.{key}", int)
    for key in ("precision", "scale", "max_integer_digits"):
        _required(config, f"decimal.{key}", int)
    _required(config, "decimal.positive_only", bool)
    _required(config, "decimal.allow_scientific_notation", bool)
    _required(config, "airflow.dag_id", str)
    _required(config, "airflow.schedule", str)
    _required(config, "airflow.catchup", bool)
    for key in ("max_active_runs", "retries", "retry_delay_seconds"):
        _required(config, f"airflow.{key}", int)
    _required(config, "s3a.path_style_access", bool)
    _required(config, "s3a.ssl_enabled", bool)
    _required(config, "s3a.filesystem", str)
    _required(config, "s3a.credentials_provider", str)
    _required(config, "smoke_test.output", str)
    _required(config, "tests.recovery_root", str)
    _required(config, "make.crypto_duration_seconds", int)
    _required(config, "generic_producer.count", int)
    _required(config, "bronze.trigger_seconds", int)
    if not 1 <= config["decimal"]["scale"] <= config["decimal"]["precision"] <= 38:
        raise ConfigError("decimal precision/scale must satisfy 1 <= scale <= precision <= 38")
    if config["decimal"]["allow_scientific_notation"]:
        raise ConfigError("decimal.allow_scientific_notation must remain false until exponent precision rules are implemented")
    if config["decimal"]["max_integer_digits"] > config["decimal"]["precision"] - config["decimal"]["scale"]:
        raise ConfigError("decimal.max_integer_digits exceeds precision minus scale")
    if config["decimal"]["max_integer_digits"] < 1:
        raise ConfigError("decimal.max_integer_digits must be positive")
    analytics = config["analytics"]
    if analytics["window_minutes"] < 1:
        raise ConfigError("analytics.window_minutes must be positive")
    for name in ("price", "quantity", "vwap"):
        precision, scale = analytics[f"{name}_precision"], analytics[f"{name}_scale"]
        if not 1 <= scale <= precision <= 38:
            raise ConfigError(f"analytics {name} precision/scale must satisfy 1 <= scale <= precision <= 38")
    if analytics["price_precision"] + analytics["quantity_precision"] + 1 > 38:
        raise ConfigError("analytics price and quantity precision leave no room for exact decimal multiplication")
    if config["crypto_producer"]["duration_seconds"] < 0 or config["crypto_producer"]["max_messages"] < 0:
        raise ConfigError("crypto producer duration and max_messages cannot be negative")
    if config["crypto_producer"]["reconnect_initial_seconds"] < 1 or config["crypto_producer"]["reconnect_max_seconds"] < config["crypto_producer"]["reconnect_initial_seconds"]:
        raise ConfigError("crypto reconnect backoff must be positive and max must be >= initial")
    if config["airflow"]["max_active_runs"] < 1 or config["airflow"]["retries"] < 0 or config["airflow"]["retry_delay_seconds"] < 0:
        raise ConfigError("Airflow active runs must be positive; retries and retry delay cannot be negative")
    if config["make"]["crypto_duration_seconds"] < 0:
        raise ConfigError("make.crypto_duration_seconds cannot be negative")
    if config["generic_producer"]["count"] < 1 or config["bronze"]["trigger_seconds"] < 1:
        raise ConfigError("generic producer count and Bronze trigger interval must be positive")
    return config


def load_config(context=None, environ=None, config_path=None):
    """Load YAML, then environment overrides. CLI callers apply arguments last."""
    env = os.environ if environ is None else environ
    selected_context = context or env.get("AETHERFLOW_CONTEXT", "host")
    if selected_context not in ("host", "compose"):
        raise ConfigError("AETHERFLOW_CONTEXT must be 'host' or 'compose'")
    path = Path(config_path or env.get("AETHERFLOW_CONFIG") or default_config_path()).expanduser()
    if not path.is_absolute():
        path = Path.cwd() / path
    try:
        with path.open(encoding="utf-8") as stream:
            raw = yaml.safe_load(stream)
    except FileNotFoundError as exc:
        raise ConfigError(f"Configuration file not found: {path}") from exc
    except yaml.YAMLError as exc:
        raise ConfigError(f"Invalid YAML configuration at {path}: {exc}") from exc
    config = copy.deepcopy(raw)
    if not isinstance(config, dict):
        raise ConfigError("Configuration root must be a YAML mapping")
    config["context"] = selected_context
    validate_config(config)
    _apply_env(config, env)
    validate_config(config)
    config["kafka"]["bootstrap_servers"] = config["contexts"][selected_context]["kafka_bootstrap_servers"]
    config["s3a"]["endpoint"] = config["contexts"][selected_context]["s3_endpoint"]
    config["spark"]["master"] = config["contexts"][selected_context]["spark_master"]
    return config


def configure_s3a(builder, config, environ=None):
    """Apply the shared S3A transport and credential settings to a Spark builder."""
    env = os.environ if environ is None else environ
    access_key = env.get("AWS_ACCESS_KEY_ID")
    secret_key = env.get("AWS_SECRET_ACCESS_KEY")
    if not access_key or not secret_key:
        raise ConfigError("AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY are missing")
    s3 = config["s3a"]
    return (builder.config("spark.hadoop.fs.s3a.endpoint", s3["endpoint"])
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", str(s3["path_style_access"]).lower())
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", str(s3["ssl_enabled"]).lower())
        .config("spark.hadoop.fs.s3a.impl", s3["filesystem"])
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", s3["credentials_provider"]))


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("usage: aetherflow_config.py dotted.config.key")
    result = load_config()
    for component in sys.argv[1].split("."):
        result = result[component]
    print(str(result).lower() if isinstance(result, bool) else result)
