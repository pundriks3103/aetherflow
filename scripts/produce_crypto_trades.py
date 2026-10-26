#!/usr/bin/env python3
"""Publish live Binance Spot trade events to the existing Kafka topic."""

import argparse
import json
import logging
import os
import re
import time
from datetime import datetime, timezone

import websocket
from kafka import KafkaProducer


LOG = logging.getLogger("aetherflow.crypto_trades")
SYMBOL_RE = re.compile(r"^[a-z0-9]+$")


def iso_utc(milliseconds):
    return datetime.fromtimestamp(milliseconds / 1000, tz=timezone.utc).isoformat()


def normalize_trade(exchange_payload):
    """Convert a Binance raw trade message to the repository event envelope."""
    required = ("s", "t", "p", "q", "T")
    missing = [field for field in required if field not in exchange_payload]
    if missing:
        raise ValueError(f"trade message is missing fields: {','.join(missing)}")

    symbol = str(exchange_payload["s"]).lower()
    trade_id = str(exchange_payload["t"])
    return {
        "source": "binance",
        "event_type": "crypto_trade",
        "event_id": f"binance:{symbol}:{trade_id}",
        "trade_id": trade_id,
        "symbol": symbol,
        "price": str(exchange_payload["p"]),
        "quantity": str(exchange_payload["q"]),
        "event_time": iso_utc(exchange_payload["T"]),
        "exchange_event_time": iso_utc(exchange_payload.get("E", exchange_payload["T"])),
        "exchange_event_time_ms": exchange_payload.get("E"),
        "trade_time_ms": exchange_payload["T"],
        "is_buyer_maker": exchange_payload.get("m"),
        "exchange_payload": exchange_payload,
    }


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--symbol", default=os.environ.get("BINANCE_SYMBOL", "btcusdt"))
    parser.add_argument(
        "--topic", default=os.environ.get("KAFKA_TOPIC", "aetherflow-events")
    )
    parser.add_argument(
        "--bootstrap-servers",
        default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"),
    )
    parser.add_argument("--duration-seconds", type=int, default=0)
    parser.add_argument("--max-messages", type=int, default=0)
    parser.add_argument("--max-backoff-seconds", type=int, default=30)
    return parser.parse_args()


def main():
    args = parse_args()
    symbol = args.symbol.lower()
    if not SYMBOL_RE.fullmatch(symbol):
        raise SystemExit("--symbol must contain only lowercase letters and digits")
    if args.duration_seconds < 0 or args.max_messages < 0:
        raise SystemExit("duration and max-messages cannot be negative")

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_servers,
        key_serializer=lambda value: value.encode("utf-8"),
        value_serializer=lambda value: json.dumps(
            value, separators=(",", ":"), ensure_ascii=False
        ).encode("utf-8"),
        acks="all",
    )
    deadline = time.monotonic() + args.duration_seconds if args.duration_seconds else None
    stream_url = f"wss://stream.binance.com:9443/ws/{symbol}@trade"
    published = 0
    backoff = 1

    try:
        while deadline is None or time.monotonic() < deadline:
            ws = None
            try:
                ws = websocket.create_connection(stream_url, timeout=15)
                LOG.info("Connected to Binance trade stream symbol=%s", symbol)
                backoff = 1
                while deadline is None or time.monotonic() < deadline:
                    timeout = 15
                    if deadline is not None:
                        timeout = max(1, min(timeout, int(deadline - time.monotonic())))
                    ws.settimeout(timeout)
                    message = ws.recv()
                    if not message:
                        raise ConnectionError("Binance closed the WebSocket")
                    payload = normalize_trade(json.loads(message))
                    producer.send(args.topic, key=payload["event_id"], value=payload).get(
                        timeout=15
                    )
                    published += 1
                    if published == 1 or published % 100 == 0:
                        LOG.info("Published crypto trades count=%d topic=%s", published, args.topic)
                    if args.max_messages and published >= args.max_messages:
                        return
            except KeyboardInterrupt:
                LOG.info("Stopping after %d published trades", published)
                return
            except Exception as exc:  # reconnect around transient network failures
                if deadline is not None and time.monotonic() >= deadline:
                    break
                LOG.warning("Trade stream error: %s; reconnecting in %ss", exc, backoff)
                time.sleep(backoff)
                backoff = min(backoff * 2, args.max_backoff_seconds)
            finally:
                if ws is not None:
                    ws.close()
    finally:
        producer.flush()
        producer.close()
        LOG.info("Finished; published crypto trades count=%d", published)


if __name__ == "__main__":
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(message)s")
    main()
