#!/usr/bin/env python3
"""Send sample JSON events to the local AetherFlow Kafka topic."""

import argparse
import json
import subprocess
from datetime import datetime, timezone


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--topic", default="aetherflow-events")
    parser.add_argument("--count", type=int, default=10)
    args = parser.parse_args()
    if args.count < 1:
        parser.error("--count must be at least 1")

    timestamp = datetime.now(timezone.utc).isoformat()
    records = [
        json.dumps(
            {
                "event_id": f"local-{timestamp}-{index:04d}",
                "event_time": timestamp,
                "entity_id": f"entity-{index % 3 + 1}",
                "event_type": "sample",
                "value": index,
            },
            separators=(",", ":"),
        )
        for index in range(1, args.count + 1)
    ]

    subprocess.run(
        [
            "docker",
            "compose",
            "exec",
            "-T",
            "kafka",
            "kafka-console-producer",
            "--bootstrap-server",
            "kafka:29092",
            "--topic",
            args.topic,
        ],
        input="\n".join(records) + "\n",
        text=True,
        check=True,
    )
    print(f"Sent {len(records)} JSON events to topic {args.topic}")


if __name__ == "__main__":
    main()
