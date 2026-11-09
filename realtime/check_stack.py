from __future__ import annotations

import argparse
import json
import sys
import urllib.request

from confluent_kafka.admin import AdminClient


def check_kafka(bootstrap: str):
    admin = AdminClient({"bootstrap.servers": bootstrap})
    metadata = admin.list_topics(timeout=10)
    topics = sorted(metadata.topics.keys())
    print("Kafka OK")
    print("topics:", ", ".join(topics))
    return topics


def check_flink(rest_url: str):
    with urllib.request.urlopen(
        rest_url.rstrip("/") + "/overview", timeout=10
    ) as response:
        payload = json.load(response)
    print("Flink OK")
    print(
        "taskmanagers={taskmanagers} slots_total={slots-total} "
        "slots_available={slots-available}".format(**payload)
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap-servers", default="127.0.0.1:19092")
    parser.add_argument("--flink-rest", default="http://127.0.0.1:18081")
    args = parser.parse_args()
    try:
        topics = check_kafka(args.bootstrap_servers)
        check_flink(args.flink_rest)
        required = {"olist_order_events", "olist_realtime_metrics"}
        missing = required.difference(topics)
        if missing:
            print("missing topics:", ", ".join(sorted(missing)))
    except Exception as error:
        print(f"stack check failed: {error}", file=sys.stderr)
        raise


if __name__ == "__main__":
    main()

