from __future__ import annotations

import argparse
import json
from pathlib import Path
import time

from confluent_kafka import Consumer, KafkaError


def parse_args():
    parser = argparse.ArgumentParser(description="消费 Flink 输出的实时指标")
    parser.add_argument("--bootstrap-servers", default="127.0.0.1:19092")
    parser.add_argument("--topic", default="olist_realtime_metrics")
    parser.add_argument("--from-beginning", action="store_true")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--output", type=Path, default=None)
    return parser.parse_args()


def format_metric(metric: dict) -> str:
    metric_type = metric.get("metric_type", "unknown")
    if metric_type == "trade":
        return (
            f"[TRADE] {metric.get('window_start')} "
            f"orders={metric.get('event_order_count', 0)} "
            f"items={metric.get('item_count', 0)} "
            f"GMV={float(metric.get('product_gmv', 0)):.2f} "
            f"freight={float(metric.get('freight_amount', 0)):.2f}"
        )
    if metric_type == "payment":
        return (
            f"[PAYMENT] {metric.get('window_start')} "
            f"paid_orders={metric.get('event_order_count', 0)} "
            f"payments={metric.get('event_count', 0)} "
            f"amount={float(metric.get('payment_amount', 0)):.2f}"
        )
    return json.dumps(metric, ensure_ascii=False, indent=2)


def main():
    args = parse_args()
    consumer = Consumer(
        {
            "bootstrap.servers": args.bootstrap_servers,
            "group.id": f"olist-metric-printer-{int(time.time())}",
            "auto.offset.reset": (
                "earliest" if args.from_beginning else "latest"
            ),
            "enable.auto.commit": True,
            "auto.commit.interval.ms": 1000,
        }
    )
    consumer.subscribe([args.topic])

    output_handle = None
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        output_handle = args.output.open("a", encoding="utf-8")

    count = 0
    print(
        f"listening topic={args.topic} "
        f"bootstrap={args.bootstrap_servers}"
    )
    try:
        while args.limit <= 0 or count < args.limit:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise RuntimeError(str(message.error()))
            metric = json.loads(message.value().decode("utf-8"))
            print(format_metric(metric), flush=True)
            if output_handle:
                output_handle.write(
                    json.dumps(metric, ensure_ascii=False) + "\n"
                )
                output_handle.flush()
            count += 1
    finally:
        consumer.close()
        if output_handle:
            output_handle.close()
    print(f"consumed={count}")


if __name__ == "__main__":
    main()

