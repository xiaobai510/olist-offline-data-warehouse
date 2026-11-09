from __future__ import annotations

import argparse
import json
from pathlib import Path
import time

from confluent_kafka import Consumer, KafkaError

from event_generator import build_events, load_order_data


def collect_metrics(bootstrap: str, topic: str, idle_seconds: float):
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": f"olist-realtime-validator-{int(time.time())}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([topic])

    metrics = []
    last_message = time.monotonic()
    try:
        while True:
            message = consumer.poll(0.5)
            if message is None:
                if time.monotonic() - last_message >= idle_seconds:
                    break
                continue
            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise RuntimeError(str(message.error()))
            metrics.append(json.loads(message.value().decode("utf-8")))
            last_message = time.monotonic()
    finally:
        consumer.close()
    return metrics


def expected_metrics(dwd_dir, limit_orders: int):
    frame = load_order_data(Path(dwd_dir), limit_orders)
    events = build_events(frame)
    trade_events = [
        event
        for event in events
        if event["event_type"] == "ORDER_CREATED"
        and event["order_status"] not in {"canceled", "unavailable"}
    ]
    payment_events = [
        event
        for event in events
        if event["event_type"] == "PAYMENT_APPROVED"
        and event["order_status"] not in {"canceled", "unavailable"}
    ]
    return {
        "trade_orders": len({event["order_id"] for event in trade_events}),
        "trade_gmv": sum(event["product_amount"] for event in trade_events),
        "payment_orders": len(
            {event["order_id"] for event in payment_events}
        ),
        "payment_amount": sum(
            event["payment_amount"] for event in payment_events
        ),
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap-servers", default="127.0.0.1:19092")
    parser.add_argument("--topic", default="olist_realtime_metrics")
    parser.add_argument("--idle-seconds", type=float, default=8.0)
    parser.add_argument("--limit-orders", type=int, default=100)
    parser.add_argument(
        "--dwd-dir",
        default=r"D:\pycharm\PythonProject\data\dwd",
    )
    args = parser.parse_args()

    metrics = collect_metrics(
        args.bootstrap_servers,
        args.topic,
        args.idle_seconds,
    )
    actual_trade = [
        metric for metric in metrics if metric.get("metric_type") == "trade"
    ]
    actual_payment = [
        metric
        for metric in metrics
        if metric.get("metric_type") == "payment"
    ]
    actual = {
        "trade_orders": sum(
            int(metric.get("event_order_count", 0))
            for metric in actual_trade
        ),
        "trade_gmv": sum(
            float(metric.get("product_gmv", 0))
            for metric in actual_trade
        ),
        "payment_orders": sum(
            int(metric.get("event_order_count", 0))
            for metric in actual_payment
        ),
        "payment_amount": sum(
            float(metric.get("payment_amount", 0))
            for metric in actual_payment
        ),
    }
    expected = expected_metrics(args.dwd_dir, args.limit_orders)

    print(f"metrics_received={len(metrics)}")
    print("expected=", expected)
    print("actual  =", actual)

    if actual["trade_orders"] != expected["trade_orders"]:
        raise AssertionError("trade order count mismatch")
    if abs(actual["trade_gmv"] - expected["trade_gmv"]) > 0.01:
        raise AssertionError("trade GMV mismatch")
    if actual["payment_orders"] != expected["payment_orders"]:
        raise AssertionError("payment order count mismatch")
    if abs(actual["payment_amount"] - expected["payment_amount"]) > 0.01:
        raise AssertionError("payment amount mismatch")
    print("realtime reconciliation PASS")


if __name__ == "__main__":
    main()

