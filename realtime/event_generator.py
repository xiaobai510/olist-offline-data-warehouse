from __future__ import annotations

import argparse
from datetime import timedelta
import hashlib
import json
from pathlib import Path
import time
from typing import Any

import pandas as pd
from confluent_kafka import Producer


REALTIME_DIR = Path(__file__).resolve().parent
PROJECT_DIR = REALTIME_DIR.parent
DEFAULT_DWD_DIR = PROJECT_DIR / "data" / "dwd"
DEFAULT_TOPIC = "olist_order_events"


def native_value(value: Any):
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    if hasattr(value, "item"):
        return value.item()
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return value


def timestamp_text(value):
    if pd.isna(value):
        return None
    return pd.Timestamp(value).strftime("%Y-%m-%dT%H:%M:%S")


def make_event_id(order_id: str, event_type: str, event_time: str) -> str:
    raw = f"{order_id}|{event_type}|{event_time}".encode("utf-8")
    return hashlib.sha1(raw).hexdigest()


def load_order_data(dwd_dir: Path, limit: int) -> pd.DataFrame:
    order_path = dwd_dir / "fact_order"
    item_path = dwd_dir / "fact_order_item"

    if not order_path.exists() or not item_path.exists():
        raise FileNotFoundError(
            "找不到 fact_order 或 fact_order_item，请先运行 build_dwd.py"
        )

    orders = pd.read_parquet(
        order_path,
        columns=[
            "order_id",
            "customer_unique_id",
            "order_status",
            "customer_state",
            "order_purchase_timestamp",
            "order_approved_at",
            "order_delivered_carrier_timestamp",
            "order_delivered_customer_timestamp",
            "payment_amount",
        ],
    )
    items = pd.read_parquet(
        item_path,
        columns=[
            "order_id",
            "order_item_id",
            "seller_id",
            "price",
            "freight_value",
            "category_name_english",
        ],
    )
    item_summary = items.groupby("order_id", as_index=False).agg(
        item_count=("order_item_id", "count"),
        seller_id=("seller_id", "first"),
        category_name_english=("category_name_english", "first"),
        product_amount=("price", "sum"),
        freight_amount=("freight_value", "sum"),
    )

    frame = orders.merge(item_summary, on="order_id", how="left")
    frame = frame.sort_values("order_purchase_timestamp").reset_index(drop=True)
    if limit > 0:
        frame = frame.head(limit).copy()
    return frame


def build_events(frame: pd.DataFrame) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    definitions = [
        ("ORDER_CREATED", "order_purchase_timestamp", 1),
        ("PAYMENT_APPROVED", "order_approved_at", 2),
        ("SHIPPED", "order_delivered_carrier_timestamp", 3),
        ("DELIVERED", "order_delivered_customer_timestamp", 4),
    ]

    for row in frame.itertuples(index=False):
        base = {
            "schema_version": 1,
            "order_id": str(row.order_id),
            "order_status": native_value(row.order_status),
            "customer_unique_id": native_value(row.customer_unique_id),
            "customer_state": native_value(row.customer_state),
            "item_count": int(native_value(row.item_count) or 0),
            "product_amount": float(native_value(row.product_amount) or 0.0),
            "freight_amount": float(native_value(row.freight_amount) or 0.0),
            "payment_amount": float(native_value(row.payment_amount) or 0.0),
            "seller_id": native_value(row.seller_id),
            "category_name_english": native_value(
                row.category_name_english
            ),
        }

        for event_type, time_field, rank in definitions:
            event_time = getattr(row, time_field)
            if pd.isna(event_time):
                continue
            event_text = timestamp_text(event_time)
            events.append(
                {
                    **base,
                    "event_id": make_event_id(
                        base["order_id"], event_type, event_text
                    ),
                    "event_type": event_type,
                    "event_time": event_text,
                    "_event_rank": rank,
                }
            )

        if (
            row.order_status == "canceled"
            and not pd.isna(row.order_purchase_timestamp)
        ):
            event_time = pd.Timestamp(
                row.order_purchase_timestamp
            ) + timedelta(minutes=1)
            event_text = timestamp_text(event_time)
            events.append(
                {
                    **base,
                    "event_id": make_event_id(
                        base["order_id"], "CANCELED", event_text
                    ),
                    "event_type": "CANCELED",
                    "event_time": event_text,
                    "order_status": "canceled",
                    "_event_rank": 5,
                }
            )

    events.sort(
        key=lambda item: (
            pd.Timestamp(item["event_time"]),
            item["order_id"],
            item["_event_rank"],
        )
    )
    for event in events:
        event.pop("_event_rank", None)
    return events


def parse_args():
    parser = argparse.ArgumentParser(description="回放 Olist DWD 数据到 Kafka")
    parser.add_argument(
        "--dwd-dir", type=Path, default=DEFAULT_DWD_DIR
    )
    parser.add_argument(
        "--bootstrap-servers", default="127.0.0.1:19092"
    )
    parser.add_argument("--topic", default=DEFAULT_TOPIC)
    parser.add_argument(
        "--limit", type=int, default=5000, help="订单数，0 表示全部"
    )
    parser.add_argument("--speed", type=float, default=100000.0)
    parser.add_argument("--max-sleep", type=float, default=0.05)
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def main():
    args = parse_args()
    frame = load_order_data(args.dwd_dir, args.limit)
    events = build_events(frame)
    if not events:
        raise RuntimeError("没有生成任何事件")

    print(
        f"orders={len(frame):,} events={len(events):,} "
        f"topic={args.topic}"
    )
    print("first event:", json.dumps(events[0], ensure_ascii=False))
    if args.dry_run:
        for event in events[:5]:
            print(json.dumps(event, ensure_ascii=False))
        return

    producer = Producer(
        {
            "bootstrap.servers": args.bootstrap_servers,
            "acks": "all",
            "compression.type": "gzip",
            "linger.ms": 5,
            "retries": 5,
            "request.timeout.ms": 30000,
            "delivery.timeout.ms": 60000,
        }
    )

    previous_time = None
    sent = 0
    started = time.monotonic()
    delivery_errors = []

    def delivery_report(error, message):
        if error is not None:
            delivery_errors.append(str(error))

    try:
        for event in events:
            current_time = pd.Timestamp(event["event_time"])
            if previous_time is not None:
                delta_seconds = max(
                    0.0,
                    (current_time - previous_time).total_seconds(),
                )
                delay = min(
                    delta_seconds / max(args.speed, 1.0),
                    args.max_sleep,
                )
                if delay > 0:
                    time.sleep(delay)

            producer.produce(
                args.topic,
                key=event["order_id"].encode("utf-8"),
                value=json.dumps(
                    event,
                    ensure_ascii=False,
                    separators=(",", ":"),
                    allow_nan=False,
                ).encode("utf-8"),
                on_delivery=delivery_report,
            )
            producer.poll(0)
            previous_time = current_time
            sent += 1
            if sent % 1000 == 0:
                producer.flush(30)
                if delivery_errors:
                    raise RuntimeError(delivery_errors[-1])
                elapsed = time.monotonic() - started
                print(
                    f"sent={sent:,} elapsed={elapsed:.1f}s "
                    f"rate={sent / max(elapsed, 0.001):.0f}/s",
                    flush=True,
                )
    finally:
        producer.flush(30)

    if delivery_errors:
        raise RuntimeError(delivery_errors[-1])

    elapsed = time.monotonic() - started
    print(
        f"completed events={sent:,} elapsed={elapsed:.1f}s "
        f"average_rate={sent / max(elapsed, 0.001):.0f}/s"
    )


if __name__ == "__main__":
    main()

