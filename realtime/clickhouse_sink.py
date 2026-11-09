from __future__ import annotations

import argparse
import json
from pathlib import Path
import time
from urllib.parse import quote
from urllib.request import Request, urlopen

from confluent_kafka import Consumer, KafkaError


REALTIME_ROOT = Path(__file__).resolve().parent


def parse_args():
    parser = argparse.ArgumentParser(
        description="将 Kafka 实时指标幂等写入 ClickHouse"
    )
    parser.add_argument("--bootstrap-servers", default="127.0.0.1:19092")
    parser.add_argument("--topic", default="olist_realtime_metrics")
    parser.add_argument("--clickhouse-url", default="http://127.0.0.1:8123")
    parser.add_argument("--batch-size", type=int, default=50)
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--idle-seconds", type=float, default=30.0)
    parser.add_argument("--output-jsonl", type=Path, default=None)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--init-schema", action="store_true")
    return parser.parse_args()


def normalize_metric(metric: dict) -> dict:
    normalized = dict(metric)
    for field in ("window_start", "window_end"):
        value = normalized.get(field)
        if isinstance(value, str):
            normalized[field] = (
                value.replace("T", " ").replace("Z", "").split(".")[0]
            )
    for field in (
        "event_order_count",
        "event_count",
        "item_count",
    ):
        normalized[field] = int(normalized.get(field) or 0)
    for field in ("product_gmv", "freight_amount", "payment_amount"):
        normalized[field] = float(normalized.get(field) or 0.0)
    return normalized


def clickhouse_query(base_url: str, query: str, body: bytes = b""):
    url = f"{base_url.rstrip('/')}/?query={quote(query)}"
    request = Request(
        url,
        data=body,
        method="POST",
        headers={"Content-Type": "application/octet-stream"},
    )
    with urlopen(request, timeout=30) as response:
        return response.read().decode("utf-8")


def ensure_schema(base_url: str):
    schema = (REALTIME_ROOT / "clickhouse" / "init.sql").read_text(
        encoding="utf-8"
    )
    for statement in schema.split(";"):
        statement = statement.strip()
        if statement:
            clickhouse_query(base_url, statement)
    print("ClickHouse schema ready")


def insert_metrics(base_url: str, metrics: list[dict]):
    body = "\n".join(json.dumps(item, ensure_ascii=False) for item in metrics)
    query = (
        "INSERT INTO olist.realtime_metrics "
        "(window_start, window_end, metric_type, event_order_count, "
        "event_count, item_count, product_gmv, freight_amount, payment_amount) "
        "FORMAT JSONEachRow"
    )
    clickhouse_query(base_url, query, body.encode("utf-8"))


def main():
    args = parse_args()
    if args.init_schema and not args.dry_run:
        ensure_schema(args.clickhouse_url)

    consumer = Consumer(
        {
            "bootstrap.servers": args.bootstrap_servers,
            "group.id": f"olist-clickhouse-sink-{int(time.time())}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([args.topic])

    output_handle = None
    if args.output_jsonl:
        args.output_jsonl.parent.mkdir(parents=True, exist_ok=True)
        output_handle = args.output_jsonl.open("a", encoding="utf-8")

    batch = []
    total = 0
    last_message = time.monotonic()
    try:
        while args.limit <= 0 or total < args.limit:
            message = consumer.poll(0.5)
            if message is None:
                if (
                    batch
                    and time.monotonic() - last_message >= 1.0
                ):
                    if not args.dry_run:
                        insert_metrics(args.clickhouse_url, batch)
                    consumer.commit(asynchronous=False)
                    batch.clear()
                if time.monotonic() - last_message >= args.idle_seconds:
                    break
                continue
            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise RuntimeError(str(message.error()))

            metric = normalize_metric(
                json.loads(message.value().decode("utf-8"))
            )
            batch.append(metric)
            total += 1
            last_message = time.monotonic()
            if output_handle:
                output_handle.write(
                    json.dumps(metric, ensure_ascii=False) + "\n"
                )
                output_handle.flush()

            if len(batch) >= args.batch_size:
                if not args.dry_run:
                    insert_metrics(args.clickhouse_url, batch)
                consumer.commit(asynchronous=False)
                print(
                    f"processed total={total:,} batch={len(batch):,} "
                    f"dry_run={args.dry_run}",
                    flush=True,
                )
                batch.clear()

        if batch:
            if not args.dry_run:
                insert_metrics(args.clickhouse_url, batch)
            consumer.commit(asynchronous=False)
            print(
                f"processed total={total:,} batch={len(batch):,} "
                f"dry_run={args.dry_run}"
            )
    finally:
        consumer.close()
        if output_handle:
            output_handle.close()

    print(f"completed metrics={total:,} dry_run={args.dry_run}")


if __name__ == "__main__":
    main()

