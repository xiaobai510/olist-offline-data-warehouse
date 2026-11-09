CREATE DATABASE IF NOT EXISTS olist;

CREATE TABLE IF NOT EXISTS olist.realtime_metrics
(
    window_start DateTime64(3),
    window_end DateTime64(3),
    metric_type LowCardinality(String),
    event_order_count UInt64,
    event_count UInt64,
    item_count UInt64,
    product_gmv Float64,
    freight_amount Float64,
    payment_amount Float64,
    ingested_at DateTime64(3) DEFAULT now64(3),
    _version UInt64 MATERIALIZED toUnixTimestamp64Milli(window_end)
)
ENGINE = ReplacingMergeTree(_version)
PARTITION BY toYYYYMM(window_start)
ORDER BY (metric_type, window_start, window_end);

