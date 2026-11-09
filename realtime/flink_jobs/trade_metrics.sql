SET 'execution.runtime-mode' = 'streaming';
SET 'execution.target' = 'remote';
SET 'rest.address' = '127.0.0.1';
SET 'rest.port' = '18081';
SET 'table.dml-sync' = 'false';
SET 'pipeline.name' = 'olist-realtime-trade-metrics-v1';
SET 'execution.checkpointing.interval' = '10 s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
SET 'restart-strategy.type' = 'fixed-delay';
SET 'restart-strategy.fixed-delay.attempts' = '10';
SET 'restart-strategy.fixed-delay.delay' = '5 s';

CREATE TABLE kafka_order_events (
    event_id STRING,
    event_type STRING,
    event_time TIMESTAMP(3),
    schema_version INT,
    order_id STRING,
    order_status STRING,
    customer_unique_id STRING,
    customer_state STRING,
    item_count INT,
    product_amount DOUBLE,
    freight_amount DOUBLE,
    payment_amount DOUBLE,
    seller_id STRING,
    category_name_english STRING,
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'olist_order_events',
    'properties.bootstrap.servers' = '127.0.0.1:19092',
    'properties.group.id' = 'flink-olist-metrics-v1',
    'scan.startup.mode' = 'latest-offset',
    'properties.auto.offset.reset' = 'latest',
    'format' = 'json',
    'json.timestamp-format.standard' = 'ISO-8601',
    'json.ignore-parse-errors' = 'true'
);

CREATE TABLE kafka_realtime_metrics (
    window_start TIMESTAMP(3),
    window_end TIMESTAMP(3),
    metric_type STRING,
    event_order_count BIGINT,
    event_count BIGINT,
    item_count BIGINT,
    product_gmv DOUBLE,
    freight_amount DOUBLE,
    payment_amount DOUBLE
) WITH (
    'connector' = 'kafka',
    'topic' = 'olist_realtime_metrics',
    'properties.bootstrap.servers' = '127.0.0.1:19092',
    'format' = 'json',
    'json.timestamp-format.standard' = 'ISO-8601',
    'sink.delivery-guarantee' = 'at-least-once',
    'properties.linger.ms' = '10',
    'properties.compression.type' = 'gzip'
);

CREATE TEMPORARY VIEW normalized_events AS
SELECT
    event_id,
    event_type,
    event_time,
    order_id,
    item_count,
    product_amount,
    freight_amount,
    payment_amount,
    CASE
        WHEN event_type = 'ORDER_CREATED' THEN 'trade'
        WHEN event_type = 'PAYMENT_APPROVED' THEN 'payment'
    END AS metric_type
FROM kafka_order_events
WHERE event_type IN ('ORDER_CREATED', 'PAYMENT_APPROVED')
  AND order_status NOT IN ('canceled', 'unavailable');

CREATE TEMPORARY VIEW metric_windows AS
SELECT
    window_start,
    window_end,
    metric_type,
    COUNT(DISTINCT order_id) AS event_order_count,
    COUNT(*) AS event_count,
    SUM(
        CASE WHEN metric_type = 'trade' THEN item_count ELSE 0 END
    ) AS item_count,
    SUM(
        CASE WHEN metric_type = 'trade' THEN product_amount ELSE 0 END
    ) AS product_gmv,
    SUM(
        CASE WHEN metric_type = 'trade' THEN freight_amount ELSE 0 END
    ) AS freight_amount,
    SUM(
        CASE WHEN metric_type = 'payment' THEN payment_amount ELSE 0 END
    ) AS payment_amount
FROM TABLE(
    TUMBLE(
        TABLE normalized_events,
        DESCRIPTOR(event_time),
        INTERVAL '10' SECOND
    )
)
GROUP BY window_start, window_end, metric_type;

INSERT INTO kafka_realtime_metrics
SELECT
    window_start,
    window_end,
    metric_type,
    event_order_count,
    event_count,
    item_count,
    product_gmv,
    freight_amount,
    payment_amount
FROM metric_windows;

