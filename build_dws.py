from datetime import datetime
import os
from pathlib import Path

from pyspark.sql import DataFrame, Window, functions as F

from spark_env import get_spark


PROJECT_DIR = Path(__file__).resolve().parent
DWD_DIR = PROJECT_DIR / "data" / "dwd"
DWS_DIR = Path(
    os.environ.get("OLIST_DWS_DIR", str(PROJECT_DIR / "data" / "dws"))
)
PROCESSED_AT = datetime.now().isoformat(timespec="seconds")

INVALID_TRADE_STATUSES = ("canceled", "unavailable")


def read_dwd(table_name: str) -> DataFrame:
    return spark.read.parquet(str(DWD_DIR / table_name))


def write_table(df: DataFrame, table_name: str):
    target = DWS_DIR / table_name
    df.write.mode("overwrite").parquet(str(target))
    print(f"{table_name:30s} rows={df.count():>10,} columns={len(df.columns):>3}")


def valid_trade_condition():
    return ~F.col("order_status").isin(*INVALID_TRADE_STATUSES)


def zero(scale: str = "decimal(18,2)"):
    return F.lit(0).cast(scale)


def build_trade_day(fact_order: DataFrame) -> DataFrame:
    valid = valid_trade_condition()

    result = (
        fact_order
        .filter(F.col("order_purchase_date").isNotNull())
        .groupBy("order_purchase_date")
        .agg(
            F.countDistinct("order_id").alias("order_count"),
            F.countDistinct(
                F.when(valid, F.col("order_id"))
            ).alias("valid_order_count"),
            F.countDistinct(
                F.when(F.col("order_status") == "canceled", F.col("order_id"))
            ).alias("canceled_order_count"),
            F.countDistinct(
                F.when(
                    valid & (F.col("payment_row_count") > 0),
                    F.col("order_id"),
                )
            ).alias("paid_order_count"),
            F.sum(F.col("item_row_count")).alias("item_row_count"),
            F.sum(
                F.when(valid, F.col("product_amount")).otherwise(zero())
            ).alias("product_gmv"),
            F.sum(
                F.when(valid, F.col("freight_amount")).otherwise(zero())
            ).alias("freight_revenue"),
            F.sum(
                F.when(valid, F.col("payment_amount")).otherwise(zero())
            ).alias("payment_amount"),
            F.sum(
                F.when(
                    valid & F.col("is_late_delivery").eqNullSafe(True),
                    F.lit(1),
                ).otherwise(F.lit(0))
            ).alias("late_delivered_order_count"),
        )
        .withColumn(
            "average_order_value",
            F.when(
                F.col("valid_order_count") > 0,
                F.col("product_gmv") / F.col("valid_order_count"),
            )
            .otherwise(zero())
            .cast("decimal(18,2)"),
        )
        .withColumn(
            "average_payment_value",
            F.when(
                F.col("paid_order_count") > 0,
                F.col("payment_amount") / F.col("paid_order_count"),
            )
            .otherwise(zero())
            .cast("decimal(18,2)"),
        )
        .withColumn(
            "cancel_rate",
            F.when(
                F.col("order_count") > 0,
                F.col("canceled_order_count") / F.col("order_count"),
            )
            .otherwise(F.lit(0.0))
            .cast("decimal(10,6)"),
        )
        .withColumn("year", F.year("order_purchase_date"))
        .withColumn("year_month", F.date_format("order_purchase_date", "yyyy-MM"))
        .withColumn("_processed_at", F.lit(PROCESSED_AT))
        .orderBy("order_purchase_date")
    )
    write_table(result, "trade_day")
    return result


def build_product_day(fact_order_item: DataFrame) -> DataFrame:
    result = (
        fact_order_item
        .filter(
            F.col("order_purchase_date").isNotNull()
            & valid_trade_condition()
        )
        .groupBy(
            "order_purchase_date",
            "product_id",
            "product_category_name",
            "category_name_english",
        )
        .agg(
            F.countDistinct("order_id").alias("order_count"),
            F.count("*").alias("item_count"),
            F.countDistinct("seller_id").alias("seller_count"),
            F.sum("price").cast("decimal(18,2)").alias("product_gmv"),
            F.sum("freight_value").cast("decimal(18,2)").alias(
                "freight_revenue"
            ),
            F.sum("item_total_amount").cast("decimal(18,2)").alias(
                "total_item_amount"
            ),
        )
        .withColumn(
            "average_item_price",
            F.when(
                F.col("item_count") > 0,
                F.col("product_gmv") / F.col("item_count"),
            )
            .otherwise(zero())
            .cast("decimal(18,2)"),
        )
        .withColumn("year", F.year("order_purchase_date"))
        .withColumn("year_month", F.date_format("order_purchase_date", "yyyy-MM"))
        .withColumn("_processed_at", F.lit(PROCESSED_AT))
    )
    write_table(result, "product_day")
    return result


def build_seller_day(
    fact_order_item: DataFrame,
    fact_order: DataFrame,
) -> DataFrame:
    order_delivery = fact_order.select(
        "order_id",
        "order_delivered_customer_timestamp",
        "is_late_delivery",
    )

    result = (
        fact_order_item
        .join(order_delivery, on="order_id", how="left")
        .filter(
            F.col("order_purchase_date").isNotNull()
            & valid_trade_condition()
        )
        .groupBy(
            "order_purchase_date",
            "seller_id",
            "seller_state",
        )
        .agg(
            F.countDistinct("order_id").alias("order_count"),
            F.count("*").alias("item_count"),
            F.countDistinct("product_id").alias("product_count"),
            F.sum("price").cast("decimal(18,2)").alias("product_gmv"),
            F.sum("freight_value").cast("decimal(18,2)").alias(
                "freight_revenue"
            ),
            F.countDistinct(
                F.when(
                    F.col("order_delivered_customer_timestamp").isNotNull(),
                    F.col("order_id"),
                )
            ).alias("delivered_order_count"),
            F.countDistinct(
                F.when(
                    F.col("is_late_delivery").eqNullSafe(True),
                    F.col("order_id"),
                )
            ).alias("late_delivered_order_count"),
        )
        .withColumn(
            "late_delivery_rate",
            F.when(
                F.col("delivered_order_count") > 0,
                F.col("late_delivered_order_count")
                / F.col("delivered_order_count"),
            )
            .otherwise(F.lit(0.0))
            .cast("decimal(10,6)"),
        )
        .withColumn("year", F.year("order_purchase_date"))
        .withColumn("year_month", F.date_format("order_purchase_date", "yyyy-MM"))
        .withColumn("_processed_at", F.lit(PROCESSED_AT))
    )
    write_table(result, "seller_day")
    return result


def build_customer_rfm(fact_order: DataFrame) -> DataFrame:
    valid_orders = (
        fact_order
        .filter(
            F.col("order_purchase_date").isNotNull()
            & F.col("customer_unique_id").isNotNull()
            & valid_trade_condition()
        )
    )

    analysis_date = valid_orders.agg(
        F.max("order_purchase_date").alias("analysis_date")
    ).first().analysis_date

    if analysis_date is None:
        raise ValueError("没有有效订单，无法计算 RFM")

    customer_base = (
        valid_orders
        .groupBy("customer_unique_id")
        .agg(
            F.min("order_purchase_date").alias("first_purchase_date"),
            F.max("order_purchase_date").alias("last_purchase_date"),
            F.countDistinct("order_id").alias("frequency"),
            F.sum("product_amount").cast("decimal(18,2)").alias(
                "product_amount"
            ),
            F.sum("freight_amount").cast("decimal(18,2)").alias(
                "freight_amount"
            ),
            F.sum("payment_amount").cast("decimal(18,2)").alias("monetary"),
        )
        .withColumn(
            "recency_days",
            F.datediff(F.lit(analysis_date), F.col("last_purchase_date")),
        )
        .withColumn(
            "average_order_value",
            (F.col("monetary") / F.col("frequency")).cast("decimal(18,2)"),
        )
    )

    r_window = Window.orderBy(F.desc("recency_days"))
    f_window = Window.orderBy(F.desc("frequency"))
    m_window = Window.orderBy(F.desc("monetary"))

    result = (
        customer_base
        .withColumn("r_score", F.ntile(5).over(r_window))
        .withColumn("f_score", F.ntile(5).over(f_window))
        .withColumn("m_score", F.ntile(5).over(m_window))
        .withColumn(
            "rfm_segment",
            F.when(
                (F.col("r_score") >= 4)
                & (F.col("f_score") >= 4)
                & (F.col("m_score") >= 4),
                F.lit("champion"),
            )
            .when(
                (F.col("r_score") >= 4) & (F.col("f_score") >= 3),
                F.lit("loyal"),
            )
            .when(
                (F.col("r_score") >= 4) & (F.col("f_score") == 1),
                F.lit("new_customer"),
            )
            .when(
                F.col("r_score") >= 4,
                F.lit("potential_loyal"),
            )
            .when(
                (F.col("r_score") <= 2) & (F.col("f_score") >= 3),
                F.lit("at_risk"),
            )
            .when(
                (F.col("r_score") <= 2) & (F.col("f_score") <= 2),
                F.lit("lost"),
            )
            .otherwise(F.lit("regular")),
        )
        .withColumn("analysis_date", F.lit(analysis_date))
        .withColumn("_processed_at", F.lit(PROCESSED_AT))
    )
    write_table(result, "customer_rfm")
    return result


def build_delivery_day(fact_order: DataFrame) -> DataFrame:
    delivered = F.col("order_delivered_customer_timestamp").isNotNull()

    result = (
        fact_order
        .filter(F.col("order_purchase_date").isNotNull())
        .groupBy("order_purchase_date", "customer_state")
        .agg(
            F.countDistinct("order_id").alias("order_count"),
            F.countDistinct(
                F.when(
                    F.col("order_delivered_carrier_timestamp").isNotNull(),
                    F.col("order_id"),
                )
            ).alias("shipped_order_count"),
            F.countDistinct(
                F.when(delivered, F.col("order_id"))
            ).alias("delivered_order_count"),
            F.countDistinct(
                F.when(
                    delivered & F.col("is_late_delivery").eqNullSafe(True),
                    F.col("order_id"),
                )
            ).alias("late_order_count"),
            F.count("approval_minutes").alias("approval_order_count"),
            F.sum("approval_minutes").alias("total_approval_minutes"),
            F.count("carrier_days").alias("carrier_day_order_count"),
            F.sum("carrier_days").alias("total_carrier_days"),
            F.sum("delivery_days").alias("total_delivery_days"),
            F.count("delivery_days").alias("delivery_day_order_count"),
            F.count("delivery_delay_days").alias("delay_day_order_count"),
            F.sum("delivery_delay_days").alias("total_delivery_delay_days"),
            F.round(F.avg("approval_minutes"), 2).alias(
                "average_approval_minutes"
            ),
            F.round(F.avg("carrier_days"), 2).alias("average_carrier_days"),
            F.round(F.avg("delivery_days"), 2).alias("average_delivery_days"),
            F.round(F.avg("delivery_delay_days"), 2).alias(
                "average_delivery_delay_days"
            ),
        )
        .withColumn(
            "late_delivery_rate",
            F.when(
                F.col("delivered_order_count") > 0,
                F.col("late_order_count")
                / F.col("delivered_order_count"),
            )
            .otherwise(F.lit(0.0))
            .cast("decimal(10,6)"),
        )
        .withColumn(
            "on_time_delivery_rate",
            F.when(
                F.col("delivered_order_count") > 0,
                1.0
                - (
                    F.col("late_order_count")
                    / F.col("delivered_order_count")
                ),
            )
            .otherwise(F.lit(0.0))
            .cast("decimal(10,6)"),
        )
        .withColumn("year", F.year("order_purchase_date"))
        .withColumn("year_month", F.date_format("order_purchase_date", "yyyy-MM"))
        .withColumn("_processed_at", F.lit(PROCESSED_AT))
    )
    write_table(result, "delivery_day")
    return result


def scalar_equal(first, second, tolerance=0.01) -> bool:
    if first is None or second is None:
        return first is None and second is None
    return abs(float(first) - float(second)) <= tolerance


def run_quality_checks(
    trade_day: DataFrame,
    product_day: DataFrame,
    seller_day: DataFrame,
    customer_rfm: DataFrame,
    delivery_day: DataFrame,
    fact_order: DataFrame,
    fact_order_item: DataFrame,
):
    checks = []

    duplicate_specs = [
        ("trade_day_unique_date", trade_day, ["order_purchase_date"]),
        (
            "product_day_unique_key",
            product_day,
            ["order_purchase_date", "product_id"],
        ),
        (
            "seller_day_unique_key",
            seller_day,
            ["order_purchase_date", "seller_id"],
        ),
        (
            "delivery_day_unique_key",
            delivery_day,
            ["order_purchase_date", "customer_state"],
        ),
        (
            "customer_rfm_unique_customer",
            customer_rfm,
            ["customer_unique_id"],
        ),
    ]

    for check_name, frame, keys in duplicate_specs:
        failed = (
            frame.groupBy(*keys)
            .count()
            .filter(F.col("count") > 1)
            .count()
        )
        checks.append((check_name, failed))

    source_order_count = fact_order.count()
    trade_order_count = (
        trade_day.agg(F.sum("order_count")).first()[0] or 0
    )
    checks.append(
        (
            "trade_day_order_count_matches_fact",
            0 if source_order_count == trade_order_count else 1,
        )
    )

    fact_gmv = (
        fact_order
        .filter(valid_trade_condition())
        .agg(F.sum("product_amount"))
        .first()[0]
        or 0
    )
    product_gmv = (
        product_day.agg(F.sum("product_gmv")).first()[0]
        or 0
    )
    seller_gmv = (
        seller_day.agg(F.sum("product_gmv")).first()[0]
        or 0
    )
    checks.append(
        (
            "product_day_gmv_matches_fact",
            0 if scalar_equal(fact_gmv, product_gmv) else 1,
        )
    )
    checks.append(
        (
            "seller_day_gmv_matches_fact",
            0 if scalar_equal(fact_gmv, seller_gmv) else 1,
        )
    )

    source_customer_count = (
        fact_order
        .filter(valid_trade_condition())
        .select("customer_unique_id")
        .distinct()
        .count()
    )
    rfm_customer_count = customer_rfm.count()
    checks.append(
        (
            "rfm_customer_count_matches_fact",
            0
            if source_customer_count == rfm_customer_count
            else 1,
        )
    )

    positive_item_counts = fact_order_item.filter(
        valid_trade_condition()
    ).count()
    product_item_counts = (
        product_day.agg(F.sum("item_count")).first()[0] or 0
    )
    checks.append(
        (
            "product_day_item_count_matches_fact",
            0 if positive_item_counts == product_item_counts else 1,
        )
    )

    quality_df = spark.createDataFrame(
        [
            (name, failed_rows, "PASS" if failed_rows == 0 else "FAIL")
            for name, failed_rows in checks
        ],
        ["check_name", "failed_rows", "status"],
    )
    quality_df.write.mode("overwrite").parquet(
        str(DWS_DIR / "_quality_checks")
    )

    print("DWS 数据质量检查：")
    quality_df.orderBy("check_name").show(truncate=False)

    failures = [name for name, failed_rows in checks if failed_rows > 0]
    if failures:
        raise RuntimeError(f"数据质量检查失败：{', '.join(failures)}")

    print(
        "金额核对： "
        f"fact_gmv={fact_gmv}, "
        f"product_gmv={product_gmv}, "
        f"seller_gmv={seller_gmv}"
    )


def main():
    global spark
    spark = get_spark("OlistBuildDWS")
    spark.sparkContext.setLogLevel("WARN")
    DWS_DIR.mkdir(parents=True, exist_ok=True)

    try:
        fact_order = read_dwd("fact_order")
        fact_order_item = read_dwd("fact_order_item")

        trade_day = build_trade_day(fact_order)
        product_day = build_product_day(fact_order_item)
        seller_day = build_seller_day(fact_order_item, fact_order)
        customer_rfm = build_customer_rfm(fact_order)
        delivery_day = build_delivery_day(fact_order)

        run_quality_checks(
            trade_day=trade_day,
            product_day=product_day,
            seller_day=seller_day,
            customer_rfm=customer_rfm,
            delivery_day=delivery_day,
            fact_order=fact_order,
            fact_order_item=fact_order_item,
        )
        print(f"DWS 构建完成：{DWS_DIR}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
