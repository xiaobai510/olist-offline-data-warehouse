from datetime import datetime
from decimal import Decimal
import os
from pathlib import Path

from pyspark.sql import DataFrame, Window, functions as F
from pyspark.sql.types import (
    DateType,
    DecimalType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from spark_env import get_spark


PROJECT_DIR = Path(__file__).resolve().parent
DWS_DIR = Path(
    os.environ.get("OLIST_DWS_DIR", str(PROJECT_DIR / "data" / "dws"))
)
ADS_DIR = Path(
    os.environ.get("OLIST_ADS_DIR", str(PROJECT_DIR / "data" / "ads"))
)
PROCESSED_AT = datetime.now().isoformat(timespec="seconds")


def read_dws(table_name: str) -> DataFrame:
    return spark.read.parquet(str(DWS_DIR / table_name))


def write_table(df: DataFrame, table_name: str):
    target = ADS_DIR / table_name
    df.write.mode("overwrite").parquet(str(target))
    print(f"{table_name:30s} rows={df.count():>10,} columns={len(df.columns):>3}")


def number(value, default=0):
    return default if value is None else value


def decimal_value(value, places=2):
    quantizer = Decimal(1).scaleb(-places)
    return Decimal(str(value)).quantize(quantizer)


def build_sales_trend(trade_day: DataFrame) -> DataFrame:
    result = (
        trade_day
        .groupBy("year_month")
        .agg(
            F.sum("order_count").cast("long").alias("order_count"),
            F.sum("valid_order_count").cast("long").alias("valid_order_count"),
            F.sum("paid_order_count").cast("long").alias("paid_order_count"),
            F.sum("canceled_order_count").cast("long").alias(
                "canceled_order_count"
            ),
            F.sum("item_row_count").cast("long").alias("item_count"),
            F.sum("product_gmv").cast("decimal(18,2)").alias("product_gmv"),
            F.sum("freight_revenue").cast("decimal(18,2)").alias(
                "freight_revenue"
            ),
            F.sum("payment_amount").cast("decimal(18,2)").alias(
                "payment_amount"
            ),
        )
        .withColumn(
            "month_start_date",
            F.to_date(F.concat(F.col("year_month"), F.lit("-01"))),
        )
        .withColumn(
            "average_order_value",
            F.when(
                F.col("valid_order_count") > 0,
                F.col("product_gmv") / F.col("valid_order_count"),
            )
            .otherwise(F.lit(0))
            .cast("decimal(18,2)"),
        )
    )

    month_window = Window.orderBy("month_start_date")
    result = (
        result
        .withColumn(
            "previous_month_gmv",
            F.lag("product_gmv").over(month_window),
        )
        .withColumn(
            "gmv_mom_rate",
            F.when(
                F.col("previous_month_gmv") > 0,
                (
                    F.col("product_gmv")
                    - F.col("previous_month_gmv")
                )
                / F.col("previous_month_gmv"),
            )
            .otherwise(F.lit(None).cast("decimal(18,6)")),
        )
        .withColumn("_processed_at", F.lit(PROCESSED_AT))
        .orderBy("month_start_date")
    )
    write_table(result, "sales_trend")
    return result


def build_category_ranking(product_day: DataFrame) -> DataFrame:
    category = (
        product_day
        .groupBy("category_name_english")
        .agg(
            F.sum("item_count").cast("long").alias("item_count"),
            F.countDistinct("product_id").alias("product_count"),
            F.sum("product_gmv").cast("decimal(18,2)").alias("product_gmv"),
            F.sum("freight_revenue").cast("decimal(18,2)").alias(
                "freight_revenue"
            ),
        )
    )

    total_gmv = category.agg(
        F.sum("product_gmv").alias("total_gmv")
    ).first().total_gmv
    total_gmv = number(total_gmv)

    result = (
        category
        .withColumn(
            "average_item_price",
            F.when(
                F.col("item_count") > 0,
                F.col("product_gmv") / F.col("item_count"),
            )
            .otherwise(F.lit(0))
            .cast("decimal(18,2)"),
        )
        .withColumn(
            "gmv_share",
            F.when(
                F.lit(total_gmv) > 0,
                F.col("product_gmv") / F.lit(total_gmv),
            )
            .otherwise(F.lit(0))
            .cast("decimal(18,6)"),
        )
        .withColumn(
            "sales_rank",
            F.row_number().over(
                Window.orderBy(F.desc("product_gmv"), "category_name_english")
            ),
        )
        .withColumn("_processed_at", F.lit(PROCESSED_AT))
        .orderBy("sales_rank")
    )
    write_table(result, "category_ranking")
    return result


def build_seller_ranking(seller_day: DataFrame) -> DataFrame:
    seller = (
        seller_day
        .groupBy("seller_id", "seller_state")
        .agg(
            F.sum("order_count").cast("long").alias("order_count"),
            F.sum("item_count").cast("long").alias("item_count"),
            F.sum("product_gmv").cast("decimal(18,2)").alias("product_gmv"),
            F.sum("freight_revenue").cast("decimal(18,2)").alias(
                "freight_revenue"
            ),
            F.sum("delivered_order_count").cast("long").alias(
                "delivered_order_count"
            ),
            F.sum("late_delivered_order_count").cast("long").alias(
                "late_delivered_order_count"
            ),
        )
    )

    total_gmv = seller.agg(
        F.sum("product_gmv").alias("total_gmv")
    ).first().total_gmv
    total_gmv = number(total_gmv)

    result = (
        seller
        .withColumn(
            "average_order_value",
            F.when(
                F.col("order_count") > 0,
                F.col("product_gmv") / F.col("order_count"),
            )
            .otherwise(F.lit(0))
            .cast("decimal(18,2)"),
        )
        .withColumn(
            "gmv_share",
            F.when(
                F.lit(total_gmv) > 0,
                F.col("product_gmv") / F.lit(total_gmv),
            )
            .otherwise(F.lit(0))
            .cast("decimal(18,6)"),
        )
        .withColumn(
            "late_delivery_rate",
            F.when(
                F.col("delivered_order_count") > 0,
                F.col("late_delivered_order_count")
                / F.col("delivered_order_count"),
            )
            .otherwise(F.lit(0))
            .cast("decimal(18,6)"),
        )
        .withColumn(
            "sales_rank",
            F.row_number().over(
                Window.orderBy(F.desc("product_gmv"), "seller_id")
            ),
        )
        .withColumn("_processed_at", F.lit(PROCESSED_AT))
        .orderBy("sales_rank")
    )
    write_table(result, "seller_ranking")
    return result


def build_rfm_summary(customer_rfm: DataFrame) -> DataFrame:
    segment = (
        customer_rfm
        .groupBy("rfm_segment")
        .agg(
            F.countDistinct("customer_unique_id").alias("customer_count"),
            F.sum("monetary").cast("decimal(18,2)").alias("total_monetary"),
            F.avg("monetary").cast("decimal(18,2)").alias(
                "average_monetary"
            ),
            F.avg("frequency").alias("average_frequency"),
            F.avg("recency_days").alias("average_recency_days"),
            F.avg("average_order_value").cast("decimal(18,2)").alias(
                "average_order_value"
            ),
            F.avg("r_score").alias("average_r_score"),
            F.avg("f_score").alias("average_f_score"),
            F.avg("m_score").alias("average_m_score"),
        )
    )

    totals = segment.agg(
        F.sum("customer_count").alias("total_customers"),
        F.sum("total_monetary").alias("total_monetary"),
    ).first()
    total_customers = number(totals.total_customers)
    total_monetary = number(totals.total_monetary)

    result = (
        segment
        .withColumn(
            "customer_share",
            F.when(
                F.lit(total_customers) > 0,
                F.col("customer_count") / F.lit(total_customers),
            )
            .otherwise(F.lit(0))
            .cast("decimal(18,6)"),
        )
        .withColumn(
            "monetary_share",
            F.when(
                F.lit(total_monetary) > 0,
                F.col("total_monetary") / F.lit(total_monetary),
            )
            .otherwise(F.lit(0))
            .cast("decimal(18,6)"),
        )
        .withColumn("_processed_at", F.lit(PROCESSED_AT))
        .orderBy(F.desc("total_monetary"), "rfm_segment")
    )
    write_table(result, "rfm_summary")
    return result


def build_delivery_summary(delivery_day: DataFrame) -> DataFrame:
    state = (
        delivery_day
        .groupBy("customer_state")
        .agg(
            F.sum("order_count").cast("long").alias("order_count"),
            F.sum("shipped_order_count").cast("long").alias(
                "shipped_order_count"
            ),
            F.sum("delivered_order_count").cast("long").alias(
                "delivered_order_count"
            ),
            F.sum("late_order_count").cast("long").alias("late_order_count"),
            F.sum("approval_order_count").cast("long").alias(
                "approval_order_count"
            ),
            F.sum("total_approval_minutes").cast("double").alias(
                "total_approval_minutes"
            ),
            F.sum("carrier_day_order_count").cast("long").alias(
                "carrier_day_order_count"
            ),
            F.sum("total_carrier_days").cast("double").alias(
                "total_carrier_days"
            ),
            F.sum("delivery_day_order_count").cast("long").alias(
                "delivery_day_order_count"
            ),
            F.sum("total_delivery_days").cast("double").alias(
                "total_delivery_days"
            ),
            F.sum("delay_day_order_count").cast("long").alias(
                "delay_day_order_count"
            ),
            F.sum("total_delivery_delay_days").cast("double").alias(
                "total_delivery_delay_days"
            ),
        )
    )

    result = (
        state
        .withColumn(
            "ship_rate",
            F.when(
                F.col("order_count") > 0,
                F.col("shipped_order_count") / F.col("order_count"),
            )
            .otherwise(F.lit(0))
            .cast("decimal(18,6)"),
        )
        .withColumn(
            "delivery_rate",
            F.when(
                F.col("order_count") > 0,
                F.col("delivered_order_count") / F.col("order_count"),
            )
            .otherwise(F.lit(0))
            .cast("decimal(18,6)"),
        )
        .withColumn(
            "late_delivery_rate",
            F.when(
                F.col("delivered_order_count") > 0,
                F.col("late_order_count")
                / F.col("delivered_order_count"),
            )
            .otherwise(F.lit(0))
            .cast("decimal(18,6)"),
        )
        .withColumn(
            "on_time_delivery_rate",
            F.when(
                F.col("delivered_order_count") > 0,
                1
                - (
                    F.col("late_order_count")
                    / F.col("delivered_order_count")
                ),
            )
            .otherwise(F.lit(0))
            .cast("decimal(18,6)"),
        )
        .withColumn(
            "average_approval_minutes",
            F.when(
                F.col("approval_order_count") > 0,
                F.col("total_approval_minutes")
                / F.col("approval_order_count"),
            ).cast("double"),
        )
        .withColumn(
            "average_carrier_days",
            F.when(
                F.col("carrier_day_order_count") > 0,
                F.col("total_carrier_days")
                / F.col("carrier_day_order_count"),
            ).cast("double"),
        )
        .withColumn(
            "average_delivery_days",
            F.when(
                F.col("delivery_day_order_count") > 0,
                F.col("total_delivery_days")
                / F.col("delivery_day_order_count"),
            ).cast("double"),
        )
        .withColumn(
            "average_delivery_delay_days",
            F.when(
                F.col("delay_day_order_count") > 0,
                F.col("total_delivery_delay_days")
                / F.col("delay_day_order_count"),
            ).cast("double"),
        )
        .withColumn("_processed_at", F.lit(PROCESSED_AT))
        .orderBy(F.desc("order_count"), "customer_state")
    )
    write_table(result, "delivery_summary")
    return result


def build_business_overview(
    trade_day: DataFrame,
    delivery_summary: DataFrame,
    customer_rfm: DataFrame,
    rfm_summary: DataFrame,
    category_ranking: DataFrame,
    seller_ranking: DataFrame,
) -> DataFrame:
    trade = trade_day.agg(
        F.min("order_purchase_date").alias("start_date"),
        F.max("order_purchase_date").alias("end_date"),
        F.sum("order_count").alias("order_count"),
        F.sum("valid_order_count").alias("valid_order_count"),
        F.sum("paid_order_count").alias("paid_order_count"),
        F.sum("canceled_order_count").alias("canceled_order_count"),
        F.sum("item_row_count").alias("item_count"),
        F.sum("product_gmv").alias("product_gmv"),
        F.sum("freight_revenue").alias("freight_revenue"),
        F.sum("payment_amount").alias("payment_amount"),
    ).first()

    delivery = delivery_summary.agg(
        F.sum("delivered_order_count").alias("delivered_order_count"),
        F.sum("late_order_count").alias("late_order_count"),
        F.sum("delivery_day_order_count").alias("delivery_day_order_count"),
        F.sum("total_delivery_days").alias("total_delivery_days"),
    ).first()

    rfm = rfm_summary.agg(
        F.sum("customer_count").alias("customer_count"),
        F.sum("total_monetary").alias("total_monetary"),
    ).first()

    top_category = category_ranking.orderBy("sales_rank").first()
    top_seller = seller_ranking.orderBy("sales_rank").first()

    order_count = int(number(trade.order_count))
    valid_order_count = int(number(trade.valid_order_count))
    paid_order_count = int(number(trade.paid_order_count))
    canceled_order_count = int(number(trade.canceled_order_count))
    customer_count = int(number(rfm.customer_count))
    delivered_order_count = int(number(delivery.delivered_order_count))
    late_order_count = int(number(delivery.late_order_count))
    delivery_day_order_count = int(number(delivery.delivery_day_order_count))
    total_delivery_days = float(number(delivery.total_delivery_days))

    product_gmv = number(trade.product_gmv)
    payment_amount = number(trade.payment_amount)
    cancel_rate = (
        canceled_order_count / order_count if order_count else 0.0
    )
    repeat_customer_count = customer_rfm.filter(
        F.col("frequency") > 1
    ).count()
    repeat_customer_rate = (
        repeat_customer_count / customer_count if customer_count else 0.0
    )
    average_order_value = (
        float(product_gmv) / valid_order_count
        if valid_order_count
        else 0.0
    )
    average_payment_value = (
        float(payment_amount) / paid_order_count
        if paid_order_count
        else 0.0
    )
    late_delivery_rate = (
        late_order_count / delivered_order_count
        if delivered_order_count
        else 0.0
    )
    average_delivery_days = (
        total_delivery_days / delivery_day_order_count
        if delivery_day_order_count
        else 0.0
    )

    start_date = trade.start_date
    end_date = trade.end_date
    analysis_days = (
        (end_date - start_date).days + 1
        if start_date is not None and end_date is not None
        else 0
    )

    schema = StructType(
        [
            StructField("analysis_start_date", DateType()),
            StructField("analysis_end_date", DateType()),
            StructField("analysis_days", IntegerType()),
            StructField("order_count", LongType()),
            StructField("valid_order_count", LongType()),
            StructField("paid_order_count", LongType()),
            StructField("canceled_order_count", LongType()),
            StructField("cancel_rate", DecimalType(18, 6)),
            StructField("item_count", LongType()),
            StructField("product_gmv", DecimalType(18, 2)),
            StructField("freight_revenue", DecimalType(18, 2)),
            StructField("payment_amount", DecimalType(18, 2)),
            StructField("average_order_value", DecimalType(18, 2)),
            StructField("average_payment_value", DecimalType(18, 2)),
            StructField("customer_count", LongType()),
            StructField("repeat_customer_count", LongType()),
            StructField("repeat_customer_rate", DecimalType(18, 6)),
            StructField("delivered_order_count", LongType()),
            StructField("late_order_count", LongType()),
            StructField("late_delivery_rate", DecimalType(18, 6)),
            StructField("average_delivery_days", DoubleType()),
            StructField("top_category", StringType()),
            StructField("top_category_gmv", DecimalType(18, 2)),
            StructField("top_seller_id", StringType()),
            StructField("top_seller_gmv", DecimalType(18, 2)),
            StructField("_processed_at", StringType()),
        ]
    )

    rows = [
        (
            start_date,
            end_date,
            analysis_days,
            order_count,
            valid_order_count,
            paid_order_count,
            canceled_order_count,
            decimal_value(cancel_rate, 6),
            int(number(trade.item_count)),
            product_gmv,
            number(trade.freight_revenue),
            payment_amount,
            decimal_value(average_order_value, 2),
            decimal_value(average_payment_value, 2),
            customer_count,
            repeat_customer_count,
            decimal_value(repeat_customer_rate, 6),
            delivered_order_count,
            late_order_count,
            decimal_value(late_delivery_rate, 6),
            average_delivery_days,
            top_category.category_name_english,
            top_category.product_gmv,
            top_seller.seller_id,
            top_seller.product_gmv,
            PROCESSED_AT,
        )
    ]

    result = spark.createDataFrame(rows, schema=schema)
    write_table(result, "business_overview")
    return result


def run_quality_checks(
    business_overview: DataFrame,
    sales_trend: DataFrame,
    category_ranking: DataFrame,
    seller_ranking: DataFrame,
    rfm_summary: DataFrame,
    delivery_summary: DataFrame,
    trade_day: DataFrame,
) -> None:
    checks = [
        (
            "business_overview_single_row",
            0 if business_overview.count() == 1 else 1,
        ),
        (
            "sales_trend_unique_month",
            sales_trend
            .groupBy("year_month")
            .count()
            .filter(F.col("count") > 1)
            .count(),
        ),
        (
            "category_ranking_unique_key",
            category_ranking
            .groupBy("category_name_english")
            .count()
            .filter(F.col("count") > 1)
            .count(),
        ),
        (
            "seller_ranking_unique_key",
            seller_ranking
            .groupBy("seller_id")
            .count()
            .filter(F.col("count") > 1)
            .count(),
        ),
        (
            "rfm_summary_unique_segment",
            rfm_summary
            .groupBy("rfm_segment")
            .count()
            .filter(F.col("count") > 1)
            .count(),
        ),
        (
            "delivery_summary_unique_state",
            delivery_summary
            .groupBy("customer_state")
            .count()
            .filter(F.col("count") > 1)
            .count(),
        ),
    ]

    trade_gmv = number(
        trade_day.agg(F.sum("product_gmv")).first()[0]
    )
    category_gmv = number(
        category_ranking.agg(F.sum("product_gmv")).first()[0]
    )
    seller_gmv = number(
        seller_ranking.agg(F.sum("product_gmv")).first()[0]
    )
    trend_gmv = number(
        sales_trend.agg(F.sum("product_gmv")).first()[0]
    )

    for check_name, value in [
        ("category_gmv_matches_dws", category_gmv),
        ("seller_gmv_matches_dws", seller_gmv),
        ("sales_trend_gmv_matches_dws", trend_gmv),
    ]:
        checks.append(
            (
                check_name,
                0 if abs(float(trade_gmv) - float(value)) <= 0.01 else 1,
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
        str(ADS_DIR / "_quality_checks")
    )

    print("ADS 数据质量检查：")
    quality_df.orderBy("check_name").show(truncate=False)

    failures = [name for name, failed_rows in checks if failed_rows > 0]
    if failures:
        raise RuntimeError(f"数据质量检查失败：{', '.join(failures)}")


def main():
    global spark
    spark = get_spark("OlistBuildADS")
    spark.sparkContext.setLogLevel("WARN")
    ADS_DIR.mkdir(parents=True, exist_ok=True)

    try:
        trade_day = read_dws("trade_day")
        product_day = read_dws("product_day")
        seller_day = read_dws("seller_day")
        customer_rfm = read_dws("customer_rfm")
        delivery_day = read_dws("delivery_day")

        sales_trend = build_sales_trend(trade_day)
        category_ranking = build_category_ranking(product_day)
        seller_ranking = build_seller_ranking(seller_day)
        rfm_summary = build_rfm_summary(customer_rfm)
        delivery_summary = build_delivery_summary(delivery_day)
        business_overview = build_business_overview(
            trade_day,
            delivery_summary,
            customer_rfm,
            rfm_summary,
            category_ranking,
            seller_ranking,
        )

        run_quality_checks(
            business_overview=business_overview,
            sales_trend=sales_trend,
            category_ranking=category_ranking,
            seller_ranking=seller_ranking,
            rfm_summary=rfm_summary,
            delivery_summary=delivery_summary,
            trade_day=trade_day,
        )

        print("经营总览：")
        business_overview.show(truncate=False)
        print("品类销售榜前十：")
        category_ranking.select(
            "sales_rank",
            "category_name_english",
            "item_count",
            "product_gmv",
            "gmv_share",
        ).show(10, truncate=False)
        print(f"ADS 构建完成：{ADS_DIR}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
