from datetime import datetime
import os
from pathlib import Path

from pyspark.sql import DataFrame, functions as F

from spark_env import get_spark


PROJECT_DIR = Path(__file__).resolve().parent
ODS_DIR = PROJECT_DIR / "data" / "ods"
DWD_DIR = Path(
    os.environ.get("OLIST_DWD_DIR", str(PROJECT_DIR / "data" / "dwd"))
)
PROCESSED_AT = datetime.now().isoformat(timespec="seconds")


def read_ods(table_name: str) -> DataFrame:
    return spark.read.parquet(str(ODS_DIR / f"olist_{table_name}"))


def text(column: str):
    return F.trim(F.col(column))


def timestamp(column: str):
    return F.to_timestamp(text(column), "yyyy-MM-dd HH:mm:ss")


def money(column: str):
    return F.col(column).cast("decimal(18,2)")


def zip_prefix(column: str):
    return F.lpad(F.regexp_replace(text(column), r"\.0$", ""), 5, "0")


def write_table(df: DataFrame, table_name: str):
    target = DWD_DIR / table_name
    df.write.mode("overwrite").parquet(str(target))
    print(f"{table_name:30s} rows={df.count():>10,} columns={len(df.columns):>3}")


def build_dimensions():
    orders = read_ods("orders")

    date_bounds = orders.select(
        F.min(F.to_date(timestamp("order_purchase_timestamp"))).alias("min_date"),
        F.max(
            F.greatest(
                F.coalesce(
                    F.to_date(timestamp("order_delivered_customer_date")),
                    F.to_date(timestamp("order_estimated_delivery_date")),
                    F.to_date(timestamp("order_purchase_timestamp")),
                ),
                F.coalesce(
                    F.to_date(timestamp("order_estimated_delivery_date")),
                    F.to_date(timestamp("order_purchase_timestamp")),
                ),
            )
        ).alias("max_date"),
    ).first()

    if date_bounds.min_date is None or date_bounds.max_date is None:
        raise ValueError("订单时间范围为空，无法生成日期维度")

    date_count = (date_bounds.max_date - date_bounds.min_date).days + 1
    dim_date = (
        spark.range(date_count)
        .select(
            F.date_add(
                F.lit(date_bounds.min_date),
                F.col("id").cast("int"),
            ).alias("date_key")
        )
        .withColumn("year", F.year("date_key"))
        .withColumn("quarter", F.quarter("date_key"))
        .withColumn("month", F.month("date_key"))
        .withColumn("day", F.dayofmonth("date_key"))
        .withColumn("day_of_week", F.dayofweek("date_key"))
        .withColumn("day_name", F.date_format("date_key", "EEEE"))
        .withColumn(
            "is_weekend",
            F.col("day_of_week").isin(1, 7),
        )
        .withColumn("year_month", F.date_format("date_key", "yyyy-MM"))
        .withColumn("_processed_at", F.lit(PROCESSED_AT))
    )
    write_table(dim_date, "dim_date")

    customers = read_ods("customers")
    dim_customer = (
        customers.select(
            text("customer_id").alias("customer_id"),
            text("customer_unique_id").alias("customer_unique_id"),
            zip_prefix("customer_zip_code_prefix").alias("zip_code_prefix"),
            F.lower(text("customer_city")).alias("city"),
            F.upper(text("customer_state")).alias("state"),
        )
        .filter(F.col("customer_id").isNotNull())
        .dropDuplicates(["customer_id"])
        .withColumn("_processed_at", F.lit(PROCESSED_AT))
    )
    write_table(dim_customer, "dim_customer")

    category_translation = (
        read_ods("category_translation")
        .select(
            text("product_category_name").alias("product_category_name"),
            text("product_category_name_english").alias("category_name_english"),
        )
        .dropDuplicates(["product_category_name"])
    )

    products = read_ods("products")
    dim_product = (
        products.select(
            text("product_id").alias("product_id"),
            F.coalesce(
                F.nullif(text("product_category_name"), F.lit("")),
                F.lit("unknown"),
            ).alias("product_category_name"),
            F.col("product_name_lenght").cast("int").alias("product_name_length"),
            F.col("product_description_lenght").cast("int").alias(
                "product_description_length"
            ),
            F.col("product_photos_qty").cast("int").alias("product_photos_qty"),
            F.col("product_weight_g").cast("double").alias("product_weight_g"),
            F.col("product_length_cm").cast("double").alias("product_length_cm"),
            F.col("product_height_cm").cast("double").alias("product_height_cm"),
            F.col("product_width_cm").cast("double").alias("product_width_cm"),
        )
        .join(
            category_translation,
            on="product_category_name",
            how="left",
        )
        .withColumn(
            "category_name_english",
            F.coalesce(
                F.col("category_name_english"),
                F.col("product_category_name"),
            ),
        )
        .filter(F.col("product_id").isNotNull())
        .dropDuplicates(["product_id"])
        .withColumn("_processed_at", F.lit(PROCESSED_AT))
    )
    write_table(dim_product, "dim_product")

    sellers = read_ods("sellers")
    dim_seller = (
        sellers.select(
            text("seller_id").alias("seller_id"),
            zip_prefix("seller_zip_code_prefix").alias("zip_code_prefix"),
            F.lower(text("seller_city")).alias("city"),
            F.upper(text("seller_state")).alias("state"),
        )
        .filter(F.col("seller_id").isNotNull())
        .dropDuplicates(["seller_id"])
        .withColumn("_processed_at", F.lit(PROCESSED_AT))
    )
    write_table(dim_seller, "dim_seller")

    geolocation = read_ods("geolocation")
    dim_geolocation = (
        geolocation.select(
            zip_prefix("geolocation_zip_code_prefix").alias("zip_code_prefix"),
            F.col("geolocation_lat").cast("double").alias("lat"),
            F.col("geolocation_lng").cast("double").alias("lng"),
            F.lower(text("geolocation_city")).alias("city"),
            F.upper(text("geolocation_state")).alias("state"),
        )
        .filter(
            F.col("zip_code_prefix").isNotNull()
            & F.col("lat").between(-90.0, 90.0)
            & F.col("lng").between(-180.0, 180.0)
        )
        .groupBy("zip_code_prefix")
        .agg(
            F.expr("percentile_approx(lat, 0.5)").alias("lat"),
            F.expr("percentile_approx(lng, 0.5)").alias("lng"),
            F.first("city", ignorenulls=True).alias("city"),
            F.first("state", ignorenulls=True).alias("state"),
        )
        .withColumn("_processed_at", F.lit(PROCESSED_AT))
    )
    write_table(dim_geolocation, "dim_geolocation")

    return {
        "customer": dim_customer,
        "product": dim_product,
        "seller": dim_seller,
    }


def build_facts(dimensions):
    orders = read_ods("orders")

    order_base = (
        orders.select(
            text("order_id").alias("order_id"),
            text("customer_id").alias("customer_id"),
            F.lower(text("order_status")).alias("order_status"),
            timestamp("order_purchase_timestamp").alias("order_purchase_timestamp"),
            timestamp("order_approved_at").alias("order_approved_at"),
            timestamp("order_delivered_carrier_date").alias(
                "order_delivered_carrier_timestamp"
            ),
            timestamp("order_delivered_customer_date").alias(
                "order_delivered_customer_timestamp"
            ),
            timestamp("order_estimated_delivery_date").alias(
                "order_estimated_delivery_timestamp"
            ),
        )
        .withColumn(
            "order_purchase_date",
            F.to_date("order_purchase_timestamp"),
        )
        .withColumn(
            "order_purchase_month",
            F.date_format("order_purchase_timestamp", "yyyy-MM"),
        )
        .filter(F.col("order_id").isNotNull())
        .dropDuplicates(["order_id"])
    )

    items = (
        read_ods("order_items")
        .select(
            text("order_id").alias("order_id"),
            F.col("order_item_id").cast("int").alias("order_item_id"),
            text("product_id").alias("product_id"),
            text("seller_id").alias("seller_id"),
            timestamp("shipping_limit_date").alias("shipping_limit_timestamp"),
            money("price").alias("price"),
            money("freight_value").alias("freight_value"),
        )
        .withColumn(
            "item_total_amount",
            (F.col("price") + F.col("freight_value")).cast("decimal(18,2)"),
        )
        .filter(
            F.col("order_id").isNotNull()
            & F.col("order_item_id").isNotNull()
        )
        .dropDuplicates(["order_id", "order_item_id"])
    )

    order_for_item = order_base.select(
        "order_id",
        "customer_id",
        "order_status",
        "order_purchase_timestamp",
        "order_purchase_date",
    )

    dwd_fact_order_item = (
        items.join(order_for_item, on="order_id", how="left")
        .join(
            dimensions["customer"].select(
                "customer_id",
                "customer_unique_id",
                F.col("state").alias("customer_state"),
            ),
            on="customer_id",
            how="left",
        )
        .join(
            dimensions["product"].select(
                "product_id",
                "product_category_name",
                "category_name_english",
            ),
            on="product_id",
            how="left",
        )
        .join(
            dimensions["seller"].select(
                "seller_id",
                F.col("state").alias("seller_state"),
            ),
            on="seller_id",
            how="left",
        )
        .withColumn("_processed_at", F.lit(PROCESSED_AT))
        .select(
            "order_id",
            "order_item_id",
            "product_id",
            "seller_id",
            "customer_id",
            "customer_unique_id",
            "order_status",
            "order_purchase_timestamp",
            "order_purchase_date",
            "shipping_limit_timestamp",
            "price",
            "freight_value",
            "item_total_amount",
            "product_category_name",
            "category_name_english",
            "customer_state",
            "seller_state",
            "_processed_at",
        )
    )
    write_table(dwd_fact_order_item, "fact_order_item")

    payments = (
        read_ods("payments")
        .select(
            text("order_id").alias("order_id"),
            F.col("payment_sequential").cast("int").alias("payment_sequential"),
            F.lower(text("payment_type")).alias("payment_type"),
            F.col("payment_installments").cast("int").alias(
                "payment_installments"
            ),
            money("payment_value").alias("payment_value"),
        )
        .filter(
            F.col("order_id").isNotNull()
            & F.col("payment_sequential").isNotNull()
        )
        .dropDuplicates(["order_id", "payment_sequential"])
    )

    dwd_fact_payment = (
        payments.join(order_for_item, on="order_id", how="left")
        .withColumn("_processed_at", F.lit(PROCESSED_AT))
        .select(
            "order_id",
            "payment_sequential",
            "payment_type",
            "payment_installments",
            "payment_value",
            "customer_id",
            "order_status",
            "order_purchase_timestamp",
            "order_purchase_date",
            "_processed_at",
        )
    )
    write_table(dwd_fact_payment, "fact_payment")

    item_agg = items.groupBy("order_id").agg(
        F.count("*").alias("item_row_count"),
        F.countDistinct("product_id").alias("product_count"),
        F.countDistinct("seller_id").alias("seller_count"),
        F.sum("price").cast("decimal(18,2)").alias("product_amount"),
        F.sum("freight_value").cast("decimal(18,2)").alias("freight_amount"),
    )

    payment_agg = payments.groupBy("order_id").agg(
        F.count("*").alias("payment_row_count"),
        F.sum("payment_value").cast("decimal(18,2)").alias("payment_amount"),
        F.max("payment_installments").alias("max_payment_installments"),
    )

    dwd_fact_order = (
        order_base.join(item_agg, on="order_id", how="left")
        .join(payment_agg, on="order_id", how="left")
        .join(
            dimensions["customer"].select(
                "customer_id",
                "customer_unique_id",
                F.col("state").alias("customer_state"),
            ),
            on="customer_id",
            how="left",
        )
        .withColumn(
            "item_row_count",
            F.coalesce(F.col("item_row_count"), F.lit(0)),
        )
        .withColumn(
            "product_count",
            F.coalesce(F.col("product_count"), F.lit(0)),
        )
        .withColumn(
            "seller_count",
            F.coalesce(F.col("seller_count"), F.lit(0)),
        )
        .withColumn(
            "payment_row_count",
            F.coalesce(F.col("payment_row_count"), F.lit(0)),
        )
        .withColumn(
            "product_amount",
            F.coalesce(
                F.col("product_amount"),
                F.lit(0).cast("decimal(18,2)"),
            ),
        )
        .withColumn(
            "freight_amount",
            F.coalesce(
                F.col("freight_amount"),
                F.lit(0).cast("decimal(18,2)"),
            ),
        )
        .withColumn(
            "payment_amount",
            F.coalesce(
                F.col("payment_amount"),
                F.lit(0).cast("decimal(18,2)"),
            ),
        )
        .withColumn(
            "approval_minutes",
            (
                F.unix_timestamp("order_approved_at")
                - F.unix_timestamp("order_purchase_timestamp")
            )
            / F.lit(60.0),
        )
        .withColumn(
            "carrier_days",
            F.datediff(
                F.to_date("order_delivered_carrier_timestamp"),
                F.to_date("order_purchase_timestamp"),
            ),
        )
        .withColumn(
            "delivery_days",
            F.datediff(
                F.to_date("order_delivered_customer_timestamp"),
                F.to_date("order_purchase_timestamp"),
            ),
        )
        .withColumn(
            "delivery_delay_days",
            F.datediff(
                F.to_date("order_delivered_customer_timestamp"),
                F.to_date("order_estimated_delivery_timestamp"),
            ),
        )
        .withColumn(
            "is_late_delivery",
            F.when(
                F.col("order_delivered_customer_timestamp").isNotNull()
                & F.col("order_estimated_delivery_timestamp").isNotNull(),
                F.col("delivery_delay_days") > 0,
            ).otherwise(F.lit(None).cast("boolean")),
        )
        .withColumn("_processed_at", F.lit(PROCESSED_AT))
    )
    write_table(dwd_fact_order, "fact_order")

    return {
        "orders": order_base,
        "items": dwd_fact_order_item,
        "payments": dwd_fact_payment,
        "order_fact": dwd_fact_order,
    }


def run_quality_checks(facts):
    checks = [
        (
            "fact_order_unique_order_id",
            facts["order_fact"]
            .groupBy("order_id")
            .count()
            .filter(F.col("count") > 1)
            .count(),
        ),
        (
            "fact_order_item_unique_key",
            facts["items"]
            .groupBy("order_id", "order_item_id")
            .count()
            .filter(F.col("count") > 1)
            .count(),
        ),
        (
            "fact_payment_unique_key",
            facts["payments"]
            .groupBy("order_id", "payment_sequential")
            .count()
            .filter(F.col("count") > 1)
            .count(),
        ),
        (
            "fact_order_missing_customer",
            facts["order_fact"]
            .filter(F.col("customer_unique_id").isNull())
            .count(),
        ),
        (
            "fact_order_negative_product_amount",
            facts["order_fact"]
            .filter(F.col("product_amount") < 0)
            .count(),
        ),
        (
            "fact_order_missing_purchase_time",
            facts["order_fact"]
            .filter(F.col("order_purchase_timestamp").isNull())
            .count(),
        ),
    ]

    quality_df = spark.createDataFrame(
        [
            (name, failed_rows, "PASS" if failed_rows == 0 else "FAIL")
            for name, failed_rows in checks
        ],
        ["check_name", "failed_rows", "status"],
    )
    quality_df.write.mode("overwrite").parquet(
        str(DWD_DIR / "_quality_checks")
    )
    print("DWD 数据质量检查：")
    quality_df.orderBy("check_name").show(truncate=False)

    failures = [name for name, failed_rows in checks if failed_rows > 0]
    if failures:
        raise RuntimeError(f"数据质量检查失败：{', '.join(failures)}")


def main():
    global spark
    spark = get_spark("OlistBuildDWD")
    spark.sparkContext.setLogLevel("WARN")
    DWD_DIR.mkdir(parents=True, exist_ok=True)

    try:
        dimensions = build_dimensions()
        facts = build_facts(dimensions)
        run_quality_checks(facts)
        print(f"DWD 构建完成：{DWD_DIR}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
