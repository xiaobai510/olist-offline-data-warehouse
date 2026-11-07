from datetime import datetime
from pathlib import Path
import os

PROJECT_DIR = Path(__file__).resolve().parent

# 必须在导入 kagglehub 前设置缓存目录
os.environ["KAGGLEHUB_CACHE"] = str(
    PROJECT_DIR / ".cache" / "kagglehub"
)

import kagglehub
from pyspark.sql import functions as F

from spark_env import get_spark


DATASET_HANDLE = "olistbr/brazilian-ecommerce"

FILES = {
    "orders": "olist_orders_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
    "payments": "olist_order_payments_dataset.csv",
    "customers": "olist_customers_dataset.csv",
    "products": "olist_products_dataset.csv",
    "sellers": "olist_sellers_dataset.csv",
    "geolocation": "olist_geolocation_dataset.csv",
    "reviews": "olist_order_reviews_dataset.csv",
    "category_translation": "product_category_name_translation.csv",
}


def main():
    source_dir = Path(
        kagglehub.dataset_download(DATASET_HANDLE)
    )

    print("数据源目录：", source_dir)

    spark = get_spark("OlistODSIngest")
    ingest_time = datetime.now().isoformat(timespec="seconds")

    try:
        for table_name, filename in FILES.items():
            source_file = source_dir / filename

            if not source_file.exists():
                raise FileNotFoundError(
                    f"找不到数据文件：{source_file}"
                )

            df = (
                spark.read
                .option("header", True)
                .option("encoding", "UTF-8")
                .csv(str(source_file))
                .withColumn(
                    "_source_file",
                    F.lit(filename),
                )
                .withColumn(
                    "_ingest_time",
                    F.lit(ingest_time),
                )
            )

            target = (
                PROJECT_DIR
                / "data"
                / "ods"
                / f"olist_{table_name}"
            )

            (
                df.write
                .mode("overwrite")
                .parquet(str(target))
            )

            print(
                f"{table_name:20s}"
                f" rows={df.count():>10,}"
                f" columns={len(df.columns):>3}"
            )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()