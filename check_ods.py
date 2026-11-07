from spark_env import get_spark

spark = get_spark("CheckODS")

base = r"D:\pycharm\PythonProject\data\ods"

orders = spark.read.parquet(base + r"\olist_orders")
items = spark.read.parquet(base + r"\olist_order_items")
payments = spark.read.parquet(base + r"\olist_payments")

print("订单数：", orders.count())
print("订单明细数：", items.count())
print("支付记录数：", payments.count())

orders.printSchema()
orders.show(5, truncate=False)

spark.stop()