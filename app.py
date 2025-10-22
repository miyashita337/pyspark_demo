# app.py
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("PySpark-Intro") \
    .getOrCreate()

# === 1) データ読込 ===
sales = spark.read.csv("data/sales.csv", header=True, inferSchema=True)
customers = spark.read.csv("data/customers.csv", header=True, inferSchema=True)

print("=== Sales schema ===")
sales.printSchema()
print("=== Customers schema ===")
customers.printSchema()

# === 2) 基本集計（売上＝price*qty） ===
sales_enriched = sales.withColumn("amount", F.col("price") * F.col("qty"))

# 例A: 全体件数と売上合計
summary = sales_enriched.agg(
    F.count("*").alias("rows"),
    F.sum("amount").alias("total_amount"),
    F.avg("amount").alias("avg_amount")
)
summary.show(truncate=False)

# 例B: カテゴリ別の件数・売上合計・平均単価
by_category = sales_enriched.groupBy("category").agg(
    F.count("*").alias("cnt"),
    F.sum("amount").alias("sales_sum"),
    F.avg("price").alias("avg_price")
).orderBy(F.desc("sales_sum"))
by_category.show(truncate=False)

# === 3) 窓関数で「カテゴリ別 売上Top3 商品」 ===
win = Window.partitionBy("category").orderBy(F.desc("amount"))
top3 = sales_enriched.withColumn("rn", F.row_number().over(win)) \
    .filter(F.col("rn") <= 3) \
    .select("category", "product", "amount", "order_id", "customer_id", "order_date") \
    .orderBy("category", F.desc("amount"))
top3.show(truncate=False)

# === 4) JOIN（顧客属性を付与） ===
joined = sales_enriched.join(customers, on="customer_id", how="left")
# 例: セグメント別の売上
seg = joined.groupBy("segment").agg(F.round(F.sum("amount"), 2).alias("sales_sum")) \
            .orderBy(F.desc("sales_sum"))
seg.show(truncate=False)

# === 5) 保存（結果をParquet/CSVへ） ===
by_category.write.mode("overwrite").parquet("out/by_category.parquet")
top3.write.mode("overwrite").csv("out/top3_products.csv", header=True)

print("結果を out/ 以下に保存しました。")
spark.stop()

