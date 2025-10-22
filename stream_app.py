# stream_app.py
from pyspark.sql import SparkSession, functions as F, types as T

spark = (SparkSession.builder
         .appName("PySpark-Streaming-Intro")
         .getOrCreate())

schema = T.StructType([
    T.StructField("event_time", T.StringType(), True),
    T.StructField("category",   T.StringType(), True),
    T.StructField("price",      T.DoubleType(), True),
    T.StructField("qty",        T.IntegerType(), True),
])

src = (spark.readStream
       .option("header", True)
       .schema(schema)
       .csv("stream/in"))

events = (src
          .withColumn("ts", F.to_timestamp("event_time", "yyyy-MM-dd HH:mm:ss"))
          .withColumn("amount", F.col("price") * F.col("qty"))
          .dropna(subset=["ts","category","amount"]))

# ★ 集計の“元”（ソートなし）
agg_base = (events
            .withWatermark("ts", "2 minutes")
            .groupBy(
                F.window(F.col("ts"), "1 minute", "10 seconds"),
                F.col("category")
            )
            .agg(F.round(F.sum("amount"), 2).alias("sales_sum"))
           )

# 1) コンソール：complete + orderBy（OK）
console_df = agg_base.orderBy(F.col("window").desc(), F.col("sales_sum").desc())
q1 = (console_df.writeStream
      .outputMode("complete")
      .option("truncate", False)
      .option("checkpointLocation", "stream/checkpoints/console")
      .format("console")
      .start())

# 2) Parquet：append（並びは後でバッチ側で整える）
q2 = (agg_base.writeStream
      .outputMode("append")
      .option("checkpointLocation", "stream/checkpoints/parquet")
      .format("parquet")
      .option("path", "stream/out")
      .start())

q1.awaitTermination()
q2.awaitTermination()

