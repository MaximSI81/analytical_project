from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, udf
from pyspark.sql.types import StructType, StructField, StringType, LongType
import json

spark = SparkSession.builder \
    .appName("KafkaBinaryDecoder") \
    .config("spark.ui.port", "4045") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

# 1. Чтение данных из Kafka
df = spark \
    .read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "cdc.stg_analytical.action_users") \
    .option("startingOffsets", "earliest") \
    .load() \
    .selectExpr("CAST(value AS STRING)")


# 2. Определение схемы (упрощенная версия)
schema = StructType([
    StructField("payload", StructType([
        StructField("after", StructType([
            StructField("user_id", LongType()),
            StructField("session_id", StringType()),
            StructField("traffic_source", StringType(), True),
            StructField("session_start", StringType()),
            StructField("session_end", StringType()),
            StructField("device", StringType(), True),
            StructField("name_link", StringType(), True),
            StructField("action_type", StringType(), True),
            StructField("purchase_amount", StringType(), True)
        ]))
    ]))
])




df.select(from_json(col("value"), schema).alias("t")) \
  .select(
        "t.payload.after.user_id",
        "t.payload.after.session_id",
        "t.payload.after.traffic_source",
        "t.payload.after.session_start",
        "t.payload.after.session_end",
        "t.payload.after.device",
        "t.payload.after.name_link",
        "t.payload.after.action_type",
        "t.payload.after.purchase_amount") \
  .show(truncate=False) 



spark.stop()