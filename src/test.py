from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, udf
from pyspark.sql.types import StructType, StructField, StringType, LongType
import json
import os

print(os.getenv("AWS_ACCESS_KEY_ID"))

spark = SparkSession.builder \
    .appName("KafkaBinaryDecoder") \
    .config("spark.ui.port", "4045") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
           "org.apache.hadoop:hadoop-aws:3.3.4"
                                    ) \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9006") \
    .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID")) \
    .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY")) \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

# 1. Чтение данных из Kafka ("kafka.bootstrap.servers", "10.130.0.25:9092" - host заменить на свой)
df = spark \
    .read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "10.130.0.25:9092") \
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


# 3. парсим и записываем в s3

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
    .write \
    .format("parquet") \
    .mode("append") \
    .option("path", f"s3a://{os.getenv("MINIO_PROD_BUCKET_NAME")}/data/") \
    .save()


spark.stop()