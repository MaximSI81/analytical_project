from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, lit
from pyspark.sql.types import StructType, StructField, StringType, LongType
import pendulum
import os, json


TOPIK = 'cdc.stg_analytical.action_users'

offsets_dict = {
    TOPIK: {
        "0": str(pendulum.now().format("YYYY-MM-DD")) + 'T00:00:00Z' }}

starting_offsets = json.dumps(offsets_dict)

spark = SparkSession.builder \
    .appName("KafkaSparkS3") \
    .config("spark.ui.port", "4045") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID")) \
    .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY")) \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

# 1. Чтение данных из Kafka 
df = spark \
    .read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", TOPIK) \
    .option("startingOffsets", starting_offsets) \
    .option("endingOffsets", "latest") \
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
        col("t.payload.after.user_id"),
        col("t.payload.after.session_id"),
        col("t.payload.after.traffic_source"),
        col("t.payload.after.session_start"),
        col("t.payload.after.session_end"),
        col("t.payload.after.device"),
        col("t.payload.after.name_link"),
        col("t.payload.after.action_type"),
        col("t.payload.after.purchase_amount")
    ) \
    .withColumn("event_date", lit(pendulum.now().format("YYYY-MM-DD"))) \
    .write \
    .format("parquet") \
    .mode("append") \
    .partitionBy("event_date") \
    .option("path", f's3a://prod/action_users') \
    .save()


spark.stop()