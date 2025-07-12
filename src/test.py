from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType
from pyspark.sql.functions import from_json, col
from datetime import datetime, timedelta
from pyspark.sql.functions import unhex

spark = SparkSession.builder \
    .appName("KafkaToS3OrderEvents") \
    .config("spark.ui.port", "4045") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
    .config("spark.driver.host", "10.130.0.11") \
    .getOrCreate()

# 2. Чтение данных из Kafka в пакетном режиме
df = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "cdc.stg_analytical.action_users") \
    .option("startingOffsets", "earliest") \
    .option("endingOffsets", "latest") \
    .load()

# 3. Проверка полученных данных
print("Схема DataFrame:")
df.printSchema()

print("\nПример данных:")
df.select("key", "value", "topic", "partition", "offset", "timestamp").show(truncate=False)

minimal_schema = StructType([
    StructField("before", 
        StructType([
            StructField("user_id", LongType()),
            StructField("session_id", StringType()),
            StructField("traffic_source", StringType(), nullable=True),
            StructField("session_start", StringType()),
            StructField("session_end", StringType()),
            StructField("device", StringType(), nullable=True),
            StructField("name_link", StringType(), nullable=True),
            StructField("action_type", StringType(), nullable=True),
            StructField("purchase_amount", StringType(), nullable=True)
        ]), 
        nullable=True
    ),
    StructField("after", 
        StructType([
            # Повторяем ту же структуру, что и в "before" 
            StructField("user_id", LongType()),
            StructField("session_id", StringType()),
            StructField("traffic_source", StringType(), nullable=True),
            StructField("session_start", StringType()),
            StructField("session_end", StringType()),
            StructField("device", StringType(), nullable=True),
            StructField("name_link", StringType(), nullable=True),
            StructField("action_type", StringType(), nullable=True),
            StructField("purchase_amount", StringType(), nullable=True)
            
        ]), 
        nullable=True
    ),
    StructField("op", StringType()),
    StructField("ts_ms", LongType(), True)
])


from pyspark.sql.functions import udf

import json

# Функция для декодирования бинарных данных
def decode_binary(binary_data):
    if binary_data:
        return binary_data.decode("utf-8")  # Преобразуем бинарные данные в строку
    return None

decode_udf = udf(decode_binary, StringType())

# Сначала декодируем, затем парсим JSON
parsed_df = df.withColumn("value_str", decode_udf(col("value"))) \
             .select(
                 from_json(col("value_str"), minimal_schema).alias("data")
             ).select("data.*")

# 6. Вывод результата
print("\nРаспарсенные данные:")
parsed_df.show(truncate=False)

spark.stop()


