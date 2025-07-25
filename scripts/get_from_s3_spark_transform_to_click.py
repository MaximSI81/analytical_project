from pyspark.sql import SparkSession
from pyspark.sql.functions import count, current_timestamp
from dotenv import load_dotenv
load_dotenv()
import pendulum, argparse

parser = argparse.ArgumentParser()
parser.add_argument('--access-key', required=True)
parser.add_argument('--secret-key', required=True)
parser.add_argument('--jdbc-url', required=True)
parser.add_argument('--db-user', required=True)
parser.add_argument('--db-password', required=True)
parser.add_argument('--table-name', required=True)

args = parser.parse_args()


spark = SparkSession.builder \
    .appName("S3SparkClick") \
    .config("spark.ui.port", "4045") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", args.access_key) \
    .config("spark.hadoop.fs.s3a.secret.key", args.secret_key) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .getOrCreate()
    
df = spark.read \
    .format("parquet") \
    .load(f"s3a://prod/action_users/event_date={pendulum.now().format("YYYY-MM-DD")}/").persist()
    
def write_to_click(name_df, jdbc_url, table_name, db_user, db_password):
    
    name_df.write \
    .format("jdbc") \
    .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
    .option("url", jdbc_url) \
    .option("dbtable", table_name) \
    .option("user", db_user) \
    .option("password", db_password) \
    .option("batchsize", 100) \
    .option("numPartitions", 1) \
    .mode("append") \
    .save()
    
    
device_df = df.groupBy("device") \
    .agg(current_timestamp().alias("load_date"), count("*").alias("users_amount"))

device_df.printSchema()

write_to_click(device_df, args.jdbc_url, args.table_name, args.db_user, args.db_password)
    
action_df = df.groupBy("action_type") \
    .agg(current_timestamp().alias("load_date"), count("*").alias("users_amount"))

action_df.printSchema()

write_to_click(action_df, args.jdbc_url, args.table_name, args.db_user, args.db_password)
    


df.unpersist()
spark.stop()