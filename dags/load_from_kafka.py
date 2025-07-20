from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
import os





args = {'owner': "Max",
        'start_date': days_ago(1),
        'retries': 1}


with DAG(dag_id="Load_from_Kafka_to_s3",
         default_args=args,
         schedule_interval='@daily',
         description="Spark Kafka S3",
         catchup=False,
         tags=['spark', 'kafka']
         ) as dag:
    
    load_task = SparkSubmitOperator(
                                        task_id="load_from_kafka_toS3",
                                        application="/opt/airflow/scripts/load_from_Kafka_to_S3.py",
                                        jars=",".join([
                                                "/opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar",
                                                "/opt/spark/jars/hadoop-aws-3.3.4.jar",
                                                "/opt/spark/jars/kafka-clients-3.5.0.jar",
                                                "/opt/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.0.jar",
                                                "/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar",
                                                "/opt/spark/jars/commons-pool2-2.11.1.jar"
                                        ]),
                                        conf={
                                                "spark.hadoop.fs.s3a.connection.ssl.enabled": "false"
                                        },
                                 
                                        conn_id="spark_default",
                                        verbose=True
                                        )