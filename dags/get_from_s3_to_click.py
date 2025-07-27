from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.dates import days_ago
from airflow.hooks.base import BaseHook

conn = BaseHook.get_connection('clickhouse_conn')

host = conn.host
port = conn.port
user = conn.login
password = conn.password
database = conn.schema


args = {'owner': "Max",
        'start_date': days_ago(1),
        'retries': 1}


with DAG(dag_id="get_s3_to_click",
         default_args=args,
         schedule_interval='@daily',
         description="S3 Spark Click",
         catchup=False,
         tags=['s3', 'clickhouse']
         ) as dag:
        
        start = EmptyOperator(
                task_id="start",
        )

        sensor_on_load_dag = ExternalTaskSensor(
                task_id="sensor_on_load_dag",
                external_dag_id="Load_from_Kafka_to_s3",
                allowed_states=["success"],
                mode="reschedule",
                timeout=360000,  # длительность работы сенсора
                poke_interval=60,  # частота проверки
        )
    
        get_task = SparkSubmitOperator(
                                        task_id="get_from_s3_to_click",
                                        application="/opt/airflow/scripts/get_from_s3_spark_transform_to_click.py",
                                        jars=",".join([
                                                "/opt/spark/jars/hadoop-aws-3.3.4.jar",
                                                "/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar",
                                                "/opt/spark/jars/commons-pool2-2.11.1.jar",
                                                "/opt/spark/jars/clickhouse-jdbc-0.4.6-all.jar"
                                        ]),
                                        application_args=['--access-key', '{{ var.value.AWS_ACCESS_KEY_ID }}',
                                                          '--secret-key', '{{ var.value.AWS_SECRET_ACCESS_KEY }}',
                                                          '--jdbc-url', f'jdbc:ch://{host}:{port}/{database}',
                                                          '--db-user', user,
                                                          '--db-password', password
                                                          ],
                                 
                                        conn_id="spark_default",
                                        verbose=True
                                        )

        end = EmptyOperator(
                task_id="end",
        )
        
        start >> sensor_on_load_dag >> get_task >> end