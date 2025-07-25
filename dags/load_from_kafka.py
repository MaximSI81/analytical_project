from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago






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
    
        start = EmptyOperator(
                task_id="start",
                )

        sensor_on_create_dag = ExternalTaskSensor(
                task_id="sensor_on_create_dag",
                external_dag_id="Create_Postgres_data",
                allowed_states=["success"],
                mode="reschedule",
                timeout=360000,  # длительность работы сенсора
                poke_interval=60,  # частота проверки
        )
    
    
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
                                        application_args=['--access-key', '{{ var.value.AWS_ACCESS_KEY_ID }}',
                                                          '--secret-key', '{{ var.value.AWS_SECRET_ACCESS_KEY }}'
                                                          ],
                                        conf={
                                                "spark.hadoop.fs.s3a.connection.ssl.enabled": "false"
                                        },
                                 
                                        conn_id="spark_default",
                                        verbose=True
                                        )
        end = EmptyOperator(
                task_id="end",
        )
        
        start >> sensor_on_create_dag >> load_task >> end