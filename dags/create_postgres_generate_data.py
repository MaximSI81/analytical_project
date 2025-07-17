
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from opt.airflow.scripts.generate_data_action_users import *
from airflow.utils.dates import days_ago


args = {
    "owner": 'Max',
    "start_date": days_ago(1),
    "retries": 1,
}


with DAG(
    dag_id="Create_Postgres_data",
    default_args=args,
    schedule_interval="0 * * * *",
    description="Загрузка сгенерированных данных в БД",
    catchup=False,
    tags=['generate', 'load_postgres'],
    concurrency=1,
) as dag:
    load_data_postgres = SQLExecuteQueryOperator(task_id="load_data_postgres",
                                                 conn_id="postgres_db",
                                                 autocommit=True,
                                                 sql=""" INSERT INTO stg_analytical.action_users (user_id, 
                                                                                                   session_id, 
                                                                                                   traffic_source, 
                                                                                                   session_start, 
                                                                                                   session_end, 
                                                                                                   device, 
                                                                                                   name_link, 
                                                                                                   action_type, 
                                                                                                   purchase_amount,
                                                                                                   created_at)
                                                        VALUES (%(user_id)s, 
                                                                %(session_id)s, 
                                                                %(traffic_source)s, 
                                                                %(session_start)s, 
                                                                %(session_end)s, 
                                                                %(device)s, 
                                                                %(name_link)s, 
                                                                %(action_type)s, 
                                                                %(purchase_amount)s,
                                                                NOW())""",
                                                 parameters=generate_data(),
                                                )
    
    
