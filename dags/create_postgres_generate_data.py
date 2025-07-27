import sys
sys.path.append('/opt/airflow/dags/src')
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from src.generate_data_action_users import *
from airflow.utils.dates import days_ago
import pandas as pd

args = {
    "owner": 'Max',
    "start_date": days_ago(1),
    "retries": 1,
}



def load_data_postgres(**context):
    df = pd.DataFrame(generate_data())
        # Подключение к PostgreSQL
    hook = PostgresHook(postgres_conn_id="postgres_db")
    engine = hook.get_sqlalchemy_engine()
    
    # Вставка всего DataFrame
    df.to_sql(
        name='action_users',         # Название таблицы
        con=engine,                  # Подключение
        schema='stg_analytical',     # Схема (если нужна)
        if_exists='append',          # Добавить к существующим данным
        index=False,                 # Не вставлять индексы
        method='multi',              # Пакетная вставка
        chunksize=1000               # Размер пачки для вставки
    )




with DAG(
    dag_id="Create_Postgres_data",
    default_args=args,
    schedule_interval="0 0 * * *",
    description="Загрузка сгенерированных данных в БД",
    catchup=False,
    tags=['generate', 'load_postgres'],
    concurrency=1,
) as dag:
    
    start = EmptyOperator(
                task_id="start",
        )
    
    load_data = PythonOperator(task_id="load_data",
                                python_callable=load_data_postgres,
                                        )
    end = EmptyOperator(
                task_id="end",
        )
    
    start >> load_data >> end