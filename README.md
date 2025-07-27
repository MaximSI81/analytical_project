## ETL-проект для анализа действий пользователей на сайте

Этот проект представляет собой ETL-пайплайн для обработки данных о действиях пользователей на сайте страховой компании.
Данные сгенерированны.
 
## 🚀 Архитектура ETL

 - Airflow
 - generated data -> Postgres -> Debezium -> Kafka -> pySpark -> S3 -> Clickhouse -> Metabase

### Основные компоненты

 - Источники данных
 **Postgres** - сгенерированные данные на **Python**, генерация и загрузка управляется **Airflow**
 - ETL 
 управляется **Airflow**, ETL **pySpark**
 - Хранилище 
 **s3minio**
 - БД для аналитики
 **Clickhouse**
 - Визуализация
 **Metabase**

## 🛠 Установка и запуск

1. Клонируйте репозиторий:
```bash
git clone https://github.com/MaximSI81/analytical_project.git
cd analytical_project
```

2. Создайте файл `.env` со следующими параметрами:
```env
AIRFLOW__CORE__FERNET_KEY= - создайте свой или измени в docker-compose на ''

_AIRFLOW_WWW_USER_USERNAME=admin
_AIRFLOW_WWW_USER_PASSWORD=admin
AIRFLOW_UID=1000
AIRFLOW_GID=0

POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow

POSTGRES_DWH_USER=postgres
POSTGRES_DWH_PASSWORD=postgres

MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin
MINIO_PROD_BUCKET_NAME=prod

AWS_ACCESS_KEY_ID= - создать в minio
AWS_SECRET_ACCESS_KEY= - создать в minio
```

## Требуемые зависимости Spark JAR

Добавьте эти JAR-файлы в `airflow_dockerfile/spark/jars/`:

- [spark-sql-kafka-0-10_2.12-3.5.0.jar](https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar)  
- [hadoop-aws-3.3.4.jar](https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar)  
- [kafka-clients-3.5.0.jar](https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.0/kafka-clients-3.5.0.jar)  
- [spark-token-provider-kafka-0-10_2.12-3.5.0.jar](https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.0/spark-token-provider-kafka-0-10_2.12-3.5.0.jar)  
- [aws-java-sdk-bundle-1.12.262.jar](https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar)  
- [commons-pool2-2.11.1.jar](https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar)
- [clickhouse-jdbc-0.4.6-all.jar](https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/0.4.6/clickhouse-jdbc-0.4.6-all.jar)

