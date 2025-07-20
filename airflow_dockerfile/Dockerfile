FROM apache/airflow:2.10.5-python3.12

COPY requirements.txt /requirements.txt

RUN python -m pip install  --upgrade pip && \
    pip install --no-cache-dir -r /requirements.txt

USER root

RUN apt update && \
    apt install -y openjdk-17-jre-headless

# Создаем директорию для JAR-файлов
RUN mkdir -p /opt/spark/jars
# Копируем JAR-файлы (должны находиться в ./spark/jars относительно Dockerfile)
COPY ./spark/jars/ /opt/spark/jars/

# Установка Python-зависимостей

RUN chown 755 /opt/spark

# Настройка переменных окружения для Spark
ENV SPARK_HOME=/home/airflow/.local/lib/python3.12/site-packages/pyspark
ENV PATH=$PATH:$SPARK_HOME/bin
ENV PYTHONPATH=$SPARK_HOME/python:$PYTHONPATH
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3
USER airflow