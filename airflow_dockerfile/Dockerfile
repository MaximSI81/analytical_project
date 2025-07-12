FROM apache/airflow:2.10.5-python3.12

# Установка системных зависимостей
USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Возвращаемся к пользователю airflow
USER airflow

# Установка зависимостей Python
COPY --chown=airflow:root requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r /tmp/requirements.txt && \
    rm /tmp/requirements.txt