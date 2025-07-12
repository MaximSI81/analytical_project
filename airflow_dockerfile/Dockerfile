FROM apache/airflow:2.10.5-python3.12

# Установка системных зависимостей (как root)
USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \  # JRE вместо JDK, если не нужны компиляторы
        curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Возвращаемся к пользователю airflow
USER airflow

# Копируем и устанавливаем Python-зависимости
COPY --chown=airflow:root requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir --user --upgrade pip && \
    pip install --no-cache-dir --user -r /tmp/requirements.txt && \
    rm /tmp/requirements.txt