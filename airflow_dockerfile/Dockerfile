FROM apache/airflow:2.10.5-python3.12

USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER airflow

COPY --chown=airflow:root requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir --user --upgrade pip && \
    pip install --no-cache-dir --user -r /tmp/requirements.txt && \
    rm /tmp/requirements.txt