FROM eclipse-temurin:19-jdk-jammy

ENV MB_PLUGINS_DIR=/home/plugins/
RUN mkdir -p /home/plugins/ && chmod 755 /home/plugins/

# Скачиваем ClickHouse драйвер через curl
RUN curl -L -o /home/plugins/clickhouse.metabase-driver.jar https://github.com/ClickHouse/metabase-clickhouse-driver/releases/download/1.53.4/clickhouse.metabase-driver.jar \
    && chmod 744 /home/plugins/clickhouse.metabase-driver.jar

# Скачиваем Metabase
ADD https://downloads.metabase.com/v0.52.13/metabase.jar /home/metabase.jar

# Скачиваем DuckDB драйвер
ADD https://github.com/MotherDuck-Open-Source/metabase_duckdb_driver/releases/download/0.2.12-b/duckdb.metabase-driver.jar /home/plugins/

# Даём права на исполнение всем .jar в plugins
RUN chmod 744 /home/plugins/*.jar

CMD ["java", "-jar", "/home/metabase.jar"]