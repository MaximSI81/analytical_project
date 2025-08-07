FROM eclipse-temurin:19-jdk-jammy

ENV MB_PLUGINS_DIR=/home/plugins/
RUN mkdir -p /home/plugins/ && chmod 755 /home/plugins/

ADD https://downloads.metabase.com/v0.52.13/metabase.jar /home/metabase.jar

ADD https://github.com/ClickHouse/metabase-clickhouse-driver/releases/download/1.53.4/clickhouse.metabase-driver.jar /home/plugins/

ADD https://github.com/MotherDuck-Open-Source/metabase_duckdb_driver/releases/download/0.2.12-b/duckdb.metabase-driver.jar /home/plugins/

RUN chmod 744 /home/plugins/*.jar

CMD ["java", "-jar", "/home/metabase.jar"]