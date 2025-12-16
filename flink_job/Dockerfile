FROM ghcr.io/lakehq/flink:1.17.2-python3.10

USER root

# Скачивание необходимых JAR файлов для коннекторов (версия 1.17 для совместимости)
RUN wget -P /opt/flink/lib/ https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.0.1-1.17/flink-sql-connector-kafka-3.0.1-1.17.jar && \
    wget -P /opt/flink/lib/ https://jdbc.postgresql.org/download/postgresql-42.6.0.jar && \
    wget -P /opt/flink/lib/ https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc/3.1.1-1.17/flink-connector-jdbc-3.1.1-1.17.jar

USER flink

WORKDIR /opt/flink/job
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY flink_streaming_job.py ./

