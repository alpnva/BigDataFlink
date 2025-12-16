# Лабораторная работа №3 — Streaming processing с помощью Flink
**Студент:** *Алапанова Эльза*
**Группа:** *М8О-209СВ-24*

---

## Цель работы

Реализовать потоковую обработку данных с использованием Apache Flink.

## Текнолоджии

* **Apache Flink** (версии 1.17.2, Python 3.10) — для потоковой обработки данных
* **Apache Kafka** (версии 7.5.0) — брокер для передачи сообщений
* **PostgreSQL** (версия 15) — хранилище для результатов
* **Docker/Docker Compose** — контейнеризация сервисов
* **Python** — реализация Kafka Producer и Flink job
* **pyflink** — Python API для Flink

## Архитектура решения

Сервисная архитектура реализована через Docker Compose:

| Сервис         | Назначение          | Порт | Примечания                                          |
| -------------- | ------------------- | ---- | --------------------------------------------------- |
| zookeeper      | Координация Kafka   | 2181 | Брокер Zookeeper                                    |
| kafka          | Kafka broker        | 9092 | Хранение и передача сообщений                       |
| postgres       | PostgreSQL          | 5432 | Хранилище для таблиц модели «звезда»                |
| jobmanager     | Flink JobManager    | 8081 | Управление Flink-кластером                          |
| taskmanager    | Flink TaskManager   | —    | Обработка потоков данных                            |
| kafka-producer | Генератор сообщений | —    | Читает CSV и отправляет JSON в Kafka                |
| flink-job      | Flink job submitter | —    | Выполняет потоковую обработку и запись в PostgreSQL |

Схема взаимодействия:

```
CSV files → Kafka Producer → Kafka Topic → Flink Job → PostgreSQL (Star Schema)
```

``` yaml
services:
  # Zookeeper для Kafka
  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    hostname: zookeeper
    container_name: zookeeper
    ports:
      - "2181:2181"
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    networks:
      - flink-network

  # Kafka broker
  kafka:
    image: confluentinc/cp-kafka:7.5.0
    hostname: kafka
    container_name: kafka
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
      - "29092:29092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: 'zookeeper:2181'
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: 'true'
    networks:
      - flink-network

  # PostgreSQL
  postgres:
    image: postgres:15
    container_name: postgres
    ports:
      - "5432:5432"
    environment:
      POSTGRES_DB: lab3
      POSTGRES_USER: admin
      POSTGRES_PASSWORD: admin123
    volumes:
      - postgres-data:/var/lib/postgresql/data
      - ./sql/init.sql:/docker-entrypoint-initdb.d/init.sql
    networks:
      - flink-network

  # Flink JobManager
  jobmanager:
    container_name: jobmanager
    hostname: jobmanager
    command: ["jobmanager"]
    image: ghcr.io/lakehq/flink:1.17.2-python3.10
    entrypoint: ["/docker-entrypoint.sh"]
    environment:
      - JOB_MANAGER_RPC_ADDRESS=jobmanager
      - POSTGRES_URL=postgresql://admin:admin123@postgres:5432/lab3
      - POSTGRES_USER=admin
      - POSTGRES_PASSWORD=admin123
      - KAFKA_BOOTSTRAP=kafka:9092
      - KAFKA_TOPIC=pet-sales
    ports: ["8081:8081"]
    networks:
      - flink-network
    volumes:
      - ./flink_job:/opt/flink/usrlib

  # Flink TaskManager
  taskmanager:
    container_name: taskmanager
    image: ghcr.io/lakehq/flink:1.17.2-python3.10
    hostname: taskmanager
    command: ["taskmanager"]
    depends_on: [jobmanager]
    entrypoint: ["/docker-entrypoint.sh"]
    environment:
      - JOB_MANAGER_RPC_ADDRESS=jobmanager
      - TASK_MANAGER_NUMBER_OF_TASK_SLOTS=4
      - POSTGRES_URL=postgresql://admin:admin123@postgres:5432/lab3
      - POSTGRES_USER=admin
      - POSTGRES_PASSWORD=admin123
      - KAFKA_BOOTSTRAP=kafka:9092
      - KAFKA_TOPIC=pet-sales
    networks:
      - flink-network
    volumes:
      - ./flink_job:/opt/flink/usrlib

  # Kafka Producer
  kafka-producer:
    build:
      context: ./kafka_prod
      dockerfile: Dockerfile
    container_name: kafka-producer
    depends_on:
      - kafka
      - postgres
    environment:
      KAFKA_BOOTSTRAP_SERVERS: kafka:9092
      KAFKA_TOPIC: pet-sales
      MESSAGE_DELAY: "0.05"
    volumes:
      - ./data:/app/data
    networks:
      - flink-network
    command: ["python", "producer.py"]
    restart: on-failure

  # Flink Job Submitter
  flink-job:
    build: ./flink_job
    platform: linux/amd64
    depends_on:
      - kafka
      - postgres
      - jobmanager
      - taskmanager
      - kafka-producer
    entrypoint: ["bash", "-c"]
    command:
      - |
        echo "Waiting for Kafka to be ready..."
        sleep 45
        echo "Waiting for Flink cluster to be ready..."
        sleep 15
        echo "Submitting Flink job..."
        flink run -m jobmanager:8081 -d --python /opt/flink/job/flink_streaming_job.py
        echo "Job submitted!"
    environment:
      POSTGRES_URL: postgresql://admin:admin123@postgres:5432/lab3
      POSTGRES_USER: admin
      POSTGRES_PASSWORD: admin123
      KAFKA_BOOTSTRAP: kafka:9092
      KAFKA_TOPIC: pet-sales
    networks:
      - flink-network
    volumes:
      - ./flink_job:/opt/flink/usrlib
    restart: "no"

networks:
  flink-network:
    driver: bridge

volumes:
  postgres-data:

```
## 7. Запуск лабораторной работы

1. Сборка образов:

```bash
docker-compose build
```

2. Запуск сервисов Kafka и PostgreSQL:

```bash
docker-compose up -d zookeeper kafka postgres
```

3. Создание Kafka topic:

```bash
docker-compose exec kafka kafka-topics --create --bootstrap-server kafka:9092 --replication-factor 1 --partitions 1 --topic pet-sales
```

4. Запуск Kafka Producer:

```bash
docker-compose up -d kafka-producer
```

5. Запуск Flink JobManager и TaskManager:

```bash
docker-compose up -d jobmanager taskmanager
```

6. Запуск Flink job submitter:

```bash
docker-compose up -d flink-job
```

---

## Исходные данные

* 10 CSV-файлов (`mock_data1.csv … mock_data10.csv`), по 1000 строк каждый
* Каждая строка преобразуется в JSON и отправляется в Kafka topic `pet-sales`

---


## Реализация

### Kafka Producer

* Читает CSV-файлы из `./data`
* Преобразует строки в JSON: добавляет `row_number`, `source_file`, `timestamp`
* Отправляет сообщения в Kafka topic `pet-sales`
* Реализована задержка между сообщениями через `MESSAGE_DELAY`

### Flink Streaming Job

* Читает сообщения из Kafka topic `pet-sales`
* Парсит JSON-сообщения
* Преобразует данные в модель «звезда»:

  * Dimensional tables: `dim_customers`, `dim_sellers`, `dim_products`, `dim_stores`, `dim_suppliers`
  * Fact table: `fact_sales`
* Пишет данные в PostgreSQL через JDBC
* Использует pyflink API, SQL-запросы для вставки данных

**Мониторинг:**

* Flink UI: [http://localhost:8081](http://localhost:8081)
* Проверка логов:

```bash
docker-compose logs -f flink-job
```

Отлично, это как раз **ключевой пункт отчёта**, и у тебя он реализован очень хорошо 👍
Ниже — **готовый раздел 4 для отчёта**, который можно **вставлять напрямую** (текст + пояснение + фрагменты кода). Я оформлю его в академическом стиле, как обычно требуют на ЛР.

---

## Код Apache Flink для трансформации данных в режиме streaming

Для реализации потоковой трансформации данных был разработан streaming-job на **Apache Flink** с использованием **PyFlink (Table API + SQL)**.
Задача Flink-приложения — считать потоковые данные из Kafka, преобразовать их в модель данных «звезда» и сохранить результат в PostgreSQL в режиме реального времени.

---

### Создание окружения Flink

В начале приложения создаётся потоковое окружение выполнения (`StreamExecutionEnvironment`) и табличное окружение (`StreamTableEnvironment`) в streaming-режиме:

```python
env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(2)

settings = EnvironmentSettings.new_instance() \
    .in_streaming_mode() \
    .build()

t_env = StreamTableEnvironment.create(env, settings)
```

Параллелизм установлен равным 2, что позволяет обрабатывать поток данных параллельно.

---

### Подключение коннекторов Kafka и PostgreSQL

Для работы с Kafka и PostgreSQL используются внешние коннекторы, которые подключаются через параметр `pipeline.jars`:

```python
kafka_jar = 'file:///opt/flink/lib/flink-sql-connector-kafka-3.0.1-1.17.jar'
postgres_jar = 'file:///opt/flink/lib/postgresql-42.6.0.jar'
jdbc_jar = 'file:///opt/flink/lib/flink-connector-jdbc-3.1.1-1.17.jar'

t_env.get_config().get_configuration().set_string(
    "pipeline.jars",
    f"{kafka_jar};{postgres_jar};{jdbc_jar}"
)
```

Это позволяет использовать Kafka как источник данных, а PostgreSQL — как приёмник (sink).

---

### Определение источника данных Kafka

Входные данные описываются как таблица `kafka_source`, которая читает сообщения из Kafka topic `pet-sales` в формате JSON:

```sql
CREATE TABLE kafka_source (
    id STRING,
    customer_first_name STRING,
    customer_last_name STRING,
    customer_age STRING,
    customer_email STRING,
    customer_country STRING,
    customer_postal_code STRING,
    customer_pet_type STRING,
    customer_pet_name STRING,
    customer_pet_breed STRING,
    seller_first_name STRING,
    seller_last_name STRING,
    seller_email STRING,
    seller_country STRING,
    seller_postal_code STRING,
    product_name STRING,
    product_category STRING,
    product_price STRING,
    product_quantity STRING,
    sale_date STRING,
    sale_customer_id STRING,
    sale_seller_id STRING,
    sale_product_id STRING,
    sale_quantity STRING,
    sale_total_price STRING,
    store_name STRING,
    store_location STRING,
    store_city STRING,
    store_state STRING,
    store_country STRING,
    store_phone STRING,
    store_email STRING,
    pet_category STRING,
    product_weight STRING,
    product_color STRING,
    product_size STRING,
    product_brand STRING,
    product_material STRING,
    product_description STRING,
    product_rating STRING,
    product_reviews STRING,
    product_release_date STRING,
    product_expiry_date STRING,
    supplier_name STRING,
    supplier_contact STRING,
    supplier_email STRING,
    supplier_phone STRING,
    supplier_address STRING,
    supplier_city STRING,
    supplier_country STRING
) WITH (
    'connector' = 'kafka',
    'topic' = 'pet-sales',
    'properties.bootstrap.servers' = 'kafka:9092',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true'
);
```

Таким образом, каждое сообщение из Kafka автоматически преобразуется в строку таблицы Flink.

---

### Создание таблиц модели «звезда» в PostgreSQL

В PostgreSQL создаются таблицы измерений (dimensions) и таблица фактов (fact):

* `dim_customers`
* `dim_sellers`
* `dim_products`
* `dim_stores`
* `dim_suppliers`
* `fact_sales`

Пример создания таблицы измерений:

```sql
CREATE TABLE IF NOT EXISTS dim_customers_sink (
    customer_id INT,
    customer_first_name STRING,
    customer_last_name STRING,
    customer_age INT,
    customer_email STRING,
    customer_country STRING,
    customer_postal_code STRING,
    customer_pet_type STRING,
    customer_pet_name STRING,
    customer_pet_breed STRING,
    PRIMARY KEY (customer_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/lab3',
    'table-name' = 'dim_customers',
    'username' = 'admin',
    'password' = 'admin123'
);
```

---

### Потоковая трансформация данных

Преобразование данных в модель «звезда» осуществляется с помощью SQL-запросов `INSERT INTO … SELECT`, которые выполняются **в режиме streaming**.

Пример заполнения таблицы `dim_customers`:

```sql
INSERT INTO dim_customers_sink
SELECT DISTINCT
    CAST(sale_customer_id AS INT) AS customer_id,
    customer_first_name,
    customer_last_name,
    CAST(customer_age AS INT) AS customer_age,
    customer_email,
    customer_country,
    customer_postal_code,
    customer_pet_type,
    customer_pet_name,
    customer_pet_breed
FROM kafka_source
WHERE sale_customer_id IS NOT NULL AND sale_customer_id <> '';
```

Заполнение таблицы фактов `fact_sales`:

```sql
INSERT INTO fact_sales_sink
SELECT
    CAST(sale_customer_id AS INT) AS customer_key,
    CAST(sale_seller_id AS INT) AS seller_key,
    CAST(sale_product_id AS INT) AS product_key,
    CAST(id AS INT) AS store_key,
    CAST(id AS INT) AS supplier_key,
    sale_date,
    CAST(sale_quantity AS INT) AS sale_quantity,
    CAST(sale_total_price AS DECIMAL(10,2)) AS sale_total_price,
    CAST(product_quantity AS INT) AS product_quantity
FROM kafka_source
WHERE id IS NOT NULL AND id <> '';
```

Все преобразования выполняются непрерывно по мере поступления новых сообщений из Kafka.

---

### Особенности потоковой обработки

* Данные обрабатываются **в режиме реального времени**
* Используется **Table API + SQL**, что упрощает реализацию ETL-логики
* Реализована модель данных **Star Schema**
* Flink автоматически управляет состоянием и масштабированием

