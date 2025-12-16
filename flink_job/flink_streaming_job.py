
import json
import logging
from datetime import datetime
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream.functions import MapFunction
from pyflink.table import StreamTableEnvironment, EnvironmentSettings
from pyflink.table.expressions import col

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class JsonParser(MapFunction):
    
    def map(self, value):
        try:
            message = json.loads(value)
            return message.get('data', {})
        except Exception as e:
            logger.error(f"Error parsing JSON: {e}")
            return {}


def create_postgres_tables(t_env):
    t_env.execute_sql("""
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
            'password' = 'admin123',
            'driver' = 'org.postgresql.Driver'
        )
    """)
    
    t_env.execute_sql("""
        CREATE TABLE IF NOT EXISTS dim_sellers_sink (
            seller_id INT,
            seller_first_name STRING,
            seller_last_name STRING,
            seller_email STRING,
            seller_country STRING,
            seller_postal_code STRING,
            PRIMARY KEY (seller_id) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/lab3',
            'table-name' = 'dim_sellers',
            'username' = 'admin',
            'password' = 'admin123',
            'driver' = 'org.postgresql.Driver'
        )
    """)
    
    t_env.execute_sql("""
        CREATE TABLE IF NOT EXISTS dim_products_sink (
            product_id INT,
            product_name STRING,
            product_category STRING,
            product_price DECIMAL(10, 2),
            product_weight DECIMAL(10, 2),
            product_color STRING,
            product_size STRING,
            product_brand STRING,
            product_material STRING,
            product_description STRING,
            product_rating DECIMAL(3, 2),
            product_reviews INT,
            product_release_date STRING,
            product_expiry_date STRING,
            pet_category STRING,
            PRIMARY KEY (product_id) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/lab3',
            'table-name' = 'dim_products',
            'username' = 'admin',
            'password' = 'admin123',
            'driver' = 'org.postgresql.Driver'
        )
    """)
    
    t_env.execute_sql("""
        CREATE TABLE IF NOT EXISTS dim_stores_sink (
            store_id INT,
            store_name STRING,
            store_location STRING,
            store_city STRING,
            store_state STRING,
            store_country STRING,
            store_phone STRING,
            store_email STRING,
            PRIMARY KEY (store_id) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/lab3',
            'table-name' = 'dim_stores',
            'username' = 'admin',
            'password' = 'admin123',
            'driver' = 'org.postgresql.Driver'
        )
    """)
    
    t_env.execute_sql("""
        CREATE TABLE IF NOT EXISTS dim_suppliers_sink (
            supplier_id INT,
            supplier_name STRING,
            supplier_contact STRING,
            supplier_email STRING,
            supplier_phone STRING,
            supplier_address STRING,
            supplier_city STRING,
            supplier_country STRING,
            PRIMARY KEY (supplier_id) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/lab3',
            'table-name' = 'dim_suppliers',
            'username' = 'admin',
            'password' = 'admin123',
            'driver' = 'org.postgresql.Driver'
        )
    """)
    
    t_env.execute_sql("""
        CREATE TABLE IF NOT EXISTS fact_sales_sink (
            customer_key INT,
            seller_key INT,
            product_key INT,
            store_key INT,
            supplier_key INT,
            sale_date STRING,
            sale_quantity INT,
            sale_total_price DECIMAL(10, 2),
            product_quantity INT
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/lab3',
            'table-name' = 'fact_sales',
            'username' = 'admin',
            'password' = 'admin123',
            'driver' = 'org.postgresql.Driver'
        )
    """)


def main():
    """Основная функция для запуска Flink job"""
    
    logger.info("="*50)
    logger.info("Starting Flink Streaming Job")
    logger.info("="*50)
    
    logger.info("Creating StreamExecutionEnvironment...")
    env = StreamExecutionEnvironment.get_execution_environment()
    logger.info("StreamExecutionEnvironment created successfully")
    
    logger.info("Setting parallelism...")
    env.set_parallelism(2)
    logger.info("Parallelism set to 2")
    
    logger.info("Creating EnvironmentSettings...")
    settings = EnvironmentSettings.new_instance() \
        .in_streaming_mode() \
        .build()
    logger.info("EnvironmentSettings created")
    
    logger.info("Creating StreamTableEnvironment...")
    t_env = StreamTableEnvironment.create(env, settings)
    logger.info("StreamTableEnvironment created successfully")
    
    kafka_jar = 'file:///opt/flink/lib/flink-sql-connector-kafka-3.0.1-1.17.jar'
    postgres_jar = 'file:///opt/flink/lib/postgresql-42.6.0.jar'
    jdbc_jar = 'file:///opt/flink/lib/flink-connector-jdbc-3.1.1-1.17.jar'
    
    t_env.get_config().get_configuration().set_string(
        "pipeline.jars",
        f"{kafka_jar};{postgres_jar};{jdbc_jar}"
    )
    
    logger.info("Creating Kafka source table")
    t_env.execute_sql("""
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
        )
    """)
    
    logger.info("Creating PostgreSQL sink tables")
    create_postgres_tables(t_env)
    
    logger.info("Processing dim_customers")
    t_env.execute_sql("""
        INSERT INTO dim_customers_sink
        SELECT DISTINCT
            CAST(sale_customer_id AS INT) as customer_id,
            customer_first_name,
            customer_last_name,
            CAST(customer_age AS INT) as customer_age,
            customer_email,
            customer_country,
            customer_postal_code,
            customer_pet_type,
            customer_pet_name,
            customer_pet_breed
        FROM kafka_source
        WHERE sale_customer_id IS NOT NULL AND sale_customer_id <> ''
    """).wait()
    
    logger.info("Processing dim_sellers")
    t_env.execute_sql("""
        INSERT INTO dim_sellers_sink
        SELECT DISTINCT
            CAST(sale_seller_id AS INT) as seller_id,
            seller_first_name,
            seller_last_name,
            seller_email,
            seller_country,
            seller_postal_code
        FROM kafka_source
        WHERE sale_seller_id IS NOT NULL AND sale_seller_id <> ''
    """).wait()
    
    logger.info("Processing dim_products")
    t_env.execute_sql("""
        INSERT INTO dim_products_sink
        SELECT DISTINCT
            CAST(sale_product_id AS INT) as product_id,
            product_name,
            product_category,
            CAST(product_price AS DECIMAL(10, 2)) as product_price,
            CAST(product_weight AS DECIMAL(10, 2)) as product_weight,
            product_color,
            product_size,
            product_brand,
            product_material,
            product_description,
            CAST(product_rating AS DECIMAL(3, 2)) as product_rating,
            CAST(product_reviews AS INT) as product_reviews,
            product_release_date,
            product_expiry_date,
            pet_category
        FROM kafka_source
        WHERE sale_product_id IS NOT NULL AND sale_product_id <> ''
    """).wait()
    
    logger.info("Processing dim_stores")
    t_env.execute_sql("""
        INSERT INTO dim_stores_sink
        SELECT DISTINCT
            CAST(id AS INT) as store_id,
            store_name,
            store_location,
            store_city,
            store_state,
            store_country,
            store_phone,
            store_email
        FROM kafka_source
        WHERE id IS NOT NULL AND id <> ''
    """).wait()
    
    logger.info("Processing dim_suppliers")
    t_env.execute_sql("""
        INSERT INTO dim_suppliers_sink
        SELECT DISTINCT
            CAST(id AS INT) as supplier_id,
            supplier_name,
            supplier_contact,
            supplier_email,
            supplier_phone,
            supplier_address,
            supplier_city,
            supplier_country
        FROM kafka_source
        WHERE id IS NOT NULL AND id <> ''
    """).wait()
    
    logger.info("Processing fact_sales")
    t_env.execute_sql("""
        INSERT INTO fact_sales_sink
        SELECT
            CAST(sale_customer_id AS INT) as customer_key,
            CAST(sale_seller_id AS INT) as seller_key,
            CAST(sale_product_id AS INT) as product_key,
            CAST(id AS INT) as store_key,
            CAST(id AS INT) as supplier_key,
            sale_date,
            CAST(sale_quantity AS INT) as sale_quantity,
            CAST(sale_total_price AS DECIMAL(10, 2)) as sale_total_price,
            CAST(product_quantity AS INT) as product_quantity
        FROM kafka_source
        WHERE id IS NOT NULL AND id <> ''
    """).wait()
    
    logger.info("Flink job completed successfully")


if __name__ == "__main__":
    main()

