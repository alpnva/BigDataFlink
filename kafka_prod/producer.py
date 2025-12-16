import os
import csv
import json
import time
from kafka import KafkaProducer
from kafka.errors import KafkaError
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class CSVToKafkaProducer:
    def __init__(self, bootstrap_servers, topic_name):
        self.topic_name = topic_name
        self.producer = None
        self.bootstrap_servers = bootstrap_servers
        max_retries = 30
        for attempt in range(max_retries):
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=bootstrap_servers,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    key_serializer=lambda k: k.encode('utf-8') if k else None,
                    acks='all',
                    retries=3,
                    max_in_flight_requests_per_connection=1
                )
                logger.info(f"Successfully connected to Kafka at {bootstrap_servers}")
                break
            except Exception as e:
                logger.warning(f"Attempt {attempt + 1}/{max_retries}: Unable to connect to Kafka: {e}")
                if attempt < max_retries - 1:
                    time.sleep(2)
                else:
                    raise Exception("Failed to connect to Kafka after multiple attempts")

    def send_message(self, message, key=None):
        try:
            future = self.producer.send(
                self.topic_name,
                value=message,
                key=key
            )
            
            # Ждем подтверждения отправки
            record_metadata = future.get(timeout=10)
            logger.debug(
                f"Message sent to {record_metadata.topic} "
                f"partition {record_metadata.partition} "
                f"offset {record_metadata.offset}"
            )
            return True
        except KafkaError as e:
            logger.error(f"Failed to send message: {e}")
            return False

    def process_csv_file(self, csv_file_path, delay=0.1):
        logger.info(f"Processing file: {csv_file_path}")
        
        messages_sent = 0
        messages_failed = 0
        
        try:
            with open(csv_file_path, 'r', encoding='utf-8') as csvfile:
                csv_reader = csv.DictReader(csvfile)
                
                for row_num, row in enumerate(csv_reader, start=1):
                    message = {
                        'source_file': os.path.basename(csv_file_path),
                        'row_number': row_num,
                        'timestamp': time.time(),
                        'data': row
                    }

                    key = row.get('id', str(row_num))
            
                    if self.send_message(message, key=key):
                        messages_sent += 1
                        if messages_sent % 100 == 0:
                            logger.info(f"Sent {messages_sent} messages from {csv_file_path}")
                    else:
                        messages_failed += 1
                    
                    if delay > 0:
                        time.sleep(delay)
        
        except Exception as e:
            logger.error(f"Error processing file {csv_file_path}: {e}")
            raise
        
        logger.info(
            f"Finished processing {csv_file_path}: "
            f"{messages_sent} sent, {messages_failed} failed"
        )
        
        return messages_sent, messages_failed

    def process_directory(self, directory_path, delay=0.1):
        logger.info(f"Processing directory: {directory_path}")
        
        total_sent = 0
        total_failed = 0
        
        csv_files = sorted([
            f for f in os.listdir(directory_path)
            if f.endswith('.csv')
        ])
        
        if not csv_files:
            logger.warning(f"No CSV files found in {directory_path}")
            return
        
        logger.info(f"Found {len(csv_files)} CSV files to process")
        
        for csv_file in csv_files:
            csv_file_path = os.path.join(directory_path, csv_file)
            sent, failed = self.process_csv_file(csv_file_path, delay=delay)
            total_sent += sent
            total_failed += failed
        
        logger.info(
            f"Total processing complete: "
            f"{total_sent} messages sent, {total_failed} failed"
        )

    def close(self):
        if self.producer:
            self.producer.flush()
            self.producer.close()
            logger.info("Kafka producer closed")


def main():
    kafka_bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    kafka_topic = os.getenv('KAFKA_TOPIC', 'pet-sales')
    data_directory = os.getenv('DATA_DIRECTORY', '/app/data')
    message_delay = float(os.getenv('MESSAGE_DELAY', '0.1'))
    
    logger.info("="*50)
    logger.info("CSV to Kafka Producer")
    logger.info("="*50)
    logger.info(f"Kafka Bootstrap Servers: {kafka_bootstrap_servers}")
    logger.info(f"Kafka Topic: {kafka_topic}")
    logger.info(f"Data Directory: {data_directory}")
    logger.info(f"Message Delay: {message_delay}s")
    logger.info("="*50)
    
    producer = CSVToKafkaProducer(
        bootstrap_servers=kafka_bootstrap_servers,
        topic_name=kafka_topic
    )
    
    try:
        producer.process_directory(data_directory, delay=message_delay)
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    except Exception as e:
        logger.error(f"Error: {e}")
    finally:
        producer.close()
    
    logger.info("Producer finished")


if __name__ == "__main__":
    main()

