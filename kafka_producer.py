import pandas as pd
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError
import json
from time import sleep
import logging
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_kafka_topic(admin_client, topic_name):
    try:
        topic = NewTopic(name=topic_name, num_partitions=1, replication_factor=1)
        admin_client.create_topics([topic])
        logger.info(f"Topic '{topic_name}' created successfully")
    except TopicAlreadyExistsError:
        logger.info(f"Topic '{topic_name}' already exists")

def send_parquet_data_to_kafka(producer, df, topic):
    batch = []
    batch_size = 100  # Adjust based on your needs

    for index, row in df.iterrows():
        message = row.to_dict()
        batch.append(message)
        
        if len(batch) >= batch_size:
            send_batch(producer, topic, batch)
            batch = []
        
    # Send any remaining messages
    if batch:
        send_batch(producer, topic, batch)

def send_batch(producer, topic, batch):
    for message in batch:
        producer.send(topic, value=message)
    producer.flush()
    logger.info(f"Sent batch of {len(batch)} messages")

if __name__ == '__main__':
    bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:29092').split(',')
    topic_name = os.getenv('KAFKA_TOPIC', 'Weather_Measurements')
    parquet_file = os.getenv('PARQUET_FILE', 'measurements.parquet')

    admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)

    create_kafka_topic(admin_client, topic_name)
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    try:
        df = pd.read_parquet(parquet_file)
        send_parquet_data_to_kafka(producer, df, topic_name)
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
    finally:
        producer.close()