import pandas as pd
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError
import json
from time import sleep


def create_kafka_topic(admin_client, topic_name):
    try:
        topic = NewTopic(name=topic_name, num_partitions=1, replication_factor=1)
        admin_client.create_topics([topic])
        print(f"Topic '{topic_name}' created successfully")
    except TopicAlreadyExistsError:
        print(f"Topic '{topic_name}' already exists")

# Iterate through each row and send to Kafka
def send_parquet_data_to_kafka(producer, df, topic):
    for index, row in df.iterrows():
        # Convert row to dictionary
        message = row.to_dict()
        
        # Send message to Kafka
        producer.send(topic, value=message)
        
        print(f"Sent message: {message}")
        
        # Optional: add a small delay to control the rate of sending
        sleep(0.1)


if __name__ == '__main__':

    bootstrap_servers = ['localhost:29092']
    topic_name = 'Weather_Measurements'
    admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)

    create_kafka_topic(admin_client, topic_name)
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    df = pd.read_parquet('measurements.parquet')
    send_parquet_data_to_kafka(producer, df, topic_name)
    producer.flush()
