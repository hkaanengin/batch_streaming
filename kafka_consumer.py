from kafka import KafkaConsumer
import json, duckdb, os, logging
from datetime import datetime
import signal

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_table(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS weather_measurements (
            id INTEGER PRIMARY KEY,
            temperature FLOAT,
            humidity FLOAT,
            date_time TIMESTAMP,
            p_date STRING,
            p_hour STRING
        )
    """)

def convert_to_timestamp(conn, date_time):
    return conn.execute(f"SELECT CAST('{date_time}' AS TIMESTAMP)").fetchone()[0]

def validate_data(data):
    required_fields = ['id', 'temp', 'humidity', 'date_time']
    return all(field in data for field in required_fields)

def insert_batch(conn, batch):
    conn.executemany("""
        INSERT INTO weather_measurements (id, temperature, humidity, date_time, p_date, p_hour)
        VALUES (?, ?, ?, ?, ?, ?)
    """, batch)
    conn.commit()

def signal_handler(signum, frame):
    logger.info("Received termination signal. Closing connections...")
    global running
    running = False

if __name__ == '__main__':
    bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:29092').split(',')
    topic_name = os.getenv('KAFKA_TOPIC', 'Weather_Measurements')
    data_dir = os.getenv('DATA_DIR', './data')
    duckdb_file = os.path.join(data_dir, 'weather_measurements.db')
    batch_size = int(os.getenv('BATCH_SIZE', 100))

    os.makedirs(data_dir, exist_ok=True)

    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='weather_group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    try:
        conn = duckdb.connect(duckdb_file)
        logger.info(f"Connected to DuckDB. Starting to consume messages from Kafka topic '{topic_name}'...")
        create_table(conn)
    except Exception as e:
        logger.error(f"Error connecting to DuckDB: {e}")
        exit(1)

    batch = []
    running = True
    try:
        while running:
            for message in consumer:
                data = message.value
                if validate_data(data):
                    batch.append((
                        data['id'],
                        data['temp'],
                        data['humidity'],
                        convert_to_timestamp(conn, data['date_time']),
                        datetime.now().strftime('%Y-%m-%d'),
                        datetime.now().strftime('%H')
                    ))
                    
                    if len(batch) >= batch_size:
                        insert_batch(conn, batch)
                        logger.info(f"Inserted batch of {len(batch)} records")
                        batch = []
                else:
                    logger.warning(f"Skipping invalid data: {data}")

                if not running:
                    break

    except Exception as e:
        logger.error(f"An error occurred: {e}")
    finally:
        if batch:
            insert_batch(conn, batch)
            logger.info(f"Inserted final batch of {len(batch)} records")
        conn.close()
        consumer.close()
        logger.info("Consumer stopped and connections closed.")