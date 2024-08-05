from kafka import KafkaConsumer
import json, duckdb, os
from datetime import datetime

def create_table(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS weather_measurements (
            id INTEGER PRIMARY KEY,
            temperature FLOAT,
            humidity FLOAT,
            date_time TIMESTAMP
        )
    """)

def convert_to_timestamp(conn, date_time):
    return conn.execute(f"SELECT CAST('{date_time}' AS TIMESTAMP)").fetchone()[0]


if __name__ == '__main__':
    bootstrap_servers = ['localhost:29092']
    topic_name = 'Weather_Measurements'

    consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=bootstrap_servers,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='weather_group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    data_dir = './data'  # This should match your Docker volume mapping
    duckdb_file = os.path.join(data_dir, 'weather_measurements.db')
    os.makedirs(data_dir, exist_ok=True)

    try:
        conn = duckdb.connect(duckdb_file)
        print(f"Connected to DuckDB. Starting to consume messages from Kafka topic '{topic_name}'...")
    except Exception as e:
        print(f"Error connecting to DuckDB: {e}")
        exit(1)

    create_table(conn)
    try:
        for message in consumer:
            data = message.value
            
            # Insert data into DuckDB
            conn.execute("""
                INSERT INTO weather_measurements (id, temperature, humidity, date_time)
                VALUES (?, ?, ?, ?)
            """, (data['id'], data['temp'], data['humidity'], convert_to_timestamp(conn, data['date_time']))
            )
            
            print(f"Inserted data: {data}")
            conn.commit()

    except KeyboardInterrupt:
        print("Stopping the consumer...")
    finally:
        # Commit any pending transactions and close the connection
        conn.commit()
        conn.close()
        consumer.close()