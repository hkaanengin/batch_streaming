import duckdb
import psycopg2
import os
from psycopg2 import sql
import time
from datetime import datetime, timedelta

# DuckDB connection
duckdb_path = './data/weather_measurements.db'

# PostgreSQL connection details
pg_host = os.getenv('POSTGRES_HOST', 'localhost')
pg_database = os.getenv('POSTGRES_DB', 'weather_db')
pg_user = os.getenv('POSTGRES_USER', 'user')
pg_password = os.getenv('POSTGRES_PASSWORD', 'password')
pg_port = os.getenv('POSTGRES_PORT', '5432')

def get_postgres_connection():
    return psycopg2.connect(
        host=pg_host,
        database=pg_database,
        user=pg_user,
        password=pg_password,
        port=pg_port
    )

def create_postgres_tables():
    with get_postgres_connection() as pg_conn:
        with pg_conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS avg_temperature_10min (
                    time_interval TIMESTAMP PRIMARY KEY,
                    avg_temperature FLOAT
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS avg_humidity_20min (
                    time_interval TIMESTAMP PRIMARY KEY,
                    avg_humidity FLOAT
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS etl_metadata (
                    key TEXT PRIMARY KEY,
                    value TIMESTAMP
                )
            """)
        pg_conn.commit()

def get_last_processed_time(pg_conn):
    with pg_conn.cursor() as cur:
        cur.execute("SELECT value FROM etl_metadata WHERE key = 'last_processed_time'")
        result = cur.fetchone()
        return result[0] if result else None

def update_last_processed_time(pg_conn, last_time):
    with pg_conn.cursor() as cur:
        cur.execute("""
            INSERT INTO etl_metadata (key, value) 
            VALUES ('last_processed_time', %s) 
            ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
        """, (last_time,))
    pg_conn.commit()

def transform_and_load():
    with get_postgres_connection() as pg_conn:
        last_processed_time = get_last_processed_time(pg_conn)
        
        # Set end_time to 1 minute ago to avoid conflicts with ongoing writes
        end_time = datetime.now() - timedelta(minutes=1)
        
        # If last_processed_time is None, set it to 24 hours ago
        if last_processed_time is None:
            last_processed_time = end_time - timedelta(hours=24)
        
        try:
            # Connect to DuckDB
            with duckdb.connect(duckdb_path, read_only=True) as duck_conn:
                # Query to calculate average temperature over 10-minute intervals
                temp_query = f"""
                SELECT 
                    time_bucket(INTERVAL '10 minutes', date_time) AS time_interval,
                    AVG(CAST(temperature AS FLOAT)) AS avg_temperature
                FROM 
                    weather_measurements
                WHERE
                    date_time > '{last_processed_time}' AND date_time <= '{end_time}'
                GROUP BY 
                    time_bucket(INTERVAL '10 minutes', date_time)
                ORDER BY 
                    time_interval
                """
                
                # Query to calculate average humidity over 20-minute intervals
                humidity_query = f"""
                SELECT 
                    time_bucket(INTERVAL '20 minutes', date_time) AS time_interval,
                    AVG(CAST(humidity AS FLOAT)) AS avg_humidity
                FROM 
                    weather_measurements
                WHERE
                    date_time > '{last_processed_time}' AND date_time <= '{end_time}'
                GROUP BY 
                    time_bucket(INTERVAL '20 minutes', date_time)
                ORDER BY 
                    time_interval
                """
                
                # Execute queries on DuckDB
                temp_result = duck_conn.execute(temp_query).fetchall()
                humidity_result = duck_conn.execute(humidity_query).fetchall()
            
            # Insert results into PostgreSQL
            with pg_conn.cursor() as cur:
                cur.executemany(
                    """
                    INSERT INTO avg_temperature_10min (time_interval, avg_temperature) 
                    VALUES (%s, %s)
                    ON CONFLICT (time_interval) 
                    DO UPDATE SET avg_temperature = EXCLUDED.avg_temperature
                    """,
                    temp_result
                )
                cur.executemany(
                    """
                    INSERT INTO avg_humidity_20min (time_interval, avg_humidity) 
                    VALUES (%s, %s)
                    ON CONFLICT (time_interval) 
                    DO UPDATE SET avg_humidity = EXCLUDED.avg_humidity
                    """,
                    humidity_result
                )
            pg_conn.commit()

            if temp_result or humidity_result:
                update_last_processed_time(pg_conn, end_time)

            print(f"Transformed and upserted {len(temp_result)} rows of average temperature data.")
            print(f"Transformed and upserted {len(humidity_result)} rows of average humidity data.")

        except Exception as e:
            print(f"An error occurred: {e}")

if __name__ == "__main__":
    create_postgres_tables()
    transform_and_load()