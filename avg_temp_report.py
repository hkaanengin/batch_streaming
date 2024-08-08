import duckdb, psycopg2, os
from psycopg2 import sql

# DuckDB connection
duckdb_path = './data/weather_measurements.db'
duck_conn = duckdb.connect(duckdb_path)

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
        pg_conn.commit()

def transform_and_load():
    # Query to calculate average temperature over 10-minute intervals
    temp_query = """
    SELECT 
        time_bucket(INTERVAL '10 minutes', date_time) AS time_interval,
        AVG(CAST(temp AS FLOAT)) AS avg_temperature
    FROM 
        weather_measurements
    GROUP BY 
        time_bucket(INTERVAL '10 minutes', date_time)
    ORDER BY 
        time_interval
    """
    
    # Query to calculate average humidity over 20-minute intervals
    humidity_query = """
    SELECT 
        time_bucket(INTERVAL '20 minutes', date_time) AS time_interval,
        AVG(CAST(humidity AS FLOAT)) AS avg_humidity
    FROM 
        weather_measurements
    GROUP BY 
        time_bucket(INTERVAL '20 minutes', date_time)
    ORDER BY 
        time_interval
    """
    
    # Execute queries on DuckDB
    temp_result = duck_conn.execute(temp_query).fetchall()
    humidity_result = duck_conn.execute(humidity_query).fetchall()
    
    # Insert results into PostgreSQL
    with get_postgres_connection() as pg_conn:
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

    print(f"Transformed and upserted {len(temp_result)} rows of average temperature data.")
    print(f"Transformed and upserted {len(humidity_result)} rows of average humidity data.")

if __name__ == "__main__":
    create_postgres_tables()
    transform_and_load()
    duck_conn.close()