from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import psycopg2

API_KEY = "your_api_key_here"
CITY = "Ho Chi Minh City"

DB_CONFIG = {
    "host": "host.docker.internal",
    "database": "weather_db",
    "user": "postgres",
    "password": "your_password_here",
    "port": "5432"
}

def get_conn():
    return psycopg2.connect(**DB_CONFIG)

# ===== TASK 1: EXTRACT =====
def extract_weather():
    url = f"http://api.openweathermap.org/data/2.5/weather?q={CITY}&appid={API_KEY}&units=metric"
    response = requests.get(url)
    data = response.json()
    return data

# ===== TASK 2: TRANSFORM =====
def transform_weather(**context):
    data = context['ti'].xcom_pull(task_ids='extract')
    transformed = {
        "city": data["name"],
        "temperature": data["main"]["temp"],
        "humidity": data["main"]["humidity"],
        "weather_desc": data["weather"][0]["description"],
        "wind_speed": data["wind"]["speed"],
        "recorded_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    return transformed

# ===== TASK 3: LOAD RAW =====
def load_raw(**context):
    data = context['ti'].xcom_pull(task_ids='transform')
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO raw_weather (city, temperature, humidity, weather_desc, wind_speed, recorded_at)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (
        data["city"], data["temperature"], data["humidity"],
        data["weather_desc"], data["wind_speed"], data["recorded_at"]
    ))
    conn.commit()
    cursor.close()
    conn.close()

# ===== TASK 4: LOAD DIM DATE =====
def load_dim_date(**context):
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO dim_date (full_date, year, month, day, hour, day_of_week)
        SELECT DISTINCT 
            full_date, year, month, day, hour, TRIM(day_of_week)
        FROM silver_weather
        WHERE CONCAT(full_date::TEXT, hour::TEXT) NOT IN (
            SELECT CONCAT(full_date::TEXT, hour::TEXT) FROM dim_date
        )
    """)
    conn.commit()
    cursor.close()
    conn.close()

# ===== TASK 5: LOAD DIM LOCATION =====
def load_dim_location(**context):
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO dim_location (city, country, latitude, longitude)
        SELECT DISTINCT city, 'Vietnam', 10.8231, 106.6297
        FROM silver_weather
        WHERE city NOT IN (SELECT city FROM dim_location)
    """)
    conn.commit()
    cursor.close()
    conn.close()

# ===== TASK 6: LOAD DIM CONDITION =====
def load_dim_condition(**context):
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO dim_condition (description, category, severity_level)
        SELECT DISTINCT weather_desc, category, severity_level
        FROM silver_weather
        WHERE weather_desc NOT IN (SELECT description FROM dim_condition)
    """)
    conn.commit()
    cursor.close()
    conn.close()

# ===== TASK 7: LOAD DIM TIME =====
def load_dim_time(**context):
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO dim_time_of_day (time_label, period, is_peak_hour)
        SELECT DISTINCT CONCAT(hour, ':00'), period, is_peak_hour
        FROM silver_weather
        WHERE CONCAT(hour, ':00') NOT IN (SELECT time_label FROM dim_time_of_day)
    """)
    conn.commit()
    cursor.close()
    conn.close()

# ===== TASK 8: LOAD FACT =====
def load_fact(**context):
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO fact_weather (date_id, location_id, condition_id, time_id, temperature, humidity, wind_speed, raw_id)
        SELECT
            d.date_id, l.location_id, c.condition_id, t.time_id,
            s.temperature, s.humidity, s.wind_speed, s.id
        FROM silver_weather s
        JOIN dim_date d         ON d.full_date = s.full_date AND d.hour = s.hour
        JOIN dim_location l     ON l.city = s.city
        JOIN dim_condition c    ON c.description = s.weather_desc
        JOIN dim_time_of_day t  ON t.time_label = CONCAT(s.hour, ':00')
        WHERE s.id NOT IN (
            SELECT raw_id FROM fact_weather WHERE raw_id IS NOT NULL
        )
    """)
    conn.commit()
    cursor.close()
    conn.close()

# ===== DAG =====
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    dag_id="weather_etl",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@hourly",
    catchup=False
) as dag:

    extract = PythonOperator(task_id="extract", python_callable=extract_weather)
    transform = PythonOperator(task_id="transform", python_callable=transform_weather)
    load_raw = PythonOperator(task_id="load_raw", python_callable=load_raw)
    dim_date = PythonOperator(task_id="load_dim_date", python_callable=load_dim_date)
    dim_location = PythonOperator(task_id="load_dim_location", python_callable=load_dim_location)
    dim_condition = PythonOperator(task_id="load_dim_condition", python_callable=load_dim_condition)
    dim_time = PythonOperator(task_id="load_dim_time", python_callable=load_dim_time)
    load_fact_task = PythonOperator(task_id="load_fact", python_callable=load_fact)

    extract >> transform >> load_raw >> [dim_date, dim_location, dim_condition, dim_time] >> load_fact_task