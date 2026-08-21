# Purpose: is the factory manager — it runs all these workers automatically, in the correct order, every hour, and alerts you if anything breaks.
# Right now you run producer.py manually. After this file, Airflow does it for you automatically.


from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
sys.path.insert(0, '/opt/airflow/dags')
from airflow.providers.http.sensors.http import HttpSensor
from airflow.sensors.filesystem import FileSensor
from airflow.sensors.sql import SqlSensor
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.datasets import Dataset


import logging

logger = logging.getLogger(__name__)


crypto_dataset = Dataset("postgres://data_platform/crypto_events")


# step 4 — alert_on_failure  why is it here - Alerts you if anything breaks in the pipeline.
def alert_on_failure(context):
    task_id = context['task_instance'].task_id
    dag_id = context['task_instance'].dag_id
    execution_date = context['execution_date']
    logger.error(f"TASK FAILED — DAG: {dag_id} | Task: {task_id} | Time: {execution_date}")

# Task fails → wait 5 minutes → retry → fails again → wait 5 minutes → retry → fails again → wait 5 minutes → retry → after 3 retries → FAILED (red) → alert_on_failure() called



# step 1 — fetch_crypto_prices  why is it here - Spark streaming (spark_stream_pg.py) runs separately and continuously — it doesn't need Airflow to trigger it.
def fetch_crypto_prices():
    logger.info("Starting crypto price fetch...")
    from api_service.fetchers.crypto import fetch_crypto
    try:
        prices = fetch_crypto()
        logger.info(f"Fetched {len(prices)} coins from CoinGecko")
        return len(prices)
    except Exception as e:
        logger.error(f"Failed to fetch prices: {e}")
        raise


def fetch_one_coin(coin_id: str):
    import requests
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {
        "vs_currency": "usd",
        "ids": coin_id,
    }
    response = requests.get(url, params=params, timeout=10)
    response.raise_for_status()
    data = response.json()
    logger.info(f"Fetched {coin_id}: ${data[0]['current_price']}")
    return data[0]['current_price']


# Hooks: PostgresHook A Hook is the Python tool that uses a Connection to actually connect to a service.
def save_prices_to_db(**context):
    from api_service.fetchers.crypto import fetch_crypto
    hook = PostgresHook(postgres_conn_id='crypto_postgres')   # Accesses the connection to the DB created in Airflow (crypto_postgres')
    conn  = hook.get_conn()
    cursor = conn.cursor()
    prices = fetch_crypto()
    for price in prices:
        cursor.execute("""
            INSERT INTO crypto_events
            (coin_id, coin_name, symbol, price_usd, fetched_at)
            VALUES (%s, %s, %s, %s, %s)
        """, (price.coin_id, price.coin_name, price.symbol, price.price_usd, price.fetched_at))
    conn.commit()
    cursor.close()
    logger.info(f"Saved {len(prices)} coins to PostgreSQL")




# step 2 — produce_to_kafka  why is it here - Airflow handles the scheduled batch ingestion part.
def log_pipeline_status(**context):       # **context gives the function access to Airflow's task context — including XCom data, execution date, dag_run info. Without this you cannot pull XCom values.
    rows = context['ti'].xcom_pull(task_ids='fetch_crypto_prices')
    import psycopg2
    conn = psycopg2.connect("postgresql://arish:Arish200502@postgres:5432/data_platform")
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO pipeline_logs (pipeline_name, task_name, status, rows_processed)
        VALUES ('ingest_dag', 'fetch_crypto_prices', 'success', %s)
    """, (rows,))  
    conn.commit()
    conn.close()
    logger.info("Pipeline log saved to PostgreSQL successfully")

with DAG(
    dag_id="crypto_ingest_dag",
    start_date=datetime(2026, 5, 14),
    schedule_interval="0 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args={               # Default settings applied to ALL tasks in this DAG — so both fetch_crypto_prices and log_pipeline_status get retries automatically.
        'retries': 3,    # Automatically retry a failed task 3 times before marking it failed.
        'retry_delay': timedelta(seconds=10),     # Wait 5 minutes between each retry — gives CoinGecko API time to recover.
        'on_failure_callback': alert_on_failure,  # Send an alert if any task fails for any reason (like a timeout).
        'sla': timedelta(minutes=10),       # This DAG MUST finish within 10 minutes. If it takes longer, Airflow sends an alert.
        'email': ['arishmahammad8@gmail.com'],
        'email_on_failure': True,
        'email_on_retry': False,
    },
    tags=["crypto", "ingest"],
) as dag:

    check_db = SqlSensor(                 # SqlSensor — checks database has data before logging status
        task_id='check_db_connection',
        conn_id='crypto_postgres',
        sql="SELECT 1 FROM crypto_events LIMIT 1;",
        poke_interval=30,
        timeout=300,
    )

    check_file = FileSensor(           # FileSensor — checks input file exists before processing
        task_id='check_input_file',
        filepath='/opt/airflow/dags/data/crypto_input.csv',
        poke_interval=30,
        timeout=300,
        mode='poke',           # Sensor actively checks every 30 seconds. Alternative is reschedule mode which frees up the worker slot between checks — important for production.
    )

    check_api = HttpSensor(           # HttpSensor — checks CoinGecko API is responding before fetching
        task_id='check_coingecko_api',
        http_conn_id='coingecko_api',
        endpoint='coins/markets',
        request_params={          # CoinGecko API endpoint parameters to fetch Bitcoin prices in USD only
            'vs_currency': 'usd',
            'ids': 'bitcoin'
        },
        poke_interval=30,   # Check every 30 seconds
        timeout=300,       # Give up after 5 minutes if API never responds
    ),

    coins = ['bitcoin', 'ethereum', 'solana', 'dogecoin', 'ripple']
    fetch_tasks = []
    for coin in coins:
        t = PythonOperator(
            task_id=f"fetch_{coin}",   # PythonOperator — runs a Python function as a task in Airflow
            python_callable=fetch_one_coin,    # the function
            op_args=[coin],      # the arguments to pass to the function
        )
        fetch_tasks.append(t)


    task2 = PythonOperator(
        task_id = "log_pipeline_status",
        python_callable = log_pipeline_status,
        provide_context = True,
    )

    save_task = PythonOperator(      # PythonOperator — runs a Python function as a task in Airflow
        task_id="save_prices_to_db",
        python_callable=save_prices_to_db,
        outlets=[crypto_dataset],
    )

    check_api >> check_file >> fetch_tasks >> save_task >> check_db >> task2    # Sequence of tasks 
