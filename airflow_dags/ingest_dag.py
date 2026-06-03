# FILE: airflow_dags/ingest_dag.py
# PURPOSE: Scheduled pipeline — fetches live crypto prices
#          from CoinGecko API and saves to PostgreSQL


from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
sys.path.insert(0, '/opt/airflow/dags')  # so Airflow can find api_service folder
from airflow.providers.http.sensors.http import HttpSensor
from airflow.sensors.filesystem import FileSensor
from airflow.sensors.sql import SqlSensor
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.datasets import Dataset
import logging

logger = logging.getLogger(__name__)

# ── DATASET DEFINITION ──────────────────────────────────────
# URI is just a label — must be IDENTICAL in analytics_dag.py
# When save_prices_to_db succeeds → this dataset is marked updated
# → analytics_dag triggers automatically (event-driven)
crypto_dataset = Dataset("postgres://data_platform/crypto_events")


# ── FUNCTION 1: alert_on_failure ────────────────────────────
# Called automatically by Airflow when ANY task fails
# context = Airflow passes this automatically — contains task info
def alert_on_failure(context):
    # DRY RUN:
    # context = {task_instance: <TaskInstance fetch_bitcoin ...>, execution_date: 2026-05-22 10:00, ...}
    task_id = context['task_instance'].task_id        # e.g. "fetch_bitcoin"
    dag_id = context['task_instance'].dag_id          # e.g. "crypto_ingest_dag"
    execution_date = context['execution_date']        # e.g. 2026-05-22 10:00:00
    logger.error(f"TASK FAILED — DAG: {dag_id} | Task: {task_id} | Time: {execution_date}")
    # OUTPUT: ERROR - TASK FAILED — DAG: crypto_ingest_dag | Task: fetch_bitcoin | Time: 2026-05-22 10:00:00
    # This log appears in Airflow UI → task logs → red ERROR line


# ── FUNCTION 2: fetch_crypto_prices ─────────────────────────
# Fetches ALL 5 coins together — returns count for XCom
# Used by log_pipeline_status to know how many rows were processed
def fetch_crypto_prices():
    logger.info("Starting crypto price fetch...")
    # Import inside function — avoids heavy import at DAG parse time
    # Airflow parses DAGs every 30 seconds — top-level imports slow this down
    from api_service.fetchers.crypto import fetch_crypto
    try:
        prices = fetch_crypto()
        # DRY RUN: prices = [CryptoPrice(coin_id='bitcoin', price_usd=77063.0, ...),
        #                     CryptoPrice(coin_id='ethereum', price_usd=2121.1, ...),
        #                     ... 3 more coins]
        logger.info(f"Fetched {len(prices)} coins from CoinGecko")
        # OUTPUT: INFO - Fetched 5 coins from CoinGecko
        return len(prices)
        # RETURNS: 5 — this value goes into XCom automatically
        # log_pipeline_status pulls this 5 later via xcom_pull
    except Exception as e:
        logger.error(f"Failed to fetch prices: {e}")
        raise  # re-raises so Airflow marks task FAILED and triggers retry


# ── FUNCTION 3: fetch_one_coin ───────────────────────────────
# Fetches price for exactly ONE coin — called by dynamic tasks
# coin_id comes from op_args=[coin] in the PythonOperator loop
def fetch_one_coin(coin_id: str):
    # DRY RUN (when called for bitcoin):
    # coin_id = "bitcoin"
    import requests
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {
        "vs_currency": "usd",
        "ids": coin_id,           # DRY RUN: "ids": "bitcoin"
    }
    response = requests.get(url, params=params, timeout=10)
    # DRY RUN: requests.get("https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&ids=bitcoin")
    # timeout=10 — if CoinGecko doesn't respond in 10 seconds → raise Timeout error → retry

    response.raise_for_status()
    # DRY RUN: status=200 → do nothing. status=429 → raise HTTPError → retry

    data = response.json()
    # DRY RUN: data = [{"id": "bitcoin", "current_price": 77063, "name": "Bitcoin", ...}]
    # CoinGecko returns a LIST even for one coin — data[0] gets first item

    logger.info(f"Fetched {coin_id}: ${data[0]['current_price']}")
    # OUTPUT: INFO - Fetched bitcoin: $77063

    return data[0]['current_price']
    # RETURNS: 77063.0 — stored in XCom for this task instance


# ── FUNCTION 4: save_prices_to_db ───────────────────────────
# Saves ALL 5 coins to PostgreSQL using PostgresHook
# Runs AFTER all 5 fetch tasks complete
# Uses ON CONFLICT DO NOTHING for idempotency
def save_prices_to_db(**context):
    # **context — Airflow passes task context (XCom, run info etc.)
    # Import inside function — avoids top-level import issues
    from api_service.fetchers.crypto import fetch_crypto

    # PostgresHook reads credentials from Airflow UI connection "crypto_postgres"
    # No hardcoded password — host/port/password all in Airflow UI
    hook = PostgresHook(postgres_conn_id='crypto_postgres')
    conn = hook.get_conn()
    # DRY RUN: conn = psycopg2 connection to postgres:5432/data_platform as arish

    cursor = conn.cursor()
    # cursor = tool to run SQL queries on the connection

    prices = fetch_crypto()
    # DRY RUN: prices = [CryptoPrice(bitcoin, 77063.0), CryptoPrice(ethereum, 2121.1), ...]

    for price in prices:
        # LOOP ITERATION 1: price = CryptoPrice(coin_id='bitcoin', coin_name='Bitcoin', ...)
        cursor.execute("""
            INSERT INTO crypto_events
            (coin_id, coin_name, symbol, price_usd, fetched_at)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (coin_id, fetched_at) DO NOTHING
        """, (price.coin_id, price.coin_name, price.symbol, price.price_usd, price.fetched_at))
        # DRY RUN iteration 1:
        # VALUES ('bitcoin', 'Bitcoin', 'BTC', 77063.0, '2026-05-22 10:00:01')
        # ON CONFLICT — checks UNIQUE(coin_id, fetched_at)
        # If 'bitcoin' + '2026-05-22 10:00:01' already exists → skip silently
        # If not exists → insert new row

        # LOOP ITERATION 2: price = CryptoPrice(coin_id='ethereum', ...)
        # VALUES ('ethereum', 'Ethereum', 'ETH', 2121.1, '2026-05-22 10:00:01')
        # ... and so on for solana, dogecoin, ripple

    conn.commit()
    # Without commit — all inserts are temporary, lost when connection closes
    # commit() = permanently save all 5 inserts to disk

    cursor.close()
    # Return cursor to database — prevents memory leak in production
    # Without this: 288 runs/day × unclosed cursors = memory exhausted

    logger.info(f"Saved {len(prices)} coins to PostgreSQL")
    # OUTPUT: INFO - Saved 5 coins to PostgreSQL


# ── FUNCTION 5: log_pipeline_status ─────────────────────────
# Logs every pipeline run to pipeline_logs table
# Pulls row count from XCom (set by fetch_crypto_prices)
def log_pipeline_status(**context):
    rows = context['ti'].xcom_pull(task_ids='fetch_crypto_prices')
    # DRY RUN: xcom_pull looks up XCom for task 'fetch_crypto_prices' in this DAG run
    # rows = 5 (the return value of fetch_crypto_prices)

    import psycopg2
    # Direct psycopg2 here (not Hook) — acceptable for simple logging task
    conn = psycopg2.connect("postgresql://arish:Arish200502@postgres:5432/data_platform")
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO pipeline_logs (pipeline_name, task_name, status, rows_processed)
        VALUES ('ingest_dag', 'fetch_crypto_prices', 'success', %s)
    """, (rows,))
    # DRY RUN: INSERT INTO pipeline_logs VALUES ('ingest_dag', 'fetch_crypto_prices', 'success', 5)
    conn.commit()
    conn.close()
    logger.info("Pipeline log saved to PostgreSQL successfully")
    # OUTPUT: INFO - Pipeline log saved to PostgreSQL successfully


# ── DAG DEFINITION ──────────────────────────────────────────
with DAG(
    dag_id="crypto_ingest_dag",        # unique name shown in Airflow UI
    start_date=datetime(2026, 5, 14),  # DAG won't run before this date
    schedule_interval="0 * * * *",     # cron: run at minute 0 of every hour
    catchup=False,                     # don't run for past missed intervals
    max_active_runs=1,                 # only ONE run at a time — prevents overlap
    default_args={
        # These apply to EVERY task in this DAG automatically
        'retries': 3,                              # retry 3 times before FAILED
        'retry_delay': timedelta(seconds=10),      # wait 10s between retries
        'on_failure_callback': alert_on_failure,   # call this if any task fails
        'sla': timedelta(minutes=10),              # alert if DAG takes > 10 min
        'email': ['arishmahammad8@gmail.com'],
        'email_on_failure': True,                  # send email on failure
        'email_on_retry': False,                   # don't email on retries
    },
    tags=["crypto", "ingest"],  # filter tags in Airflow UI
) as dag:

    # ── TASK 1: HttpSensor ───────────────────────────────────
    # DRY RUN: Airflow calls GET https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&ids=bitcoin
    # Every 30 seconds until HTTP 200 received or 300 seconds timeout
    check_api = HttpSensor(
        task_id='check_coingecko_api',
        http_conn_id='coingecko_api',       # reads URL from Airflow UI connection
        endpoint='coins/markets',
        request_params={
            'vs_currency': 'usd',
            'ids': 'bitcoin'
        },
        poke_interval=30,   # check every 30 seconds
        timeout=300,        # give up after 5 minutes
    ),
    # DRY RUN RESULT: HTTP 200 → sensor passes → pipeline continues
    # If CoinGecko down: sensor keeps poking every 30s → after 300s → FAILED → retry

    # ── TASK 2: FileSensor ───────────────────────────────────
    # DRY RUN: checks if /opt/airflow/dags/data/crypto_input.csv exists
    # Every 30 seconds until file found or 300 seconds timeout
    check_file = FileSensor(
        task_id='check_input_file',
        filepath='/opt/airflow/dags/data/crypto_input.csv',
        poke_interval=30,
        timeout=300,
        mode='poke',   # holds worker slot — checks every 30s continuously
        # mode='reschedule' would release worker between checks (better for production)
    )
    # DRY RUN RESULT: file exists → sensor passes → pipeline continues

    # ── TASKS 3-7: Dynamic PythonOperators (5 coins) ────────
    # Loop creates 5 separate tasks automatically
    coins = ['bitcoin', 'ethereum', 'solana', 'dogecoin', 'ripple']
    fetch_tasks = []
    for coin in coins:
        # DRY RUN iteration 1: coin = "bitcoin"
        t = PythonOperator(
            task_id=f"fetch_{coin}",           # DRY RUN: "fetch_bitcoin"
            python_callable=fetch_one_coin,    # function to call
            op_args=[coin],                    # DRY RUN: op_args=["bitcoin"]
            # Airflow calls: fetch_one_coin("bitcoin")
        )
        fetch_tasks.append(t)
    # After loop: fetch_tasks = [fetch_bitcoin_task, fetch_ethereum_task, ...]
    # All 5 run IN PARALLEL after check_file succeeds

    # ── TASK 8: save_prices_to_db ────────────────────────────
    # Waits for ALL 5 fetch tasks to finish
    # outlets=[crypto_dataset] → after success, analytics_dag triggers
    save_task = PythonOperator(
        task_id="save_prices_to_db",
        python_callable=save_prices_to_db,
        outlets=[crypto_dataset],
        # DRY RUN: after task succeeds →
        # Airflow marks postgres://data_platform/crypto_events as updated →
        # analytics_dag sees update → triggers automatically
    )

    # ── TASK 9: SqlSensor ────────────────────────────────────
    # Verifies data actually landed in the database
    # DRY RUN: runs SELECT 1 FROM crypto_events LIMIT 1
    # If returns row → sensor passes. If empty → keeps poking
    check_db = SqlSensor(
        task_id='check_db_connection',
        conn_id='crypto_postgres',
        sql="SELECT 1 FROM crypto_events LIMIT 1;",
        poke_interval=30,
        timeout=300,
    )
    # DRY RUN RESULT: returns 1 row → sensor passes → pipeline continues

    # ── TASK 10: log_pipeline_status ─────────────────────────
    # Records this pipeline run in pipeline_logs table
    log_status = PythonOperator(
        task_id="log_pipeline_status",
        python_callable=log_pipeline_status,
        provide_context=True,
        # provide_context=True passes Airflow context (XCom, run info) to function
    )

    # ── PIPELINE SEQUENCE ────────────────────────────────────
    # DRY RUN of full flow:
    # 1. check_coingecko_api → GET CoinGecko → HTTP 200 → PASS
    # 2. check_input_file → file exists → PASS
    # 3. [fetch_bitcoin, fetch_ethereum, fetch_solana,
    #     fetch_dogecoin, fetch_ripple] → all run in PARALLEL
    #    → each returns price float into XCom
    # 4. save_prices_to_db → fetches all 5 → inserts into DB
    #    → ON CONFLICT skips duplicates → commits → outlets fires
    #    → analytics_dag auto-triggered
    # 5. check_db_connection → SELECT 1 → row found → PASS
    # 6. log_pipeline_status → pulls XCom rows=5 → inserts pipeline_log
    check_api >> check_file >> fetch_tasks >> save_task >> check_db >> log_status






# AIRFLOW SCHEDULER
#       |
#       | (every hour — schedule_interval="0 * * * *")
#       ↓
# crypto_ingest_dag    ← your DAG
#       |
#       ↓
# check_coingecko_api  ← Task 1 (HttpSensor)
# - GET https://api.coingecko.com/api/v3/coins/markets?ids=bitcoin
# - pokes every 30 seconds
# - HTTP 200 received → PASS
# - CoinGecko down → retry every 30s → timeout 300s → FAILED
#       |
#       ↓
# check_input_file     ← Task 2 (FileSensor)
# - checks /opt/airflow/dags/data/crypto_input.csv exists
# - pokes every 30 seconds
# - file found → PASS
# - file missing → retry every 30s → timeout 300s → FAILED
#       |
#       ↓
# ┌─────────────────────────────────────────┐
# │  fetch_bitcoin   fetch_ethereum         │  ← Tasks 3-7
# │  fetch_solana    fetch_dogecoin         │  (5 PythonOperators)
# │  fetch_ripple                           │  ALL RUN IN PARALLEL
# │                                         │
# │  each calls fetch_one_coin(coin_id)     │
# │  each hits CoinGecko for ONE coin only  │
# │  fetch_bitcoin  → returns 77063.0       │
# │  fetch_ethereum → returns 2121.1        │
# │  fetch_solana   → returns 84.58         │
# │  fetch_dogecoin → returns 0.10          │
# │  fetch_ripple   → returns 1.36          │
# │                                         │
# │  if fetch_bitcoin fails → only bitcoin  │
# │  fails, other 4 still succeed           │
# └─────────────────────────────────────────┘
#       |
#       | (all 5 fetch tasks must finish before next step)
#       ↓
# save_prices_to_db    ← Task 8 (PythonOperator)
# - connects to PostgreSQL via PostgresHook (crypto_postgres)
# - no hardcoded password — reads from Airflow UI connection
# - fetches all 5 coins via fetch_crypto()
# - loops through each coin:
#     INSERT INTO crypto_events (coin_id, coin_name, symbol, price_usd, fetched_at)
#     VALUES ('bitcoin', 'Bitcoin', 'BTC', 77063.0, '2026-05-22 10:00:01')
#     ON CONFLICT (coin_id, fetched_at) DO NOTHING  ← skips duplicates
#     ... repeat for ethereum, solana, dogecoin, ripple
# - conn.commit() → permanently saves all 5 rows
# - outlets=[crypto_dataset] fires →
#     Airflow marks postgres://data_platform/crypto_events as updated →
#     analytics_dag triggers automatically (event-driven)
#       |
#       ↓
# check_db_connection  ← Task 9 (SqlSensor)
# - runs: SELECT 1 FROM crypto_events LIMIT 1
# - row returned → data confirmed in DB → PASS
# - empty result → pokes every 30s → timeout 300s → FAILED
#       |
#       ↓
# log_pipeline_status  ← Task 10 (PythonOperator)
# - pulls rows count from XCom (from fetch_crypto_prices)
#     rows = context['ti'].xcom_pull(task_ids='fetch_crypto_prices')
#     rows = 5
# - writes to pipeline_logs table:
#     INSERT INTO pipeline_logs
#     VALUES ('ingest_dag', 'fetch_crypto_prices', 'success', 5)
#       |
#       ↓
# DONE ✓ — DAG run marked SUCCESS in Airflow UI
#
# ── IF ANY TASK FAILS ──────────────────────────────────────
# → retries=3: retry 3 times (wait 10s between each)
# → after 3 retries still failing → FAILED (red in UI)
# → alert_on_failure() called → logs error message
# → email sent to arishmahammad8@gmail.com
# → sla=10min: if DAG takes longer than 10 min → SLA alert
#
# ── PARALLEL ANALYTICS FLOW (auto-triggered) ───────────────
# save_prices_to_db succeeds
#       |
#       | (outlets=[crypto_dataset] fires)
#       ↓
# analytics_dag triggers automatically
#       |
#       ↓
# calculate_analytics  ← Task 1
# - connects via PostgresHook
# - runs: SELECT coin_id, AVG(price_usd), MAX(price_usd),
#                MIN(price_usd), COUNT(*) FROM crypto_events
#         GROUP BY coin_id ORDER BY avg_price DESC
# - logs results:
#     bitcoin  — Avg: $73,524 | Max: $77,709 | Records: 23
#     ethereum — Avg: $2,079  | Max: $2,133  | Records: 23
#     ...
#       |
#       ↓
# DONE ✓
