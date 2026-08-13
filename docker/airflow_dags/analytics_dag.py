# query the database and calculate simple analytics after every ingest run.
# ingest_dag → save_prices_to_db finishes
#            → outlets=[crypto_dataset] fires
#            → Airflow sees crypto_dataset updated
#            → analytics_dag triggers automatically
#            → calculate_analytics() runs
#            → logs avg/max/min per coin

# Step 1 — Imports + Dataset definition Done
# Define the same crypto_dataset URI so Airflow knows which dataset to listen for.
# Step 2 — Write analytics function ← We are here
# calculate_analytics() — connects to PostgreSQL via Hook, runs SQL to calculate average, max, min price per coin, logs results.
# Step 3 — Define the DAG
# Create a DAG with schedule=[crypto_dataset] instead of a cron expression. This tells Airflow — "don't run on a timer, run when crypto_dataset is updated."


from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.datasets import Dataset
from datetime import datetime
import logging
import sys
sys.path.insert(0, '/opt/airflow/dags')

logger = logging.getLogger(__name__)



# step 1 — Imports + Dataset definition
crypto_dataset = Dataset("postgres://data_platform/crypto_events")



# step 2 — Write analytics function 
def calculate_analytics():
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    hook = PostgresHook(postgres_conn_id='crypto_postgres')
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT coin_id,
               AVG(price_usd) as avg_price,
               MAX(price_usd) as max_price,
               MIN(price_usd) as min_price,
               COUNT(*) as total_records
        FROM crypto_events
        GROUP BY coin_id
        ORDER BY avg_price DESC
    """)
    results = cursor.fetchall()
    for row in results:
        logger.info(f"{row[0]} — Avg: ${row[1]:,.2f} | Max: ${row[2]:,.2f} | Records: {row[4]}")
    cursor.close()


# step 3 — Define the DAG 
with DAG(
    dag_id="analytics_dag",
    schedule=[crypto_dataset],       # Instead of cron like 0 * * * *, this DAG listens for dataset updates — runs when crypto_dataset is marked updated in Airflow. 
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["analytics", "crypto"],    # 
) as dag:

    run_analytics = PythonOperator(
        task_id="calculate_analytics",
        python_callable=calculate_analytics,
    )