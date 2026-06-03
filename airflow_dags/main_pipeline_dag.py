# Purpose of main_pipeline_dag.py - This file does the same job as ingest_dag.py but written in the modern TaskFlow API style.
# ingest_dag.py = old style car (manual gear, more steps)
# main_pipeline_dag.py = new style car (automatic, less steps, same destination)

# Both files schedule the same crypto pipeline — but main_pipeline_dag.py is cleaner, less code


# Step 1 — fetch_prices()
#          Fetch live crypto prices from CoinGecko
#          Returns list of coin data

# Step 2 — log_status(row_count)
#          Receives the count from Step 1 via XCom (automatically)
#          Writes pipeline log to PostgreSQL

# Dependencies:
#          fetch_prices() >> log_status()



from airflow.decorators import dag, task
from datetime import datetime
import logging
import sys
sys.path.insert(0, '/opt/airflow/dags')

logger = logging.getLogger(__name__)



# Airflow sees @dag decorator
@dag(
    dag_id="main_pipeline_dag",
    start_date=datetime(2026, 5, 15),
    schedule_interval="@hourly",
    catchup=False,    # This DAG should have been running since May 15. Did I miss any runs?  Airflow says:"Ok, start fresh from now. Ignore the past."
    tags=["crypto", "pipeline"]
)
def main_pipeline():
    @task
    # Step 1 — fetch_prices()
    def fetch_prices():
        from api_service.fetchers.crypto import fetch_crypto
        prices = fetch_crypto()
        logger.info(f"Fetched {len(prices)} coins")
        return len(prices)

    @task
    # Step 2 — log_status(row_count)
    def log_status(row_count):
        import psycopg2
        conn = psycopg2.connect("postgresql://arish:Arish200502@postgres:5432/data_platform")
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO pipeline_logs (pipeline_name, task_name, status, rows_processed)
            VALUES ('main_pipeline', 'fetch_prices', 'success', %s)
        """, (row_count,))        
        conn.commit()
        conn.close()
        logger.info(f"Pipeline log saved — {row_count} rows processed")

    # count = 5 (return value of fetch_prices)
    # Airflow also stores 5 in XCom automatically
    count = fetch_prices()
    
    # Airflow sees count being passed
    # Automatically sets dependency: fetch_prices >> log_status
    # Passes count=5 to log_status via XCom
    log_status(count)


# Registers the DAG with Airflow
# Without this line, Airflow cannot see the DAG
main_pipeline()







# AIRFLOW SCHEDULER
#       |
#       | (every hour automatically)
#       ↓
#   main_pipeline()   ← this is your DAG
#       |
#       ↓
#   fetch_prices()    ← Task 1
#   - calls CoinGecko API
#   - gets 5 coins
#   - returns 5
#       |
#       | (passes 5 via XCom automatically)
#       ↓
#   log_status(5)     ← Task 2
#   - receives 5
#   - writes to PostgreSQL:
#     "main_pipeline ran, 5 rows processed, success"
#       |
#       ↓
#   DONE 

