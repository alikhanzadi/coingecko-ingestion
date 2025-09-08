# Basic DAG every 10 minutes:
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from scripts.fetch_coingecko_prices import fetch_prices

default_args = {
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

with DAG('fetch_crypto_prices',
         schedule_interval='*/10 * * * *',
         default_args=default_args,
         catchup=False) as dag:

    t1 = PythonOperator(
        task_id='fetch_prices',
        python_callable=fetch_prices
    )

# 2nd one
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pathlib import Path
import sys

# Make sure Airflow finds the ingestion script
sys.path.append(str(Path(__file__).resolve().parents[2] / "scripts"))
from fetch_coingecko_prices import fetch_prices_to_snowflake

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id='fetch_crypto_prices',
    default_args=default_args,
    schedule_interval='@hourly',  # Or your preferred frequency
    catchup=False
) as dag:

    t1 = PythonOperator(
        task_id='ingest_prices',
        python_callable=fetch_prices_to_snowflake
    )
