# airflow/dags/crypto_etl_dag.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime, timedelta
import os

# Import your extraction function
from airflow.scripts.extract_coingecko_data import extract_and_load_coingecko_data

# Define dbt project and profile paths
DBT_PROJECT_DIR = os.path.join(os.environ.get('AIRFLOW_HOME', '~/airflow_crypto_project'), 'dbt', 'crypto_analytics')
DBT_PROFILE = 'crypto_data_project' # Must match the profile name in ~/.dbt/profiles.yml

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='crypto_data_pipeline',
    default_args=default_args,
    description='A DAG to extract crypto data, load to Snowflake, and transform with dbt',
    schedule_interval=timedelta(days=1), # Run daily
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['crypto', 'dbt', 'snowflake'],
) as dag:

    create_raw_table = SnowflakeOperator(
        task_id='create_raw_coingecko_table',
        snowflake_conn_id='snowflake_default',
        sql="""
            CREATE TABLE IF NOT EXISTS CRYPTO_DB.RAW.COINGECKO_HISTORICAL_PRICES (
                coin_id VARCHAR,
                date DATE,
                price FLOAT,
                market_cap FLOAT,
                total_volume FLOAT,
                extraction_timestamp TIMESTAMP_LTZ
            );
        """,
    )

    extract_and_load = PythonOperator(
        task_id='extract_and_load_coingecko_data',
        python_callable=extract_and_load_coingecko_data,
        op_kwargs={
            'coin_ids': ['bitcoin', 'ethereum', 'solana', 'binancecoin', 'dogecoin', 'cardano', 'ripple', 'polkadot', 'litecoin'],
            'vs_currency': 'usd',
        },
    )

    # dbt commands
    # You might want to run dbt seed if you have static data to load
    dbt_deps = BashOperator(
        task_id='dbt_deps',
        bash_command=f'dbt deps --project-dir {DBT_PROJECT_DIR} --profile {DBT_PROFILE}',
        cwd=DBT_PROJECT_DIR # Set current working directory for dbt
    )

    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command=f'dbt run --project-dir {DBT_PROJECT_DIR} --profile {DBT_PROFILE}',
        cwd=DBT_PROJECT_DIR
    )

    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command=f'dbt test --project-dir {DBT_PROJECT_DIR} --profile {DBT_PROFILE}',
        cwd=DBT_PROJECT_DIR
    )

    create_raw_table >> extract_and_load >> dbt_deps >> dbt_run >> dbt_test