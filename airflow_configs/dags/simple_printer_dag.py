# from airflow import DAG
# # from airflow.providers.standard.operators.python import PythonOperator
# from airflow.providers.standard.operators.python import PythonOperator
# from datetime import datetime, timedelta

# # Define the Python function you want to run as a task
# def print_hello():
#     """
#     This function simply prints a message to the Airflow logs.
#     """
#     print("Hello, Airflow! This message will appear in the task logs.")

# # Define default arguments for the DAG
# default_args = {
#     'owner': 'airflow',
#     'depends_on_past': False,
#     'email_on_failure': False,
#     'email_on_retry': False,
#     'retries': 1,
#     'retry_delay': timedelta(minutes=5),
# }

# # Instantiate the DAG object
# with DAG(
#     dag_id='simple_printer_dag',
#     default_args=default_args,
#     description='A simple DAG to run a Python print statement',
#     # schedule_interval=None,  # Set to None for manual triggers
#     start_date=datetime(2023, 1, 1),
#     catchup=False,
#     tags=['example', 'print'],
# ) as dag:
#     # Define the task using PythonOperator
#     # It calls the 'print_hello' function
#     run_print_hello_task = PythonOperator(
#         task_id='print_hello_task',
#         python_callable=print_hello,
#     )

from airflow import DAG
# from airflow.operators.python import PythonOperator
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime

def my_print_function():
    print("Printing from the classic PythonOperator.")

with DAG(dag_id="classic_print_dag", start_date=datetime(2026, 1, 1), schedule=None) as dag:
    task1 = PythonOperator(
        task_id="print_task",
        python_callable=my_print_function
    )