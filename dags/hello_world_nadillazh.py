from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

def print_hello():
    return 'Hello world!'

dag = DAG('hello_world', description='Simple DAG to print Hello World',
          schedule_interval='@daily',
          start_date=datetime(2024, 1, 1),
          catchup=False,
          tags=['example', 'tag'] )

task_hello = PythonOperator(
    task_id='print_hello',
    python_callable=print_hello,
    dag=dag
)