from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator

from api_to_csv import api_data_to_csv

default_args = {
    'owner': 'Aditya Gupta',
    'start_date': datetime(2022, 3, 14),
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

dag = DAG('Airflow_assignment',
          default_args=default_args,
          schedule_interval='0 6 * * *',
          template_searchpath=['/usr/local/airflow/sql_files'],
          catchup=False)

t1 = PythonOperator(task_id='store_data_in_csv', python_callable=api_data_to_csv, dag=dag)

t2 = PostgresOperator(task_id='create_postgres_table', postgres_conn_id='postgres_conn', sql="create_table.sql", dag=dag)

t3 = PostgresOperator(task_id='insert_into_table', postgres_conn_id='postgres_conn', sql="insert_into_table.sql", dag=dag)

t1 >> t2 >> t3
