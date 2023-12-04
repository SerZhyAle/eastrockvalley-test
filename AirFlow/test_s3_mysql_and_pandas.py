from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'description': 'read CSV',
    'start_date': datetime(2023, 12, 1),
    'email': ['serzhyale@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
}

def read_csv(key, bucket_name):
    hook = S3Hook('s3_connection')
    file_content = hook.read_key(
        key=key, bucket_name=bucket_name
    )
    df = pd.read_csv(file_content)
    # Perform operations on DataFrame
    return df

dag = DAG('pandas_csv_dag',
          default_args=default_args,
          schedule='@daily'
          )

read_csv_task = PythonOperator(
    task_id='read_csv',
    python_callable=read_csv,
    op_kwargs={'key': 'games.csv',
               'bucket_name': 'eastrockvalley-test'
               },
    dag=dag
)