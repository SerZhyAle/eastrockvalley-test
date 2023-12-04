"""
EastRockValle test
"""
# airflow related
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
# other packages
from datetime import datetime
from datetime import timedelta
from airflow.hooks import S3_hook
from airflow.hooks import mysql_hook
from io import StringIO
# import pandas as pd
import boto3                                     # AWS
from sqlalchemy import create_engine             # MySQL connection

from datetime import datetime, timedelta
import codecs
import os
import logging
import boto3
import pandas as pd
import sys
from pandas.io import sql
import MySQLdb

#Airflow Read File from S3
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

schedule = '30 * * * *'

# Airflow Read File from S3

def download_from_s3(key: str, bucket_name: str, local_path: str) -> str:
    hook = S3Hook('s3_connection')
    file_content = hook.read_key(
        key=key, bucket_name=bucket_name
    )

    body = file_content['Body']
    csv_string = body.read().decode('utf-8')
"""
    col_names = ["GameID",
                 "UserID",
                 "GameType",
                 "PlayDate",
                 "Duration"]

    df = pd.read_csv(StringIO(csv_string), names=col_names, header=0)

    df.info()
"""


def insert_to_mysql():
    # mysql connection
    conn_str_app = MySqlHook('eastrockvalley-mysql-8').get_uri()
    engine_app = create_engine(conn_str_app, echo=False)

    df.to_sql(con=con, name='table_name_for_df', if_exists='replace', flavor='mysql')
    qry = """
      insert into sandbox.test_table(id, name)
        values(1, 'test');
      """
    df = pd.sql_query(qry, engine_app)
    # already_processed = df['filename'].tolist()
    return 1
    #  already_processed

this_dag = DAG(
    dag_id='s3-test',
    default_args=default_args,
    schedule=schedule,
)

task_download_from_s3 = PythonOperator(
        task_id='task_download_from_s3',
        python_callable=download_from_s3,
        dag=this_dag,
        op_kwargs={
            'key': 'games.csv',
            'bucket_name': 'eastrockvalley-test'
        }
)

task_insert_to_mysql = PythonOperator(
        task_id='task_insert_to_mysql',
        python_callable=insert_to_mysql,
        dag=this_dag
)

task_download_from_s3 >> task_insert_to_mysql

"""

# Airflow Read File from S3

#Airflow Read File from S3
def source1_to_s3():
 # code that writes our data from source 1 to s3
def source2_to_hdfs(config, ds, **kwargs):
 # code that writes our data from source 2 to hdfs
 # ds: the date of run of the given task.
 # kwargs: keyword arguments containing context parameters for the run.
def source3_to_s3():
 # code that writes our data from source 3 to s3

def download_file_from_s3(remote_path, local_path, prefix, suffix,conn_id, ext, **kwargs):
    conn = S3_Hook(aws_conn_id='aws-s3')
    fl = 'games.csv'
    remote_filepath = remote_path + fl
    local_filepath = local_path + fl
    conn.retrieve_file(remote_filepath, local_filepath)
    return local_filepath

def bulk_load_sql(table_name, **kwargs):
    local_filepath = kwargs['ti'].xcom_pull(task_ids='download_file')
    conn = MySqlHook(conn_name_attr='ib_sql')
    conn.bulk_load(table_name, local_filepath)
    return table_name

dag = DAG('carga-mails-ejecutivos-6', schedule_interval="@daily")
t_read = PythonOperator(
        task_id='process_file',
        python_callable=download_file_from_s3,
        provide_context=True,
        dag=dag)
t_write = PythonOperator(
        task_id='file_to_MySql',
        provide_context=True,
        python_callable=bulk_load_sql,
        dag=dag)
t_read >> t_write

"""