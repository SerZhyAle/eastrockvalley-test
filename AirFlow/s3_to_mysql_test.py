"""
EastRockValle test TEST
"""

import airflow
from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.amazon.aws.transfers.s3_to_sql import S3ToSqlOperator

from airflow import DAG
from datetime import datetime, timedelta
import os
from airflow.models.connection import Connection

default_args = {
    'start_date': datetime(2023, 12, 3),
    'owner': 'airflow',
    'depends_on_past': False,
    'description': 'read CSV insert to MySQL',
    'email': ['serzhyale@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}
schedule = '1 * * * *'


def parse_csv_to_list(filepath):
    import csv

    with open(filepath, newline="") as file:
        return list(csv.reader(file))


dag = DAG(dag_id='Test_s3_to_mysql',
          default_args=default_args,
          schedule=schedule,
          description='TEST: read S3 CSV insert into RDS MySQL'
          )

s3_to_mysql_test = S3ToSqlOperator(
    task_id='s3_to_mysql_test',
    schema='staging',
    table='test',
    s3_bucket='eastrockvalley-test',
    s3_key='test.csv',
    sql_conn_id='eastrockvalley-mysql-8',
    aws_conn_id='s3_conn',
    column_list=["id", "name"],
    commit_every=0,
    parser=parse_csv_to_list,
    dag=dag
)

s3_to_mysql_test
