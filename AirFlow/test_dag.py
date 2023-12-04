import datetime

from collections.abc import MutableMapping
import collections.abc
collections.MutableMapping = collections.abc.MutableMapping
from airflow import DAG
from airflow.operators.empty import EmptyOperator

test_dag = DAG(
    dag_id="erv-test",
    start_date=datetime.datetime(2023, 12, 1),
    schedule="@daily",
)
EmptyOperator(task_id="task", dag=test_dag)