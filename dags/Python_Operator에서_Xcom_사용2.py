from __future__ import annotations

import datetime

import pendulum

from airflow.models.dag import DAG
from airflow.operators.python import task

import os

dag_id = os.path.basename(__file__)

with DAG(
    dag_id=dag_id,
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2021, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
    tags=["example", "example2"],
    params={"example_key": "example_value"},
) as dag:
    
    @task(task_id='python_xcom_push_by_return')
    def xcom_push_result(**kwargs):
        return 'Success'

    @task(task_id='python_xcom_pull_1')
    def xcom_pull_1(**kwargs):
        ti = kwargs['ti']
        value1 = ti.xcom_pull(task_ids='python_xcom_push_by_return')
        print(value1)

    @task(task_id='python_xcom_pull_2')
    def xcom_pull_2(status, **kwargs):
        print(status)

    result = xcom_push_result()
    xcom_pull_2(result)
    result >> xcom_pull_1()

if __name__ == "__main__":
    dag.test()
