from __future__ import annotations

import datetime

import pendulum

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

import os

dag_id = os.path.basename(__file__)

with DAG(
    dag_id=dag_id,
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2021, 1, 1, tz="Asia/Seoul"),
    catchup=False,
) as dag:
    
    host_name = BashOperator(
        task_id="hots_name",
        bash_command="echo $HOSTNAME"
    )
    
    say_hello = BashOperator(
        task_id="say_hello",
        bash_command="/opt/airflow/plugins/shell/select_fruit.sh hello"
    )

    host_name >> say_hello

if __name__ == "__main__":
    dag.test()
