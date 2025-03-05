from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.models import Variable

import os

dag_id = os.path.basename(__file__)

with DAG(
    dag_id=dag_id,
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2021, 1, 1, tz="Asia/Seoul"),
    catchup=False,
) as dag:
    var_value = Variable.get("sample_key")

    var1 = BashOperator(
        task_id="var1",
        bash_command=f"echo Variable:{var_value}"
    )
    
    var2 = BashOperator(
        task_id="var2",
        bash_command="echo Variable:{{var.value.sample_key}}"
    )

    var1
    var2

if __name__ == "__main__":
    dag.test()
