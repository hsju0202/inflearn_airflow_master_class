import pendulum

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

import os

dag_id = os.path.basename(__file__)

with DAG(
    dag_id=dag_id,
    schedule="10 0 L * *", # 매월 말일
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False,
) as dag:
    
    # START_DATE: 전월 말일
    # END_DATE: 1일 전
    macros_bash = BashOperator(
        task_id="call_common_func",
        env={
            'START_DATE': '{{ data_interval_start.in_timezone("Asia/Seoul") | ds }}',
            'END_DATE': '{{ (data_interval_end.in_timezone("Asia/Seoul") - macros.dateutil.relativedelta.relativedelta(days=1)) | ds }}',
        },
        bash_command='echo "START_DATE: $START_DATE" && echo "END_DATE: $END_DATE"'
    )

    macros_bash

if __name__ == "__main__":
    dag.test()
