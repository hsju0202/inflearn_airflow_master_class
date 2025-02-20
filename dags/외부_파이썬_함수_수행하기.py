import pendulum

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator

import os

from common.common_func import get_sftp

dag_id = os.path.basename(__file__)

with DAG(
    dag_id=dag_id,
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2021, 1, 1, tz="Asia/Seoul"),
    catchup=False,
) as dag:
    
    call_common_func = PythonOperator(
        task_id="call_common_func",
        python_callable=get_sftp
    )

    call_common_func

if __name__ == "__main__":
    
    dag.test()
