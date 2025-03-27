from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import task

import os

dag_id = os.path.basename(__file__)

with DAG(
    dag_id=dag_id,
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2021, 1, 1, tz="Asia/Seoul"),
    catchup=False,
) as dag:
    
    @task.branch(task_id="select_random")
    def select_random():
        import random

        task_list = ['A','B C']
        selected_task = random.choice(task_list)

        if selected_task == 'A':
            return 'task_a'
        elif selected_task == 'B C':
            return ['task_b', 'task_c']
        
    def common_fn(**kwargs):
        print(kwargs['selected'])


    task_a = PythonOperator(
        task_id="task_a",
        python_callable=common_fn,
        op_kwargs={'selected': 'A'}
    )

    task_b = PythonOperator(
        task_id="task_b",
        python_callable=common_fn,
        op_kwargs={'selected': 'B'}
    )

    task_c = PythonOperator(
        task_id="task_c",
        python_callable=common_fn,
        op_kwargs={'selected': 'C'}
    )
    
    select_random() >> [task_a, task_b, task_c]

if __name__ == "__main__":
    dag.test()
