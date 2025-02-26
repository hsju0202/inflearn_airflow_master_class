from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.operators.email import EmailOperator

import os

dag_id = os.path.basename(__file__)


# docker-compose.yaml 편집 필요 (environment 항목에 추가)

# AIRFLOW_SMTP_SMTP_HOST:'smtp.gmail.com'
# AIRFLOW_SMTP_SMTP_USER:'{gmail 계정}'
# AIRFLOW_SMTP_SMTP_PASSWORD:'{앱 비밀번호}'
# AIRFLOW_SMTP_SMTP_PORT:587
# AIRFLOW_SMTP_SMTP_MAIL_FROM:'{gmail 계정}'

with DAG(
    dag_id=dag_id,
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2021, 1, 1, tz="Asia/Seoul"),
    catchup=False,
) as dag:
    
    send_email_task = EmailOperator(
        task_id='send_email_task',
        to='sukju.hong@credos.one',
        subject='Airflow 메일',
        html_content='Airflow 작업이 완료 되었습니다.'
    )

if __name__ == "__main__":
    dag.test()
