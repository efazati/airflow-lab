from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'echo_time',
    default_args=default_args,
    description='Echo current time every hour',
    schedule_interval='0 * * * *',  # Every hour
    catchup=False,
    tags=['example', 'time'],
) as dag:

    echo_task = BashOperator(
        task_id='echo_current_time',
        bash_command='date "+%Y-%m-%d %H:%M:%S" && echo "Current time: $(date)"',
    )

    echo_task



