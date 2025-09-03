from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create the DAG
dag = DAG(
    'hello_world_simple',
    default_args=default_args,
    description='A simple Hello World DAG',
    schedule='@daily',
    catchup=False,
    tags=['simple', 'hello-world'],
)

# Define tasks
start_task = BashOperator(
    task_id='start',
    bash_command='echo "Hello World! Starting the workflow..."',
    dag=dag,
)

process_task = BashOperator(
    task_id='process_data',
    bash_command='echo "Processing data..." && sleep 2 && echo "Data processed successfully!"',
    dag=dag,
)

notify_task = BashOperator(
    task_id='send_notification',
    bash_command='echo "Sending notification..." && echo "Workflow completed successfully!"',
    dag=dag,
)

# Set dependencies
start_task >> process_task >> notify_task
