"""
Sample DAG using traditional Python approach (old-fashioned style).
This demonstrates the classic way of creating Airflow DAGs before DAG Factory.
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

def print_hello():
    """Simple Python function for demonstration."""
    print("Hello from traditional Airflow DAG!")
    return "Hello World"

def print_date():
    """Print current date."""
    print(f"Current date: {datetime.now()}")

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create the DAG
dag = DAG(
    'sample_dag',
    default_args=default_args,
    description='A simple sample DAG using traditional Python approach',
    schedule_interval='@daily',
    catchup=False,
    tags=['sample', 'traditional', 'python'],
)

# Define tasks
start_task = BashOperator(
    task_id='start_task',
    bash_command='echo "Starting sample DAG execution"',
    dag=dag,
)

hello_task = PythonOperator(
    task_id='hello_task',
    python_callable=print_hello,
    dag=dag,
)

date_task = PythonOperator(
    task_id='date_task',
    python_callable=print_date,
    dag=dag,
)

end_task = BashOperator(
    task_id='end_task',
    bash_command='echo "Sample DAG execution completed"',
    dag=dag,
)

# Set task dependencies
start_task >> [hello_task, date_task] >> end_task
