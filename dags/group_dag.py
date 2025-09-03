from airflow import DAG
from airflow.operators.bash import BashOperator
from group.group_downloads import downloads_tasks
from group.group_transforms import transforms_tasks
from datetime import datetime
 
with DAG('group_dag', start_date=datetime(2022, 1, 1), 
    schedule_interval='@daily', catchup=False) as dag:

    args={'start_date':dag.start_date,'schedule_interval':dag.schedule_interval,'catchup':dag.catchup}
    
    downloads =downloads_tasks()
 
    check_files = BashOperator(
        task_id='check_files',
        bash_command='sleep 10'
    )

    transforms =transforms_tasks()


    downloads >> check_files >> transforms