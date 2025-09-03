from airflow import DAG,Dataset
from airflow.decorators import task

from datetime import datetime,date
my_file=Dataset('/tmp/myfile.txt')

with DAG(
    dag_id='Producer',
    start_date=datetime(2023, 1, 1),
    schedule="@daily",
    catchup=False
):
    @task(outlets=[my_file])
    def update_dataset():
        with open(my_file.uri,'a+') as f:
            f.write('producer update')

    update_dataset() 