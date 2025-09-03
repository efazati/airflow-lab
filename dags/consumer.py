from airflow import DAG,Dataset
from airflow.decorators import task

from datetime import datetime,date
my_file=Dataset('/tmp/myfile.txt')

with DAG(
    dag_id='consumer',
    start_date=datetime(2023, 1, 1),
    schedule=[my_file],
    catchup=False
):
    @task
    def read_dataset():
        with open(my_file.uri,'r') as f:
            print(f.read())

    read_dataset() 