"""
Dataset ETL Pipeline DAG - Loaded via DAG Factory
Complex multi-stage data processing pipeline demonstrating ETL best practices
"""

from dagfactory import load_yaml_dags
import os

# Get the directory containing this file
dags_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else '/opt/airflow/dags'

# Load the dataset ETL pipeline DAG
dataset_dag_path = os.path.join(dags_dir, 'dataset_etl_pipeline.yaml')
if os.path.exists(dataset_dag_path):
    load_yaml_dags(
        globals_dict=globals(),
        dags_folder=dags_dir,
        config_filepath=dataset_dag_path
    )
    print("✅ Loaded dataset ETL pipeline DAG")
else:
    print("❌ dataset_etl_pipeline.yaml not found")
