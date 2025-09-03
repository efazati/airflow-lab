"""
Parallel Processing DAG - Loaded via DAG Factory
"""

from dagfactory import load_yaml_dags
import os

# Get the directory containing this file
dags_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else '/opt/airflow/dags'

# Load the parallel processing DAG
parallel_dag_path = os.path.join(dags_dir, 'parallel_processing.yaml')
if os.path.exists(parallel_dag_path):
    load_yaml_dags(
        globals_dict=globals(),
        dags_folder=dags_dir,
        config_filepath=parallel_dag_path
    )
    print("✅ Loaded parallel processing DAG")
else:
    print("❌ parallel.yaml not found")
