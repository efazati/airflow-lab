"""
DAG Factory Loader for Airflow 3.0
This file properly loads YAML DAG configurations using DAG Factory
"""

from dagfactory import load_yaml_dags
import os

# Get the directory containing this file
dags_dir = os.path.dirname(__file__)

# List of DAG subdirectories with YAML files
dag_folders = [
    'parallel_dag',
    'xcom_dag',
    'producer',
    'consumer',
    # 'group_dag',      # Will enable after fixing TaskGroup syntax
    # 'user_process',   # Will enable after adding postgres connections
    # 'elastic_dag'     # Elasticsearch disabled to save resources
]

# Load all YAML DAGs using the correct DAG Factory approach for Airflow 3.0
for folder in dag_folders:
    try:
        folder_path = os.path.join(dags_dir, folder)
        if os.path.exists(folder_path):
            # DAG Factory automatically discovers YAML files in the folder
            load_yaml_dags(
                globals_dict=globals(),
                dags_folder=folder_path
            )
            print(f"✅ Loaded DAGs from {folder}")
    except Exception as e:
        print(f"❌ Error loading DAGs from {folder}: {e}")

print(f"🎯 DAG Factory loaded {len([k for k in globals().keys() if not k.startswith('_')])} DAG objects")
