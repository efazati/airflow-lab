"""
Streaming Platforms Data Pipeline - DAG Factory Approach
Uses dlt to load streaming platform datasets into DuckDB with unified schema
"""

from dagfactory import load_yaml_dags
import os

# Get the directory containing this file
dags_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else '/opt/airflow/dags/streaming_platforms_dlt'

# Load the streaming platforms DAG configuration
streaming_dag_path = os.path.join(dags_dir, 'streaming_platforms_dlt.yaml')
if os.path.exists(streaming_dag_path):
    load_yaml_dags(
        globals_dict=globals(),
        dags_folder=dags_dir,
        config_filepath=streaming_dag_path
    )
    print("✅ Loaded streaming platforms dlt DAG")
else:
    print("❌ streaming_platforms_dlt.yaml not found")
