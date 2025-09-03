"""
Basic Workflow DAG - DAG Factory Approach
Demonstrates mixed Python and Bash operators with XCom data passing
"""

from dagfactory import load_yaml_dags
import os

# Get the directory containing this file
dags_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else '/opt/airflow/dags/basic_dag_factory'

# Load the basic DAG factory configuration
basic_dag_path = os.path.join(dags_dir, 'basic_dag_factory.yaml')
if os.path.exists(basic_dag_path):
    load_yaml_dags(
        globals_dict=globals(),
        dags_folder=dags_dir,
        config_filepath=basic_dag_path
    )
    print("✅ Loaded basic DAG factory workflow")
else:
    print("❌ basic_dag_factory.yaml not found")
