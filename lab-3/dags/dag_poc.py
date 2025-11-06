"""
DAG Factory Loader
Loads DAGs from YAML configuration files using dag-factory
Documentation: https://github.com/astronomer/dag-factory
"""
from pathlib import Path
from dagfactory import load_yaml_dags

# Get the directory containing this file
dags_dir = Path(__file__).parent

# Load all YAML files in the dags directory
yaml_file = dags_dir / "dag_poc.yml"

# Generate DAGs from YAML configuration
# This creates DAG objects that Airflow will automatically detect
load_yaml_dags(str(yaml_file), globals())

