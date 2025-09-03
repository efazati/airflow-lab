"""
Parallel DAG converted to use DAG Factory YAML configuration.
A simple DAG that runs tasks in parallel.
"""

from dagfactory import DagFactory
import os

# Path to the YAML configuration file
config_file = os.path.join(os.path.dirname(__file__), "parallel_dag.yaml")

# Create DAG Factory instance and generate DAGs
dag_factory = DagFactory(config_file)

# Generate DAGs from the configuration
dag_factory.clean_dags(globals())
dag_factory.generate_dags(globals())
