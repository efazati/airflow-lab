"""
XCom DAG converted to use DAG Factory YAML configuration.
Demonstrates data passing between tasks via XComs.
"""

from dagfactory import DagFactory
import os

# Path to the YAML configuration file
config_file = os.path.join(os.path.dirname(__file__), "xcom_dag.yaml")

# Create DAG Factory instance and generate DAGs
dag_factory = DagFactory(config_file)

# Generate DAGs from the configuration
dag_factory.clean_dags(globals())
dag_factory.generate_dags(globals())
