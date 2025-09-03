"""
Consumer DAG converted to use DAG Factory YAML configuration.
Processes data when a dataset change is detected.
"""

from dagfactory import DagFactory
import os

# Path to the YAML configuration file
config_file = os.path.join(os.path.dirname(__file__), "consumer.yaml")

# Create DAG Factory instance and generate DAGs
dag_factory = DagFactory(config_file)

# Generate DAGs from the configuration
dag_factory.clean_dags(globals())
dag_factory.generate_dags(globals())
