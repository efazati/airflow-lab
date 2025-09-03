"""
Custom functions for producer DAG.
"""

from airflow import Dataset

# Define the dataset
my_file = Dataset('/tmp/myfile.txt')

def update_dataset():
    """Update the dataset file."""
    with open(my_file.uri, 'a+') as f:
        f.write('producer update')
