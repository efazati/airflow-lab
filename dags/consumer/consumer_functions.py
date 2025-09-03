"""
Custom functions for consumer DAG.
"""

from airflow import Dataset

# Define the dataset (same as producer)
my_file = Dataset('/tmp/myfile.txt')

def read_dataset():
    """Read the dataset file."""
    with open(my_file.uri, 'r') as f:
        print(f.read())
