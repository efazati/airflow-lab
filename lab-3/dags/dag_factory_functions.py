"""
Python functions used by dag-factory YAML configurations
"""


def process_data_func(**context):
    """
    Example Python function called from dag-factory YAML
    """
    print("Processing data in Python function...")
    print(f"Execution date: {context.get('ds')}")
    print(f"Task instance: {context.get('task_instance')}")
    return "Data processed successfully"

