"""
Python functions for the basic DAG factory workflow
"""

def process_data(**context):
    """Simple Python function for data processing"""
    print("Processing data in Python...")
    import time
    time.sleep(1)
    return {"status": "success", "processed_items": 42}

def send_email(**context):
    """Simulate sending an email notification"""
    task_instance = context['task_instance']
    result = task_instance.xcom_pull(task_ids='python_task')
    print(f"Sending email: Processed {result['processed_items']} items successfully!")
    return "Email sent"
