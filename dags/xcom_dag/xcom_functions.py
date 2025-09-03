"""
Custom functions for XCom DAG example.
"""

def t1_function(ti):
    """Push value to XCom."""
    ti.xcom_push(key="my_key", value=12)

def t2_function(ti):
    """Pull and print value from XCom."""
    print(ti.xcom_pull(key="my_key", task_ids="t1"))

def branch_function(ti):
    """Branch based on XCom value."""
    value = ti.xcom_pull(key="my_key", task_ids='t1')
    if (value == 12):
        return 't2'
    return 't3'
