"""
Custom functions for elastic DAG.
"""

from hooks.elastic.elastic_hook import ElasticHook

def print_es_info():
    """Print Elasticsearch information using custom hook."""
    hook = ElasticHook()
    print(hook.info())
