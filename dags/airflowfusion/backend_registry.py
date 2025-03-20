import time
from airflow.operators.python import get_current_context
import os

LOG_FOLDER = "/usr/local/airflow/dags"
LOG_FILE = "log.txt"

def read(backend_name, key, *args, **kwargs):
    print(f"Reading from backend: {backend_name}")

    start_time = time.time()
    result = None
    if backend_name == "xcom":
        result = xcom_read(key, *args, **kwargs)

    duration = time.time() - start_time
    print(f"Read from {backend_name} with key {key} in {duration} seconds")
    context = get_current_context()  # Retrieve the execution context
    task_id = context['task'].task_id  # Get the current task's ID
    dag_id = context['dag'].dag_id
    
    with open(LOG_FOLDER  + '/' + dag_id + '/' + LOG_FILE, "a") as f:
        f.write(f"read {task_id} {backend_name} <{key}> {duration}\n")
    
    if result == None:
        raise ValueError(f"Unsupported backend: {backend_name}")
    return result
    


def write(backend_name, key, value, *args, **kwargs):
    start_time = time.time()
    if backend_name == "xcom":
        return xcom_write(key, value, *args, **kwargs)
    duration = time.time() - start_time
    print(f"Write to {backend_name} with key {key} in {duration} seconds")
    context = get_current_context()  # Retrieve the execution context
    task_id = context['task'].task_id  # Get the current task's ID
    dag_id = context['dag'].dag_id
    with open(LOG_FOLDER  + '/' + dag_id + '/' + LOG_FILE, "a") as f:
        f.write(f"write {task_id} {backend_name} <{key}> {duration}\n")
    
    raise ValueError(f"Unsupported backend: {backend_name}")


# CUSTOM BACKENDS HANDLER

# XCOM
def xcom_read(key):
    context = get_current_context()
    task_instance = context['ti']
    return task_instance.xcom_pull(key=key)

def xcom_write(key, value):
    context = get_current_context()
    task_instance = context['ti']
    task_instance.xcom_push(key=key, value=value)