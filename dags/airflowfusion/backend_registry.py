import time
from airflow.operators.python import get_current_context
import os
from pathlib import Path

LOG_FOLDER = "/usr/local/airflow/dags"
LOG_FILE = "log.txt"
def read(backend_name, key, *args, **kwargs):
    print(f"Reading from backend: {backend_name}")

    start_time = time.time()
    result = None
    if backend_name == "xcom":
        result = xcom_read(key, *args, **kwargs)
    elif backend_name == "s3":
        result = s3_read(key, args[0], args[1], **kwargs)
    elif backend_name == "custom":
        result = kwargs['custom_read']()

    duration = time.time() - start_time
    print(f"Read from {backend_name} with key {key} in {duration} seconds")
    context = get_current_context()  # Retrieve the execution context
    task_id = context['task'].task_id  # Get the current task's ID
    dag_id = context['dag'].dag_id
    
    if not dag_id.endswith("_optimized"):
        file_path = LOG_FOLDER  + '/' + dag_id + '/' + LOG_FILE
        output_file = Path(file_path)
        output_file.parent.mkdir(exist_ok=True, parents=True)
        with open(file_path, "a") as f:
            f.write(f"read {task_id} {backend_name} <{key}> {duration}\n")
    
    return result
    


def write(backend_name, key, value, *args, **kwargs):
    start_time = time.time()
    if backend_name == "xcom":
        xcom_write(key, value, *args, **kwargs)
    elif backend_name == "s3":
        s3_write(key, value, args[0], args[1], **kwargs)
    elif backend_name == "custom":
        kwargs['custom_write'](value)
    duration = time.time() - start_time
    print(f"Write to {backend_name} with key {key} in {duration} seconds")
    context = get_current_context()  # Retrieve the execution context
    task_id = context['task'].task_id  # Get the current task's ID
    dag_id = context['dag'].dag_id
    if not dag_id.endswith("_optimized"):
        file_path = LOG_FOLDER  + '/' + dag_id + '/' + LOG_FILE
        output_file = Path(file_path)
        output_file.parent.mkdir(exist_ok=True, parents=True)
        with open(LOG_FOLDER  + '/' + dag_id + '/' + LOG_FILE, "a+") as f:
            f.write(f"write {task_id} {backend_name} <{key}> {duration}\n")


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

# S3
def s3_read(key, s3_client, bucket_name):
    return s3_client.get_object(Bucket=bucket_name, Key=key)

def s3_write(key, value, s3_client, bucket_name):
    s3_client.put_object(Bucket=bucket_name, Key=key, Body=value)