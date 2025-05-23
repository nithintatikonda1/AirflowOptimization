"""
### Push and pull XComs implicitly and explicitly with traditional operators

This DAG shows how to pass data between traditional operators using XCom.
"""

from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from pendulum import datetime
from airflowfusion.backend_registry import read, write
from airflowfusion.fuse import create_optimized_dag
import random
from datetime import timedelta


def sender_task_function(**context):
    # push values to XCom explicitly with a specific key by using the .xcom_push method
    # of the task instance (ti) in the Airflow context
    write('xcom', 'my_number', 23)
    
    write('xcom', 'return', "Avery")


def receiver_task_function(**context):
    # pull values from XCom explicitly with a specific key by using the .xcom_pull method
    xcom_received = read('xcom', 'return')
    my_number = read('xcom', 'my_number')

    print(xcom_received + f" deserves {my_number} treats!")
    if random.random() < :
        raise ValueError("Random Error")


@dag(
    start_date=datetime(2023, 3, 27),
    dag_id="push_pull",
    schedule=None,
    catchup=False,
    tags=["traditional operators"],
)
def standard_xcom_traditional():
    sender_task = PythonOperator(
        task_id="sender_task",
        python_callable=sender_task_function,
    )

    receiver_task = PythonOperator(
        task_id="receiver_task",
        python_callable=receiver_task_function,
        retries=20,
        retry_delay=timedelta(milliseconds=1),
        params={'failure_rate': 0}
    )

    sender_task >> receiver_task


dag = standard_xcom_traditional()
fused_dag = create_optimized_dag(dag, parallelize=False)
optimized_dag = create_optimized_dag(dag)