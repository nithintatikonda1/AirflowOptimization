from airflow import DAG
from airflow.operators.python import PythonOperator
from pprint import pprint
from airflow.decorators import task
from airflowfusion.fuse import create_optimized_dag
from airflowfusion.operator import ParallelFusedPythonOperator



# Declare a dag with two python tasks that just print something

@task
def print_hello():
    print("Hello World")

@task
def print_goodbye():
    print("Goodbye World")

with DAG("test_dag", schedule_interval=None, start_date=None) as dag:
    print_hello() >> print_goodbye()

    x = ParallelFusedPythonOperator(task_id="fused", dag=dag)

    #fused_dag = create_optimized_dag(dag)

