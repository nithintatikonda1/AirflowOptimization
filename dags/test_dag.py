from airflow import DAG
from airflow.operators.python import PythonOperator
from pprint import pprint
from airflow.decorators import task
from airflowfusion.fuse import create_optimized_dag
from airflowfusion.operator import ParallelFusedPythonOperator



# Declare a dag with two python tasks that just print something


def print_goodbye():
    print("Goodbye World")

with DAG("test_dag", schedule_interval=None, start_date=None) as dag:

    t1 = PythonOperator(
        task_id="print_hello",
        python_callable=print_goodbye,
        dag=dag,
        provide_context=True,
    )

    t2 = PythonOperator(
        task_id="print_goodbye",
        python_callable=print_goodbye,
        dag=dag,
        provide_context=True,
    )

    t1 >> t2

    print(t2.upstream_list)

    

    #fused_dag = create_optimized_dag(dag)

