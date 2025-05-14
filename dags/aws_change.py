from airflow import DAG
from airflow.operators.python import PythonOperator
from pprint import pprint
from airflowfusion.fuse import create_optimized_dag
from airflowfusion.backend_registry import read, write

from pathlib import Path



def dispense50(**kwargs):
    amount = kwargs['amount']
    number_of_50 = amount // 50
    remainder = amount % 50

    write('xcom', 'number_of_50', number_of_50)
    write('xcom', 'amount', remainder)
    


def dispense20(**kwargs):
    amount = read('xcom', 'amount')

    number_of_20 = amount // 20
    remainder = amount % 20

    write('xcom', 'number_of_20', number_of_20)
    write('xcom', 'amount', remainder)

def dispense10(**kwargs):
    amount = read('xcom', 'amount')

    number_of_10 = amount // 10
    remainder = amount % 10

    write('xcom', 'amount', remainder)
    write('xcom', 'number_of_10', number_of_10)

def dispense1(**kwargs):
    amount = read('xcom', 'amount')

    number_of_1 = amount // 1
    remainder = amount % 1

    write('xcom', 'amount', remainder)
    write('xcom', 'number_of_1', number_of_1)


dag_id = 'aws_change'
dag = DAG(
    dag_id=dag_id,
    description='Given amount, return number of each denomination',
    schedule_interval=None
)


t1 = PythonOperator(
    task_id="dispense50",
    python_callable=dispense50,
    dag=dag,
    provide_context=True,
    op_kwargs={'amount': 99}
)

t2 = PythonOperator(
    task_id="dispense20",
    python_callable=dispense20,
    dag=dag,
    provide_context=True,
)

t3 = PythonOperator(
    task_id="dispense10",
    python_callable=dispense10,
    dag=dag,
    provide_context=True,
)

t4 = PythonOperator(
    task_id="dispense1",
    python_callable=dispense1,
    dag=dag,
    provide_context=True,
)



# Create dependency to ensure run_get_data runs first before process_data
t1 >> t2 >> t3 >> t4


fused_dag = create_optimized_dag(dag, parallelize=False)
optimized_dag = create_optimized_dag(dag)


