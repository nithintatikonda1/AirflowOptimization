from airflow import DAG
from airflow.operators.python import PythonOperator
from pprint import pprint
from airflowfusion.fuse import create_optimized_dag


def dispense50(**kwargs):
    amount = kwargs['amount']
    ti = kwargs['ti']

    number_of_50 = amount // 50
    remainder = amount % 50

    ti.xcom_push(key='number_of_50', value = number_of_50)
    ti.xcom_push(key='amount', value = remainder)


def dispense20(**kwargs):
    ti = kwargs['ti']
    amount = ti.xcom_pull(key='amount')

    number_of_20 = amount // 20
    remainder = amount % 20

    ti.xcom_push(key='number_of_20', value = number_of_20)
    ti.xcom_push(key='amount', value = remainder)

def dispense10(**kwargs):
    ti = kwargs['ti']
    amount = ti.xcom_pull(key='amount')

    number_of_10 = amount // 10
    remainder = amount % 10

    ti.xcom_push(key='number_of_10', value = number_of_10)
    ti.xcom_push(key='amount', value = remainder)

def dispense1(**kwargs):
    ti = kwargs['ti']
    amount = ti.xcom_pull(key='amount')

    number_of_1 = amount // 1
    remainder = amount % 1

    ti.xcom_push(key='number_of_1', value = number_of_1)
    ti.xcom_push(key='amount', value = remainder)


dag = DAG(
    dag_id='aws_change',
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



# Create dependency to ensure run_get_data runs first before process_data
t1

total_costs = {'dispense50': 2}
read_costs = {'dispense50': {'dispense50': 0}
            }

fused_dag = create_optimized_dag(dag, total_costs, read_costs, 1)


