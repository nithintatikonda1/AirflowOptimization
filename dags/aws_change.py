from airflow import DAG
from airflow.operators.python import PythonOperator
from pprint import pprint
from airflowfusion.fuse import create_optimized_dag, create_optimized_dag_integer_programming

def dispense50(**kwargs):
    amount = kwargs['amount']
    ti = kwargs['ti']

    number_of_50 = amount // 50
    remainder = amount % 50

    ti.xcom_push(key='number_of_50', value = number_of_50)
    ti.xcom_push(key='amount1', value = remainder)


def dispense20(**kwargs):
    ti = kwargs['ti']
    amount = ti.xcom_pull(key='amount1')

    number_of_20 = amount // 20
    remainder = amount % 20

    ti.xcom_push(key='number_of_20', value = number_of_20)
    ti.xcom_push(key='amount2', value = remainder)

def dispense10(**kwargs):
    ti = kwargs['ti']
    amount = ti.xcom_pull(key='amount2')

    number_of_10 = amount // 10
    remainder = amount % 10

    ti.xcom_push(key='number_of_10', value = number_of_10)
    ti.xcom_push(key='amount3', value = remainder)

def dispense1(**kwargs):
    ti = kwargs['ti']
    amount = ti.xcom_pull(key='amount3')

    number_of_1 = amount // 1
    remainder = amount % 1

    ti.xcom_push(key='number_of_1', value = number_of_1)
    ti.xcom_push(key='amount4', value = remainder)


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

total_costs = {'dispense50': 2, 'dispense20': 2, 'dispense10': 2, 'dispense1': 2}

read_costs = {'dispense50': {'amount1': 1}, 
              'dispense20': {'amount2': 1}, 
              'dispense10': {'amount3': 1}, 
              'dispense1': {'amount4': 1}
            }
read_costs = {'dispense50': {'dispense50': 0, 'dispense20': 0, 'dispense10': 0, 'dispense1': 0}, 
              'dispense20': {'dispense50': 0, 'dispense20': 0, 'dispense10': 0, 'dispense1': 0}, 
              'dispense10': {'dispense50': 0, 'dispense20': 0, 'dispense10': 0, 'dispense1': 0}, 
              'dispense1': {'dispense50': 0, 'dispense20': 0, 'dispense10': 0, 'dispense1': 0}
            }
read_costs = {'dispense50': {'amount1': 1}, 
              'dispense20': {'amount2': 1}, 
              'dispense10': {'amount3': 1}, 
              'dispense1': {'amount4': 1}
            }

#fused_dag = create_optimized_dag_integer_programming(dag, total_costs, read_costs, 1)


