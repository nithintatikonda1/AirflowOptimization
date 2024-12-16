from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from pprint import pprint
from airflowfusion.fuse import create_optimized_dag, create_optimized_dag_integer_programming


def task2(**kwargs):
    ti = kwargs['ti']
    ti.xcom_push(key='amount', value = 100)

def task4(**kwargs):
    pass

def task5(**kwargs):
    ti = kwargs['ti']
    amount = ti.xcom_pull(key='amount')



dag = DAG(
    dag_id='distant_example_high_read_cost',
    description='Show fusion across branches',
    schedule_interval=None
)


t1 = BashOperator(
        task_id='task1',
        bash_command='echo "Hello from task 1!"'
    )

t2 = PythonOperator(
    task_id="task2",
    python_callable=task2,
    dag=dag,
    provide_context=True,
)

t3 = BashOperator(
        task_id='task3',
        bash_command='echo "Hello from task 3!"'
    )

t4 = PythonOperator(
    task_id="task4",
    python_callable=task4,
    dag=dag,
    provide_context=True,
)

t5 = PythonOperator(
    task_id="task5",
    python_callable=task5,
    dag=dag,
    provide_context=True,
)

t6 = BashOperator(
        task_id='task6',
        bash_command='echo "Hello from task 6!"'
    )


# Create dependency to ensure run_get_data runs first before process_data
t1 >> t2
t1 >> t3

t2 >> t4
t3 >> t4

t4 >> t5
t4 >> t6

total_costs = {'task1': 2, 'task2': 30, 'task3': 2, 'task4': 2, 'task5': 30, 'task6': 2}
read_costs = {'task2': {'amount': 29}
            }

fused_dag = create_optimized_dag_integer_programming(dag, total_costs, read_costs, 1)


