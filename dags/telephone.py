from airflow import DAG
from airflow.operators.python import PythonOperator
from pendulum import datetime
import random
import string
from airflowfusion.backend_registry import read, write
from airflowfusion.fuse import create_optimized_dag

SECRET_WORD = "Avery likes to cuddle"
NUM_LETTERS_TO_REPLACE_OUTER_TG = 2
NUM_LETTERS_TO_REPLACE_INNER_TG = 3

# Function to simulate misunderstanding of the word
def misunderstand_task():
    word = read('xcom', 'word')
    num_letters_to_replace = NUM_LETTERS_TO_REPLACE_OUTER_TG
    for _ in range(num_letters_to_replace):
        random_index = random.randint(0, len(word) - 1)
        random_letter = random.choice(string.ascii_lowercase)
        word = word[:random_index] + random_letter + word[random_index + 1 :]
    write('xcom', 'word', word)
    

def misunderstand_task2():
    word = read('xcom', 'word')
    num_letters_to_replace = NUM_LETTERS_TO_REPLACE_INNER_TG
    for _ in range(num_letters_to_replace):
        random_index = random.randint(0, len(word) - 1)
        random_letter = random.choice(string.ascii_lowercase)
        word = word[:random_index] + random_letter + word[random_index + 1 :]
    write('xcom', 'word', word)


# Function to return the secret word
def upstream():
    write('xcom', 'word', SECRET_WORD)


# Function to output the final result
def downstream():
    new_word = read('xcom', 'word')
    write('xcom', 'result', f"The secret word was: {new_word}")


# Define the DAG
with DAG(
    dag_id='telephone_game_1',
    start_date=datetime(2023, 7, 1),
    schedule=None,
    catchup=False,
    tags=["webinar", "@task_group", "nesting", "passing_information"],
) as dag:

    # Define the tasks using PythonOperator
    upstream_task = PythonOperator(
        task_id='upstream',
        python_callable=upstream
    )

    t1 = PythonOperator(
        task_id='misunderstand_task1',
        python_callable=misunderstand_task,
    )

    t2 = PythonOperator(
        task_id='misunderstand_task2',
        python_callable=misunderstand_task2,
    )

    t3 = PythonOperator(
        task_id='misunderstand_task3',
        python_callable=misunderstand_task,
    )

    t4 = PythonOperator(
        task_id='misunderstand_task4',
        python_callable=misunderstand_task2,
    )

    downstream_task = PythonOperator(
        task_id='downstream',
        python_callable=downstream,
    )

    # Set task dependencies
    upstream_task >> t1 >> t2 >> t3 >> t4 >> downstream_task

    fused_dag = create_optimized_dag(dag, timing=True)
