from airflow import DAG
from airflow.operators.python import PythonOperator
from airflowfusion.operator import FusedPythonOperator
from airflowfusion.fuse import create_optimized_dag_integer_programming
from airflowfusion.backend_registry import read, write
import base64
import re

def base64_decode():
    input = 'eW91ciB0ZXh0'
    decoded = base64.b64decode(input).decode('utf-8')
    write('xcom', 'base64_decode', decoded)

def avg_word_length(sentence):
  words = sentence.split()
  return (sum(len(word) for word in words)/len(words))

def generate_stats():
    input = read('xcom', 'base64_decode')
    stats = {}
    stats['text_length'] = len(input)
    stats['avg_word_length'] = avg_word_length(input)
    stats['num_digits'] = len([x for x in input.split() if x.isdigit()])
    stats['num_special_chars'] = len([x for x in input.split() if not x.isalnum()])
    write('xcom', 'stats', stats)


def string_clean():
    input = read('xcom', 'base64_decode')
    cleaned_string = re.sub('\W+',' ', input)
    write('xcom', 'cleaned_string', cleaned_string)

def tokenize_count():
    input = read('xcom', 'cleaned_string')
    lowered_string = " ".join(x.lower() for x in input.split())
    lowered_string = lowered_string.replace('[^\w\s]','')
    split_string = lowered_string.split()
    word_count_map = {}
    for word in split_string:
        if word in word_count_map:
            word_count_map[word] += 1
        else:
            word_count_map[word] = 1
    write('xcom', 'word_count_map', word_count_map)


dag = DAG(
    dag_id='stock',
    description='Buy or sell stock',
    schedule_interval=None
)


t1 = FusedPythonOperator(
    task_id="decode",
    python_callable=base64_decode,
    dag=dag,
    provide_context=True,
)

t2 = FusedPythonOperator(
    task_id="stats",
    python_callable=generate_stats,
    dag=dag,
    provide_context=True,
)

t3 = FusedPythonOperator(
    task_id="clean",
    python_callable=string_clean,
    dag=dag,
    provide_context=True,
)

t4 = FusedPythonOperator(
    task_id="tokenize",
    python_callable=tokenize_count,
    dag=dag,
    provide_context=True,
)


t1 >> t2 >> t3 >> t4

#fused_dag = create_optimized_dag_integer_programming(dag, None, None, 1)
