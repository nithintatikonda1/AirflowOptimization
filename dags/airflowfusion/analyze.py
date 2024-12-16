from airflowfusion.task import TaskGraph
from airflow.operators.python import PythonOperator
from airflowfusion.util import get_function_definition_lines
from airflowfusion.util import PULL_PATTERN, PUSH_PATTERN
import regex as re
from collections import defaultdict
from pprint import pprint

def create_read_costs_matrix(task_graph: TaskGraph, read_costs_per_task):
    if task_graph.fusion_performed:
        raise Exception("Fusion already performed")
    
    task_id_to_read, read_to_task_ids, task_id_to_write, write_to_task_ids = analyze_task_graph(task_graph)

    read_costs = {}
    for task in task_graph.tasks:
        task_id1 = task.operators[0].task_id
        read_costs[task_id1] = {}
        for task2 in task_graph.tasks:
            task_id2 = task2.operators[0].task_id
            read_costs[task_id1][task_id2] = 0

            for read in task_id_to_read[task_id2]:
                if read in task_id_to_write[task_id1] or read in task_id_to_read[task_id1]:
                    if task_id1 in read_costs_per_task:
                        read_costs[task_id1][task_id2] = max(read_costs_per_task[task_id1].get(read, 0), read_costs[task_id1][task_id2])

    return read_costs

    

def analyze_task_graph(task_graph: TaskGraph):
    if task_graph.fusion_performed:
        raise Exception("Fusion already performed")
    
    # Find the reads/writes per tasks
    task_id_to_read = {}
    read_to_task_ids = defaultdict(set)
    task_id_to_write = {}
    write_to_task_ids = defaultdict(set)
    for task in task_graph.tasks:
        operator = task.operators[0]
        task_id = operator.task_id
        task_id_to_read[task_id] = set()
        task_id_to_write[task_id] = set()
        if isinstance(operator, PythonOperator):
            lines = get_function_definition_lines(operator.python_callable)
            for line in lines:
                pull_search = re.search(PULL_PATTERN, line)
                push_search = re.search(PUSH_PATTERN, line)
                key = None
                if pull_search:
                    key = pull_search.group(1)
                    task_id_to_read[task_id].add(key)
                    read_to_task_ids[key].add(task_id)
                elif push_search:
                    key = push_search.group(1)
                    task_id_to_write[task_id].add(key)
                    write_to_task_ids[key].add(task_id)

    return task_id_to_read, read_to_task_ids, task_id_to_write, write_to_task_ids

                


