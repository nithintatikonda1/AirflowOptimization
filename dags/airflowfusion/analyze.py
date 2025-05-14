from collections import deque
from airflowfusion.task import TaskGraph
from airflow.operators.python import PythonOperator
from airflow.models.baseoperator import BaseOperator
from airflow.operators.python import BranchPythonOperator
from airflowfusion.operator import ParallelFusedPythonOperator
from airflowfusion.optimize import optimize_integer_program
from airflow import DAG
import regex as re
from collections import defaultdict
from pprint import pprint

def is_fusable(op1: BaseOperator, op2: BaseOperator) -> bool:
    if op1 == op2:
        return False
    
    # If either operator is a branch operator, then they can't be fused
    if type(op1) is BranchPythonOperator or type(op2) is BranchPythonOperator:
        return False
    
    #If either operator has a BranchPythonOperator upstream, then they can't be fused
    for op in [op1, op2]:
        queue = deque()
        queue.append(op)
        while queue:
            current_op = queue.popleft()
            if type(current_op) is BranchPythonOperator:
                return False
            for upstream_op in current_op.upstream_list:
                queue.append(upstream_op)
            
    

    if type(op1) is ParallelFusedPythonOperator and type(op2) is not ParallelFusedPythonOperator:
        return False
    if type(op1) is not ParallelFusedPythonOperator and type(op2) is ParallelFusedPythonOperator:
        return False
    if type(op1) is ParallelFusedPythonOperator and type(op2) is ParallelFusedPythonOperator and op1.sharding_function != op2.sharding_function:
        return False
    
    return True

def create_fusion_possible_matrix(dag: DAG):
    fusion_possible = defaultdict(lambda: defaultdict(int))

    tasks = list(dag.task_dict.values())
    for op1 in tasks:
        for op2 in tasks:
            task1_id = op1.task_id
            task2_id = op2.task_id
            if is_fusable(op1, op2):
                fusion_possible[task1_id][task2_id] = 1

    return fusion_possible

def create_predecessor_matrix(dag: DAG):
        predecessors = defaultdict(lambda: defaultdict(int))

        def dfs(op, onPath):
            task_id = op.task_id 
            for predecessor_task_id in onPath:
                predecessors[predecessor_task_id][task_id] = 1

            onPath.append(task_id)
            for downstream_op in op.downstream_list:
                dfs(downstream_op, onPath)

            onPath.pop()

        operators = dag.task_dict.values()
        for op in operators:
            predecessors[op.task_id][op.task_id] = 0
            dfs(op, [])

        return predecessors

def create_read_costs_matrix(dag, read_write_costs_per_task):

    task_ids = list(dag.task_dict.keys())
    read_costs = {}
    for task_id1 in task_ids:
        read_costs[task_id1] = {}
        for task_id2 in task_ids:
            read_costs[task_id1][task_id2] = 0

            task1_reads_writes = set()
            for backend, key, read_write in read_write_costs_per_task.get(task_id1, []):
                task1_reads_writes.add((backend, key))
            
            for backend, key, read_write in read_write_costs_per_task.get(task_id2, []):
                if read_write == "read" and (backend, key) in task1_reads_writes:
                    read_costs[task_id1][task_id2] += read_write_costs_per_task[task_id2][(backend, key, read_write)]

    return read_costs