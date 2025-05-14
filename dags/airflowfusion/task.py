from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.models.baseoperator import BaseOperator
from collections import deque
from collections import defaultdict
import regex as re
import copy
import logging
from airflowfusion.optimize import optimize_integer_program
from pprint import pprint
from airflowfusion.operator import FusedPythonOperator, ParallelFusedPythonOperator

class TaskNode:
    
    def __init__(self, operators: list[BaseOperator], partition_function: str):
        self.operators = operators
        self.partition_function = partition_function
        self.upstream = []
        self.downstream = []


    def __str__(self) -> str:
        s = "<"
        for op in self.operators:
            s += op.task_id + ","
        return s[:-1] + ">"
    
    def __repr__(self) -> str:
        return self.__str__()
    
    def __eq__(self, __value: object) -> bool:
        return id(__value) == id(self)
    
    def __hash__(self) -> int:
        return id(self)
    
    def __lt__(self, other):
        return True
    
    def unique_id(self) -> str:
        return self.operators[0].task_id
    
    def is_parallelizable(self) -> bool:
        return type(self.operators[0]) is ParallelFusedPythonOperator
    
class TaskGraph:

    def __init__(self, tasks: list[TaskNode]):
        self.tasks = tasks

    def get_roots(self):
        return [task for task in self.tasks if len(task.upstream) == 0]
    
    def get_leaves(self):
        return [task for task in self.tasks if len(task.downstream) == 0]
        