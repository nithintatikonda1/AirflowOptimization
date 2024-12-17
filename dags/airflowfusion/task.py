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

class TaskNode:
    
    def __init__(self, operators: list[BaseOperator], partition_function: str, read_costs: dict = {}, total_cost: int = 0, failure_rate: int = 0):
        self.operators = operators
        self.partition_function = partition_function
        self.upstream = []
        self.downstream = []
        
        # Map of task id to read cost. 
        # The task id represents the operator that writes the data this task node reads
        self.profiling_costs_exist = False
        self.read_costs = read_costs
        self.total_cost = total_cost

        self.failure_rate = failure_rate


    def get_execution_time(self):
        return self.total_cost / (1 - self.failure_rate)

    def add_profiling_costs(self, read_costs: dict, total_cost: int):
        self.read_costs = read_costs
        self.total_cost = total_cost

    def set_failure_rate(self, failure_rate: int):
        self.failure_rate = failure_rate

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
    
class TaskGraph:

    def __init__(self, tasks: list[TaskNode], scheduling_cost: int):
        self.tasks = tasks
        self.scheduling_cost = scheduling_cost
        self.fusion_performed = False

    def get_roots(self):
        return [task for task in self.tasks if len(task.upstream) == 0]
    
    def get_leaves(self):
        return [task for task in self.tasks if len(task.downstream) == 0]
    
    def is_fusable(self, task1: TaskNode, task2: TaskNode) -> bool:
        if task1 == task2:
            return False
        if isinstance(task1.operators[0], PythonOperator) and isinstance(task2.operators[0], PythonOperator) and not isinstance(task1.operators[0], BranchPythonOperator) and not isinstance(task2.operators[0], BranchPythonOperator):
            return True
        return False
    
    def is_fusion_beneficial(self, task1: TaskNode, task2: TaskNode) -> bool:
        if not self.is_fusable(task1, task2):
            return False
        #TODO: Check if fusion saves time
        return False
    
    def perform_complete_fusion(self):
        if self.fusion_performed:
            raise Exception("Fusion already performed")
        self.perform_fusion(self.is_fusable)
        self.fusion_performed = True

    def perform_optimized_fusion(self):
        if self.fusion_performed:
            raise Exception("Fusion already performed")
        self.perform_fusion(self.is_fusion_beneficial)
        self.fusion_performed = True

    def get_fusion_variables(self):
        if self.fusion_performed:
            raise Exception("Fusion already performed")
        
        predecessors = self.getPredecessors()
        fusion_possible = self.getFusionPossible()
        read_costs = self.getReadCosts()
        total_costs = self.getTotalCosts()

        
        return optimize_integer_program(predecessors, fusion_possible, self.scheduling_cost, total_costs, read_costs)
        
    def getTotalCosts(self):
        if self.fusion_performed:
            raise Exception("Fusion already performed")
        total_costs = defaultdict(int)
        for task in self.tasks:
            task_id = task.operators[0].task_id
            total_costs[task_id] = task.total_cost
        return total_costs

    def getReadCosts(self):
        if self.fusion_performed:
            raise Exception("Fusion already performed")
        read_costs = defaultdict(lambda: defaultdict(int))
        for task1 in self.tasks:
            for task2 in self.tasks:
                task1_id = task1.operators[0].task_id
                task2_id = task2.operators[0].task_id
                read_costs[task1_id][task2_id] = task1.read_costs.get(task2_id, 0)

        return read_costs
        
    def getPredecessors(self):
        if self.fusion_performed:
            raise Exception("Fusion already performed")
        predecessors = defaultdict(lambda: defaultdict(int))

        def dfs(task, onPath):
            task_id = task.operators[0].task_id
            for predecessor_task in onPath:
                predecessors[predecessor_task][task_id] = 1

            onPath.append(task_id)
            for downstream_task in task.downstream:
                dfs(downstream_task, onPath)

            onPath.pop()

        for task in self.tasks:
            predecessors[task.operators[0].task_id][task.operators[0].task_id] = 0
            dfs(task, [])

        return predecessors
    
    def getFusionPossible(self):
        if self.fusion_performed:
            raise Exception("Fusion already performed")
        fusion_possible = defaultdict(lambda: defaultdict(int))

        for task1 in self.tasks:
            for task2 in self.tasks:
                task1_id = task1.operators[0].task_id
                task2_id = task2.operators[0].task_id
                if self.is_fusable(task1, task2):
                    fusion_possible[task1_id][task2_id] = 1

        return fusion_possible
    
    def perform_fusion(self, decision_function) -> None:
        """
        Modifies the graph by fusing consecutive tasks if they can be fused
        """
        # Get a set of all tasks currently in the graph
        in_graph = set()
        queue = deque(self.get_roots())
        while len(queue) > 0:
            task = queue.popleft()
            if task in in_graph:
                continue
            in_graph.add(task)

            for downstream_task in task.downstream:
                if downstream_task not in in_graph:
                    in_graph.add(downstream_task)
                    queue.append(downstream_task)

        #Fuse consecutive nodes if beneficial
        queue = deque(self.get_roots())

        while len(queue) > 0:
            task = queue.popleft()
            if task not in in_graph:
                continue
            fused = False

            downstream_task = True
            for dt in task.downstream:
                if decision_function(task, dt):
                    fused = True
                    downstream_task = dt
                    break

            if fused:
                new_task = fuse(task, downstream_task)
                in_graph.discard(task)
                in_graph.discard(downstream_task)
                in_graph.add(new_task)
                queue.appendleft(new_task)
            else:
                for dt in task.downstream:
                    queue.append(dt)

        self.tasks = list(in_graph)