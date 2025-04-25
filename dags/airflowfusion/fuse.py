from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.models.baseoperator import BaseOperator
from collections import deque
from collections import defaultdict
import regex as re
import copy
import logging
from airflowfusion.analyze import create_read_costs_matrix
from airflowfusion.task import TaskGraph, TaskNode
from pprint import pprint
from airflowfusion.read_write_interceptor import ReadWriteInterceptor
from airflowfusion.operator import ParallelFusedPythonOperator, FusedPythonOperator
import os
import pickle
from airflowfusion.backend_registry import read, write
from airflowfusion.analyze import create_fusion_possible_matrix, create_predecessor_matrix, create_read_costs_matrix
from airflowfusion.optimize import optimize_integer_program
import time
from heapq import heappush, heappop


def copy_operator(operator: BaseOperator, dag: DAG) -> BaseOperator:
    """
    Creates copy of an operator with a different dag

    Args:
        operator (BaseOperator): Operator to copy
        dag (DAG): dag to copy operator to

    Returns:
        BaseOperator: The copied operator
    """
    args = operator._BaseOperator__init_kwargs.copy()
    args['dag'] = dag
    operator_copy = type(operator)(**args)
    return operator_copy


def build_graph_from_dag(dag: DAG, total_costs: dict = {}, read_costs: dict = {}, scheduling_cost=0) -> TaskGraph:
    """
    Takes a DAG and builds a graph of task nodes

    Args:
        dag (DAG): Input DAG.

    Returns:
        TaskGraph: Graph of task nodes
    """
    sorted_operators = list(dag.topological_sort())
    task_map = {}
    
    for operator in sorted_operators:
        partition_function = None
        if type(operator) is ParallelFusedPythonOperator:
            partition_function = operator.partition_function
        task = TaskNode([operator], partition_function, read_costs=read_costs.get(operator.task_id, {}), total_cost=total_costs.get(operator.task_id, 0))
        task_map[operator] = task

        for upstream_operator in operator.upstream_list:
            upstream_task = task_map[upstream_operator]
            upstream_task.downstream.append(task)
            task.upstream.append(upstream_task)

    task_graph =  TaskGraph(list(task_map.values()), scheduling_cost, dag.dag_id)

    return task_graph




def create_operator_from_task(task: TaskNode, dag: DAG, persistent_store: callable=None, sharding_num: int=8) -> list[PythonOperator]:
    if len(task.operators) == 1 and type(task.operators[0]) is not ParallelFusedPythonOperator:
        copied_operator = copy_operator(task.operators[0], dag)
        return [copied_operator]
    
    if type(task.operators[0]) is not ParallelFusedPythonOperator or sharding_num == 1:

        interceptor = ReadWriteInterceptor()
        
        execute_function = interceptor.get_pipeline_function(task.operators)
        return [FusedPythonOperator(
            task_id= "--".join([op.task_id for op in task.operators]),
            dag=dag,
            execute_function=execute_function
        )]
    elif type(task.operators[0]) is ParallelFusedPythonOperator:
        data_collection_functions = []
        sharding_function = task.operators[0].sharding_function
        compute_functions = []
        merge_function = task.operators[0].merge_function
        write_functions = []

        for operator in task.operators:
            data_collection_functions.append(operator.data_collection_function)
            compute_functions.append(operator.compute_function)
            write_functions.append(operator.write_function)

        # combined data collection function that returns a list of outputs from each data collection function
        def combined_data_collection_and_sharding():
            for task_index, data_collection_function in enumerate(data_collection_functions):
                sharded_data = sharding_function(sharding_num, data_collection_function())
                for sharding_index, data in enumerate(sharded_data):
                    key = task.operators[task_index].task_id + "_before_" + str(sharding_index)
                    write("xcom", key, data)

        # compute function that acts on each of the sharded data
        def combined_compute(sharding_index):
            for task_index, compute_function in enumerate(compute_functions):
                data = read("xcom", task.operators[task_index].task_id + "_before_" + str(sharding_index))
                output = compute_function(data)
                write("xcom", task.operators[task_index].task_id + "_after_" + str(sharding_index), output)

        # merge data from compute functions and write merged output
        def combined_merge_and_write():
            for task_index, write_function in enumerate(write_functions):
                all_data = []
                for sharding_index in range(sharding_num):
                    data = read("xcom", task.operators[task_index].task_id + "_after_" + str(sharding_index))
                    all_data.append(data)
                output = merge_function(all_data)
                write_function(output)
        
        interceptor = ReadWriteInterceptor()
        starting_operator =PythonOperator(
            task_id= "--".join([op.task_id for op in task.operators] + ["---start"]),
            python_callable=interceptor.optimize_function_without_context(combined_data_collection_and_sharding),
            dag=dag,
        )

        interceptor = ReadWriteInterceptor()
        ending_operator =PythonOperator(
            task_id= "--".join([op.task_id for op in task.operators] + ["---end"]),
            python_callable=interceptor.optimize_function_without_context(combined_merge_and_write),
            dag=dag,
        )

        compute_operators = []
        for i in range(sharding_num):
            interceptor = ReadWriteInterceptor()
            compute_operator = PythonOperator(
                task_id= "--".join([op.task_id for op in task.operators] + ["---compute", str(i)]),\
                python_callable=interceptor.optimize_function_without_context(lambda: combined_compute(i)),
                dag=dag,
            )
            compute_operator.set_upstream(starting_operator)
            compute_operator.set_downstream(ending_operator)
            compute_operators.append(compute_operator)

        return [starting_operator, compute_operators, ending_operator]


        

def compute_fusion(dag: DAG, read_write_costs_per_task: dict, total_costs: dict, scheduling_cost: float):
    predecessors = create_predecessor_matrix(dag, read_write_costs_per_task)
    fusion_possible = create_fusion_possible_matrix(dag)
    read_costs = create_read_costs_matrix(dag, read_write_costs_per_task)

    
    return optimize_integer_program(predecessors, fusion_possible, scheduling_cost, total_costs, read_costs)



def build_dag_from_graph(task_graph: TaskGraph, fused_dag: DAG, total_costs: dict = {}, read_costs: dict = {}, scheduling_cost=0.5, num_workers=8):
    operator_map = {}
    """
    for task in task_graph.tasks:
        operators = create_operator_from_task(task, fused_dag)
        operator_map[task] = operators
    
    for task in task_graph.tasks:
        operators = operator_map[task]

        for downstream_task in task.downstream:
            try:
                downstream_operator = operator_map[downstream_task][0]
                operators[-1].set_downstream(downstream_operator)
            except Exception as e:
                raise ValueError(operator_map)
    """
    executing_tasks = [] # Min heap of (end time, task)
    queued_tasks = [] # Min heap of (start time, task)
    finished_tasks = set()
    for task in task_graph.get_roots():
        heappush(queued_tasks, (0, task))

    latest_end_time = 0
    while executing_tasks or queued_tasks:
        if executing_tasks:
            end_time, task = heappop(executing_tasks)
            while executing_tasks and executing_tasks[0][0] == end_time:
                heappop(executing_tasks)
            latest_end_time = max(latest_end_time, end_time)
            finished_tasks.add(task.unique_id())

            for downstream_task in task.downstream:
                predecessors_finished = all(predecessor.unique_id() in finished_tasks for predecessor in downstream_task.upstream)
                if predecessors_finished:
                    heappush(queued_tasks, (end_time + scheduling_cost, downstream_task))

        while queued_tasks and len(executing_tasks) < num_workers:
            available_workers = max((num_workers - len(executing_tasks)) // 2, 1)
            _, task = heappop(queued_tasks)
            task_duration = sum(total_costs[operator.task_id] for operator in task.operators)
            if task.is_parallelizable():
                end_time = latest_end_time + scheduling_cost + task_duration / available_workers
                for _ in range(available_workers):
                    heappush(executing_tasks, (end_time, task))
                operator_map[task] = create_operator_from_task(task=task, dag=fused_dag, sharding_num=available_workers)
            else:
                end_time = latest_end_time + scheduling_cost + task_duration
                heappush(executing_tasks, (end_time, task))
                operator_map[task] = create_operator_from_task(task=task, dag=fused_dag, sharding_num=1)

    for task in task_graph.tasks:
        operators = operator_map[task]

        for downstream_task in task.downstream:
            try:
                downstream_operator = operator_map[downstream_task][0]
                operators[-1].set_downstream(downstream_operator)
            except Exception as e:
                raise ValueError(operator_map)



def create_optimized_dag(dag: DAG, default_args=None, timing=False) -> DAG:
    logging.info('Beginning creation of optimized DAG')
    new_dag = DAG(dag_id=dag._dag_id + '_optimized', default_args=default_args)

    for key, val in dag.__dict__.items():
        if key == '_dag_id':
            continue
        elif key == 'safe_dag_id':
            new_dag.__dict__[key] = new_dag._dag_id
        elif key == 'task_count':
            new_dag.__dict__[key] = 0
        elif key == 'task_dict':
            new_dag.__dict__[key] = {}
        elif key == '_task_group':
            continue
        else:
            new_dag.__dict__[key] = val


    task_graph, total_costs, read_costs = create_optimized_task_graph(dag, timing)
    build_dag_from_graph(task_graph, new_dag, total_costs, read_costs)
    return new_dag

def create_optimized_task_graph(dag: DAG, timing=False) -> TaskGraph:
    logging.info('Beginning creation of optimized task graph')
    dag_id = dag._dag_id
    task_ids = list(dag.task_dict.keys())

    # task durations costs from file. first check ./include then check ../include
    total_costs = None
    if os.path.exists(f"./include/dag_timings/{dag.dag_id}_task_durations.pkl"):
        total_costs = f"./include/dag_timings/{dag.dag_id}_task_durations.pkl"
    elif os.path.exists(f"../include/dag_timings/{dag.dag_id}_task_durations.pkl"):
        total_costs = f"../include/dag_timings/{dag.dag_id}_task_durations.pkl"
    else:
        raise Exception("Could not find task durations for DAG")
    logging.info("Getting tasks durations from file")
    with open(total_costs, "rb") as f:
        total_costs = pickle.load(f)
    logging.info("Task durations loaded")

    # read write costs from file
    read_write_costs = None
    if os.path.exists(f"./include/dag_timings/{dag.dag_id}_timing_logs.pkl"):
        read_write_costs = f"./include/dag_timings/{dag.dag_id}_timing_logs.pkl"
    elif os.path.exists(f"../include/dag_timings/{dag.dag_id}_timing_logs.pkl"):
        read_write_costs = f"../include/dag_timings/{dag.dag_id}_timing_logs.pkl"
    else:
        raise Exception("Could not find read write costs for DAG")
    logging.info("Getting read costs from file")
    with open(read_write_costs, "rb") as f:
        read_write_costs = pickle.load(f)
    logging.info("Read costs loaded")

    # Create data matrices
    for task in task_ids:
        if task not in total_costs:
            total_costs[task] = 1000
        elif total_costs[task] == None:
            total_costs[task] = 0
        elif task == 'vectorize_summaries':
            total_costs[task] = 1000

    read_costs = create_read_costs_matrix(dag, read_write_costs)
    predecesors = create_predecessor_matrix(dag)
    fusion_possible = create_fusion_possible_matrix(dag)

    # If IP variables already exist, load them, otherwise run IP
    fused_edges, operator_edges = None, None
    if os.path.exists(f"./include/{dag_id}.pkl"):
        logging.info("IP variables already exist")
        with open(f"./include/{dag_id}.pkl", "rb") as f:
            fused_edges, operator_edges = pickle.load(f)
    else:
        logging.info("IP variables do not exist. Preparing for IP")

        logging.info("Starting IP")
        start_time = time.time()
        fused_edges, operator_edges = optimize_integer_program(predecesors, fusion_possible, 0.5, total_costs, read_costs)
        elapsed_time = time.time() - start_time
        logging.info("IP complete in %.2f seconds", elapsed_time)

        # Write the dag id and elapsed time to a file with the formate: dag_id, elapsed_time
        if timing:
            with open(f"./include/optimization_time.txt", "a") as f:
                f.write(f"{dag_id}, {elapsed_time}\n")

        # Serialize
        #CHANGE LOCATION
        logging.info("Serializing IP variables")
        with open(f"./include/{dag_id}.pkl", "wb") as f:
            pickle.dump((fused_edges, operator_edges), f)
        logging.info("IP variables serialized")


    # task_ids to operators
    task_id_to_operator = dag.task_dict
    
    # Find fused components
    adj_lst = defaultdict(list)
    for edge in fused_edges:
        adj_lst[edge[0]].append(edge[1])
        adj_lst[edge[1]].append(edge[0])

    explored = set()
    task_id_to_component_id = {}
    component_id = 0
    components = []
    for task_id in task_ids:
        if task_id not in explored:
            components.append([])
            queue = deque([task_id])
            while len(queue) > 0:
                task_id = queue.popleft()
                task_id_to_component_id[task_id] = component_id
                components[component_id].append(task_id)
                explored.add(task_id)
                for neighbor in adj_lst[task_id]:
                    if neighbor not in explored:
                        queue.append(neighbor)
            component_id += 1

    # Create Task Nodes
    task_id_to_task_node = {}
    tasks = []
    for component in components:
        # Reorder within the component
        for from_task_id, to_task_id in fused_edges:
            if from_task_id in component:
                component.remove(from_task_id)
                component.insert(component.index(to_task_id), from_task_id)

        task = TaskNode([task_id_to_operator[task_id] for task_id in component], None)
        tasks.append(task)
        for task_id in component:
            task_id_to_task_node[task_id] = task

    # Link Task Nodes
    for edge in operator_edges:
        task_id_to_task_node[edge[0]].downstream.append(task_id_to_task_node[edge[1]])
        task_id_to_task_node[edge[1]].upstream.append(task_id_to_task_node[edge[0]])

    # Create task graph
    task_graph = TaskGraph(tasks)
        
    return task_graph, total_costs, read_costs