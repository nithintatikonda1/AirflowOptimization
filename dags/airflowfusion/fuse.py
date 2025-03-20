from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.models.baseoperator import BaseOperator
from collections import deque
from collections import defaultdict
from airflowfusion.util import get_function_definition_lines
from airflowfusion.util import PULL_PATTERN, PUSH_PATTERN
import regex as re
import copy
import logging
from airflowfusion.analyze import create_read_costs_matrix
from airflowfusion.task import TaskGraph, TaskNode
from pprint import pprint
from airflowfusion.read_write_interceptor import ReadWriteInterceptor
from airflowfusion.operator import ParallelFusedPythonOperator
import os
import pickle


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

def fuse(task1: TaskNode, task2: TaskNode) -> TaskNode:
    """
    Takes two task nodes and combines them into one tasknode, reassigning the upstream and downstream tasks

    Args:
        task1 (TaskNode): First task node
        task2 (TaskNode): Second task node

    Returns:
        TaskNode: The newly created fused Task Node
    """
    fused_task = TaskNode(task1.operators + task2.operators, task1.partition_function if task1.partition_function == task2.partition_function else None)

    upstream = task1.upstream.copy()
    upstream.extend(x for x in task2.upstream if (x not in upstream and x != task1))
    fused_task.upstream = upstream

    downstream = task2.downstream.copy()
    downstream.extend(x for x in task1.downstream if (x not in downstream and x != task2))
    fused_task.downstream = downstream

    for task in fused_task.downstream:
        if task1 in task.upstream:
            task.upstream.remove(task1)
        if task2 in task.upstream:
            task.upstream.remove(task2)
        task.upstream.append(fused_task)

    for task in fused_task.upstream:
        if task1 in task.downstream:
            task.downstream.remove(task1)
        if task2 in task.downstream:
            task.downstream.remove(task2)
        task.downstream.append(fused_task)

    return fused_task


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


def fuse_python_functions(functions, op_kwargs_list):
    """
    Takes a list of functions and outputs a string which contains the definition for the fused function.

    Args:
        functions (list[Function]): List of functions to fuse

    Returns:
        str: The new function definition.
    """
    def generate_unique_variable_name():
        i = 0
        while True:
            yield "unique_variable_" + str(i)
            i += 1
    gen_variable = generate_unique_variable_name()
    def generate_unique_function_name():
        i = 0
        while True:
            yield "unique_function_" + str(i)
            i += 1
    gen_function = generate_unique_function_name()


    push_pattern = PUSH_PATTERN
    pull_pattern = PULL_PATTERN

    persisted = {}


    result = []
    function_call_lines = []

    result.append("persistence_map = {} \n")
    for i in range(len(functions)):
        func = functions[i]
        op_kwargs = op_kwargs_list[i]
        function_lines = get_function_definition_lines(func)
        function_name = next(gen_function)
        result.append('def ' + function_name + '():' )
        if op_kwargs is not None:
            result.append('    nonlocal kwargs')
            result.append('    kwargs = kwargs | ' + str(op_kwargs))
        function_call_lines.append(function_name + "()")
        for line in function_lines[1:]:
            pull_search = re.search(pull_pattern, line)
            push_search = re.search(push_pattern, line)
            if pull_search and pull_search.group(1) in persisted:
                key = pull_search.group(1)
                new_line = re.sub(pull_pattern, 'persistence_map[\'' + key + '\']', line)
                result.append(new_line)
            elif push_search:
                key = push_search.group(1)
                value = push_search.group(2)
                variable_name = next(gen_variable)
                persisted[key] = variable_name
                index = len(line) - len(line.lstrip())
                new_line = line[:index] + 'persistence_map[\'' + key + '\'] = ' + value
                result.append(new_line)
            else:
                result.append(line)

    result.extend(function_call_lines)

    for i in range(len(result)):
        result[i] = '    ' + result[i]

    result.insert(0, "def fused_function(**kwargs):")

    result.append("    ti = kwargs['ti']")
    result.append("    for key, value in persistence_map.items():")
    result.append("        ti.xcom_push(key=key, value=value)")

    #print("\n".join(result))
    return "\n".join(result)

def create_operator_from_task(task: TaskNode, dag: DAG) -> PythonOperator:
    if len(task.operators) == 1:
        copied_operator = copy_operator(task.operators[0], dag)
        return copied_operator
    
    functions = [operator.python_callable for operator in task.operators]
    op_kwarg_list = [operator.op_kwargs for operator in task.operators]
    op_kwargs = {}
    for kwargs in op_kwarg_list:
        op_kwargs.update(kwargs)
    
    """
    op_kwargs_list = [operator._BaseOperator__init_kwargs['op_kwargs'] if 'op_kwargs' in operator._BaseOperator__init_kwargs else None for operator in task.operators]
    function_def = fuse_python_functions(functions, op_kwargs_list)
    local_scope = {}
    exec(function_def, globals(), local_scope)
    result = local_scope 
    """
    interceptor = ReadWriteInterceptor()
    
    return PythonOperator(
        task_id= "--".join([op.task_id for op in task.operators]),
        python_callable=interceptor.get_pipeline_function(functions),
        op_kwargs=op_kwargs,
        dag=dag,
    )

def build_dag_from_graph(task_graph: TaskGraph, fused_dag: DAG):
    queue = deque(task_graph.get_roots())
    operator_map = {}

    """
    while len(queue) > 0:
        task = queue.popleft()
        if task in operator_map:
            continue
        
        operator = create_operator_from_task(task, fused_dag)
        operator_map[task] = operator
        for upstream_task in task.upstream:
            upstream_operator = operator_map[upstream_task]
            operator.set_upstream(upstream_operator)

        for downstream_task in task.downstream:
            if downstream_task not in operator_map:
                queue.append(downstream_task)
    """

    for task in task_graph.tasks:
        operator = create_operator_from_task(task, fused_dag)
        operator_map[task] = operator
    
    for task in task_graph.tasks:
        operator = operator_map[task]

        for downstream_task in task.downstream:
            downstream_operator = operator_map[downstream_task]
            operator.set_downstream(downstream_operator)


def create_optimized_dag_integer_programming(dag: DAG, total_costs, read_costs, scheduling_cost: int, default_args=None) -> DAG:
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


    task_graph = create_optimized_task_graph_integer_programming(dag, total_costs, read_costs, scheduling_cost)
        
    build_dag_from_graph(task_graph, new_dag)
    return new_dag

def create_optimized_task_graph_integer_programming(dag: DAG, total_costs, read_write_costs, scheduling_cost: int) -> TaskGraph:
    logging.info('Beginning creation of optimized task graph')
    dag_id = dag._dag_id

    task_graph = build_graph_from_dag(dag, scheduling_cost = scheduling_cost)
    task_ids = [task.operators[0].task_id for task in task_graph.tasks]
    r = {t1: {t2: 0 for t2 in task_ids} for t1 in task_ids}

    fused_edges, operator_edges = None, None
    # If IP variables already exist
    ip_vars_found = os.path.exists(f"./include/{dag_id}.pkl")
    if ip_vars_found:
        logging.info("IP variables already exist")
        with open(f"./include/{dag_id}.pkl", "rb") as f:
            fused_edges, operator_edges = pickle.load(f)
    else:
        logging.info("IP variables do not exist. Preparing for IP")
        # if total_costs is a string, user has provided
        #print current working directory

        if isinstance(total_costs, str):
            logging.info("Getting tasks durations from file")
            with open(total_costs, "rb") as f:
                total_costs = pickle.load(f)
            logging.info("Task durations loaded")
        else:
            logging.info("Task durations provided")

        if isinstance(read_write_costs, str):
            logging.info("Getting read costs from file")
            with open(read_write_costs, "rb") as f:
                read_write_costs = pickle.load(f)
            logging.info("Read costs loaded")
        else:
            logging.info("Read costs provided")

        read_costs = create_read_costs_matrix(task_graph, read_write_costs)

        for task in task_graph.tasks:
            task.read_costs = read_costs[task.operators[0].task_id]
            task.total_cost = total_costs[task.operators[0].task_id]

        for task in task_graph.tasks:
            task.total_cost = total_costs[task.operators[0].task_id]

        logging.info("Starting IP")
        fused_edges, operator_edges = task_graph.get_fusion_variables()
        logging.info("IP complete")
        # Serialize
        logging.info("Serializing IP variables")
        with open(f"../include/{dag_id}.pkl", "wb") as f:
            pickle.dump((fused_edges, operator_edges), f)
        logging.info("IP variables serialized")


    # task_ids to operators
    task_id_to_operator = {}
    for task in task_graph.tasks:
        task_id_to_operator[task.operators[0].task_id] = task.operators[0]
    
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
    task_graph = TaskGraph(tasks, scheduling_cost, dag.dag_id)
        
    return task_graph

def create_optimized_dag(dag: DAG, total_costs: dict, read_costs: dict, scheduling_cost: int) -> DAG:
    logging.info('Beginning creation of optimized DAG')
    new_dag = DAG(dag_id=dag._dag_id + '_optimized')

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

    task_graph = build_graph_from_dag(dag, total_costs, read_costs, scheduling_cost)
    task_graph.perform_complete_fusion()
    build_dag_from_graph(task_graph, new_dag)
    return new_dag