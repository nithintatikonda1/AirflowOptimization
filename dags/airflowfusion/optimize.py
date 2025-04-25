import gurobipy as gp
from gurobipy import GRB
from collections import defaultdict
from pprint import pprint
import logging
import pickle
import os

def optimize_integer_program(predecessors, fusion_possible, c, t, r):
    """
    Takes two task nodes and combines them into one tasknode, reassigning the upstream and downstream tasks

    Args:
        predecessors (dict): predecessors[i][j] = 1 if task i is a predecessor of task j
        fusion_possible (dict): fusion_possible[i][j] = 1 if task i is fusable with task j
        c (int) : Scheduling cost
        t (dict) : map of task id to total execution time.
        r (dict) : r[i][j] = eliminatable read cost in task j if task i and j are fused

    Returns:
        TaskNode: The newly created fused Task Node
    """    

    tasks = list(predecessors.keys())
    predecessors = predecessors.copy()
    fusion_possible = fusion_possible.copy()
    t = t.copy()
    r = r.copy()

    # Add a dummy starting node and a dummy ending node
    dummy_starting_task = 'dummy_starting_task'
    dummy_ending_task = 'dummy_ending_task'
    tasks.append(dummy_starting_task)
    tasks.append(dummy_ending_task)

    # Add predecessor relationship for dummy tasks
    for task in tasks:
        predecessors[dummy_starting_task][task] = 1
        predecessors[task][dummy_ending_task] = 1

    # Starting and ending tasks should not be fusable
    for task in tasks:
        fusion_possible[dummy_starting_task][task] = 0
        fusion_possible[task][dummy_starting_task] = 0
        fusion_possible[task][dummy_ending_task] = 0
        fusion_possible[dummy_ending_task][task] = 0

    # Starting and ending tasks have execution and read times of 0
    t[dummy_starting_task] = 0
    t[dummy_ending_task] = 0
    r[dummy_starting_task] = {}
    r[dummy_ending_task] = {}
    for task in tasks:
        r[task][dummy_ending_task] = 0
        r[task][dummy_starting_task] = 0
        r[dummy_starting_task][task] = 0
        r[dummy_ending_task][task] = 0
        

    # Task cannot be its own predecessor
    for task in tasks:
        predecessors[task][task] = 0


    m = gp.Model("model")

    # Utility constants
    M = 100000

    # Variables

    d = {}
    for task in tasks:
        d[task] = m.addMVar(shape=1, vtype=GRB.CONTINUOUS, name="d_" + task)
        m.addConstr(d[task] >= 0, "dNonNegative_" + task)

    e = defaultdict(dict)
    f = defaultdict(dict)
    x = defaultdict(dict)
    y = defaultdict(dict)
    g = defaultdict(dict)
    h = defaultdict(dict)
    for task1 in tasks:
        for task2 in tasks:
            if task1 != task2:
                e[task1][task2] = m.addMVar(shape=1, vtype=GRB.BINARY, name="e_" + task1 + "_" + task2)
                f[task1][task2] = m.addMVar(shape=1, vtype=GRB.BINARY, name="f_" + task1 + "_" + task2)
                x[task1][task2] = m.addMVar(shape=1, vtype=GRB.BINARY, name="x_" + task1 + "_" + task2)
                y[task1][task2] = m.addMVar(shape=1, vtype=GRB.BINARY, name="y_" + task1 + "_" + task2)
                g[task1][task2] = m.addMVar(shape=1, vtype=GRB.CONTINUOUS, name="g_" + task1 + "_" + task2)
                m.addConstr(g[task1][task2] >= 0, "gNonNegative_" + task1 + "_" + task2)
                h[task1][task2] = m.addMVar(shape=1, vtype=GRB.CONTINUOUS, name="h_" + task1 + "_" + task2)
                m.addConstr(h[task1][task2] >= 0, "hNonNegative_" + task1 + "_" + task2)

    a = defaultdict(lambda : defaultdict(dict))
    b = defaultdict(lambda : defaultdict(dict))
    for task1 in tasks:
        for task2 in tasks:
            for task3 in tasks:
                if task1 != task2 and task2 != task3 and task1 != task3:
                    a[task1][task2][task3] = m.addMVar(shape=1, vtype=GRB.BINARY, name="a_" + task1 + "_" + task2 + "_" + task3)
                    b[task1][task2][task3] = m.addMVar(shape=1, vtype=GRB.BINARY, name="b_" + task1 + "_" + task2 + "_" + task3)
                    

    # Constraints

    for task1 in tasks:
        for task2 in tasks:
            if task1 != task2:
                # 1. Enforce d_i being the longest path for operator dependencies
                m.addConstr(d[task1] >= g[task1][task2], "longestPath1_" + task1 + "_" + task2)

                # 2. Enforce g_ij for the previous constraint
                m.addConstr(g[task1][task2] <= M * e[task1][task2], "gConstr1_" + task1 + "_" + task2)
                m.addConstr(g[task1][task2] - c - t[task2] - d[task2] <= M - M * e[task1][task2], "gConstr2_" + task1 + "_" + task2)
                m.addConstr(-g[task1][task2] + c + t[task2] + d[task2] <= M - M * e[task1][task2], "gConstr3_" + task1 + "_" + task2)

                # 3. Enforce d_i being the longest path for fusion dependencies
                m.addConstr(d[task1] >= h[task1][task2], "longestPath2_" + task1 + "_" + task2)
                
                # 4. Enforce h_ij for the previous constraint
                m.addConstr(h[task1][task2] <= M * f[task1][task2], "hConstr1_" + task1 + "_" + task2)
                m.addConstr(h[task1][task2] - t[task2] - d[task2] + gp.quicksum(r[task3][task2] * x[task3][task2] for task3 in tasks if task2 != task3) <= M - M * f[task1][task2], "hConstr2_" + task1 + "_" + task2)
                m.addConstr(-h[task1][task2] + t[task2] + d[task2] - gp.quicksum(r[task3][task2] * x[task3][task2] for task3 in tasks if task2 != task3) <= M - M * f[task1][task2], "hConstr3_" + task1 + "_" + task2)
                
                # 5. There can only be one incoming or one outgoing fused edge
                m.addConstr(gp.quicksum(e[task1][task3] for task3 in tasks if task1 != task3) <= M - M * f[task1][task2], "outConstr1_" + task1 + "_" + task2)
                m.addConstr(gp.quicksum(e[task3][task1] for task3 in tasks if task1 != task3) <= M - M * f[task2][task1], "inConstr1_" + task1 + "_" + task2)

                # 6. Enforce x_ij whcih checks for predecessor relationship within fused component
                m.addConstr(x[task1][task2] >= f[task1][task2], "xConstr1_" + task1 + "_" + task2)
                m.addConstr(x[task1][task2] <= f[task1][task2] + gp.quicksum(b[task1][task3][task2] for task3 in tasks if task1 != task3 and task2 != task3), "xConstr3_" + task1 + "_" + task2)
                if task1 < task2:
                    m.addConstr(x[task1][task2] + x[task2][task1] <= 1, "xConstrFix_" + task1 + "_" + task2)

                # 8. Enforce predecessor constraints from original workflow.
                m.addConstr(y[task1][task2] >= f[task1][task2] + e[task1][task2], "yConstr1_" + task1 + "_" + task2)
                m.addConstr(y[task1][task2] <= f[task1][task2] + e[task1][task2] + gp.quicksum(a[task1][task3][task2] for task3 in tasks if task1 != task3 and task2 != task3), "yConstr3_" + task1 + "_" + task2)
                m.addConstr(y[task1][task2] >= predecessors[task1][task2], "yConstr4_" + task1 + "_" + task2)
                if task1 < task2:
                    m.addConstr(y[task1][task2] + y[task2][task1] <= 1, "yConstrFix_" + task1 + "_" + task2)

                # 12. Set tasks as unfusable
                if not fusion_possible[task1][task2]:
                    m.addConstr(f[task1][task2] == 0, "unfusable_" + task1 + "_" + task2)

    for task in tasks:
        # 5. There can only be one incoming or one outgoing fused edge
        m.addConstr(gp.quicksum(f[task][task2] for task2 in tasks if task != task2) <= 1, "outConstr2_" + task)
        m.addConstr(gp.quicksum(f[task2][task] for task2 in tasks if task != task2) <= 1, "inConstr2_" + task)

        # 10. All tasks have path to end
        if task != dummy_ending_task:
            m.addConstr(y[task][dummy_ending_task] == 1, "pathToEnd_" + task)

        # 11. All tasks have path to start
        if task != dummy_starting_task:
            m.addConstr(y[dummy_starting_task][task] == 1, "pathToStart_" + task)


    for task1 in tasks:
        for task2 in tasks:
            for task3 in tasks:
                if task1 != task2 and task2 != task3 and task1 != task3:
                    # 6. Enforce x_ij whcih checks for predecessor relationship within fused component
                    m.addConstr(x[task1][task2] >= b[task1][task3][task2], "xConstr2_" + task1 + "_" + task3 + "_" + task2)

                    # 7. Enforce b_ikj for the previous constraint
                    m.addConstr(b[task1][task3][task2] <= f[task1][task3], "bConstr1_" + task1 + "_" + task3 + "_" + task2)
                    m.addConstr(b[task1][task3][task2] <= x[task3][task2], "bConstr2_" + task1 + "_" + task3 + "_" + task2)
                    m.addConstr(b[task1][task3][task2] >= f[task1][task3] + x[task3][task2] - 1, "bConstr3_" + task1 + "_" + task3 + "_" + task2)

                    # 8. Enforce predecessor constraints from original workflow.
                    m.addConstr(y[task1][task2] >= a[task1][task3][task2], "yConstr2_" + task1 + "_" + task3 + "_" + task2)

                    # 9. Enforce a_ikj for the previous constraint
                    m.addConstr(a[task1][task3][task2] <= f[task1][task3] + e[task1][task3], "aConstr1_" + task1 + "_" + task3 + "_" + task2)
                    m.addConstr(a[task1][task3][task2] <= y[task3][task2], "aConstr2_" + task1 + "_" + task3 + "_" + task2)
                    m.addConstr(a[task1][task3][task2] >= f[task1][task3] + e[task1][task3] + y[task3][task2] - 1, "aConstr3_" + task1 + "_" + task3 + "_" + task2)
    
    # Objective
    m.setObjective(d[dummy_starting_task], GRB.MINIMIZE)

    # Set time limit
    m.setParam(GRB.Param.TimeLimit, 1200)

    # Solve 
    m.optimize()

    # Return discovered edges
    fused_edges = []
    operator_edges = []
    for from_task in e.keys():
        if from_task == dummy_starting_task or from_task == dummy_ending_task:
            continue
        for to_task in e[from_task].keys():
            if to_task == dummy_starting_task or to_task == dummy_ending_task:
                continue
            if e[from_task][to_task].X >= 0.5:
                operator_edges.append((from_task, to_task))
            if f[from_task][to_task].X >= 0.5:
                fused_edges.append((from_task, to_task))

    """
    print('fused edges: ', fused_edges)
    print('operator edges: ', operator_edges)

    # Print the values of all IP variables having to do with the task ids 'vectorize_summaries' and 'split'
    print('y[vectorize_summaries][split]: ', y['vectorize_summaries']['split'].X)
    print('y[split][vectorize_summaries]: ', y['split']['vectorize_summaries'].X)

    print('e[vectorize_summaries][split]: ', e['vectorize_summaries']['split'].X)
    print('e[split][vectorize_summaries]: ', e['split']['vectorize_summaries'].X)

    print('f[vectorize_summaries][split]: ', f['vectorize_summaries']['split'].X)
    print('f[split][vectorize_summaries]: ', f['split']['vectorize_summaries'].X)
    exit(0)
    """
    return fused_edges, operator_edges
