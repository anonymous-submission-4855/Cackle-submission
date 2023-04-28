import argparse
import numpy as np
import pandas as pd
import random
import matplotlib.pyplot as plt
import ast
import gzip
from scipy.stats import cosine
from collections import defaultdict

class Task:

    def __init__(self, stage, task_id, time):
        self.stage = stage
        self.time = time
        self.task_id = task_id

    def set_end_time(self, start_time):
        self.end_time = start_time+self.time

    def complete(self, curr_time):
        self.stage.complete_task(self.task_id, curr_time)


class Stage:

    def __init__(self, query, stage_id, tasks, deps):
        self.remaining_tasks = {}
        self.runnable = len(deps) == 0
        self.stage_id = stage_id
        for task_id in range(len(tasks)):
            self.remaining_tasks[task_id] = Task(self, task_id, tasks[task_id])
        self.tasks_to_start = list(self.remaining_tasks.values())
        random.shuffle(self.tasks_to_start)
        self.parent_stages = []
        self.remaining_child_stages = deps.copy()

    def set_parent_stage(self, parent_stage):
        self.parent_stages.append(parent_stage)

    def complete_task(self, task_id, curr_time):
        del self.remaining_tasks[task_id]
        if len(self.remaining_tasks) == 0:
            for stage in self.parent_stages:
                stage.complete_dependent(self.stage_id, curr_time)

    def complete_dependent(self, dep_id, curr_time):
        self.remaining_child_stages.remove(dep_id)
        if len(self.remaining_child_stages) == 0:
            self.runnable=True

class Query:

    def __init__(self, query_data, start_time):
        self.stage_map = {}
        final_stage = len(query_data)-1
        self.start_time = start_time
        self.stages = {}
        self.complete = False
        for stage_id in query_data:
            self.stages[stage_id] = Stage(self, stage_id, query_data[stage_id][3], query_data[stage_id][2])
            for dep in query_data[stage_id][2]:
                self.stages[dep].set_parent_stage(self.stages[stage_id])
        self.stages[final_stage].set_parent_stage(self)

    def complete_dependent(self, dep_id, curr_time):
        self.end_time = curr_time
        self.complete = True
            
    def get_next_tasks(self, num_tasks):
        del_stages = []
        tasks = []
        for stage_idx in self.stages:
            stage = self.stages[stage_idx]
            if len(stage.remaining_tasks) == 0:
                del_stages.append(stage)
            elif stage.runnable and len(stage.tasks_to_start) > 0:
                remaining_tasks = num_tasks-len(tasks)
                new_tasks = stage.tasks_to_start[:min(len(stage.tasks_to_start), remaining_tasks)]
                tasks+=new_tasks
                stage.tasks_to_start = stage.tasks_to_start[len(new_tasks):]
                if len(tasks) == num_tasks:
                    break
        for stage in del_stages:
            del self.stages[stage.stage_id]
        return tasks

def get_query_data():
    query_data = defaultdict(lambda: {})

    with open("query_data") as qdata_file:
        for line in qdata_file:
            line = line.strip()
            split_line = line.split(",")
            query = int(split_line[0])
            stage = int(split_line[1])
            stage_start = int(split_line[2])
            stage_output_size = int(split_line[3])
            deps = [] if len(split_line[4]) == 0 else [int(x) for x in split_line[4].split(":")]
            task_runtimes = [int(x) for x in split_line[5:]]
            query_data[query][stage] = [stage_start, stage_output_size, deps, task_runtimes]
    return query_data

def get_latency_cost(args, percentile):
    start_buff = 600 
    query_data = get_query_data()

    query_start_times = defaultdict(lambda: [])
    last_query_time = 0
    with gzip.open(args.query_file, "rt") as infile:
        for line in infile:
            line = line.strip().split(",")
            start_time = int(line[0])+start_buff
            query = int(line[1][1:])
            query_start_times[start_time].append(query)
            last_query_time = max(start_time, last_query_time)
    running_queries = []
    executors = []
    executor_count = []
    if args.strategy == "fixed":
        executors = [None for x in range(int(args.strategy_params))]
    if args.strategy == "given":
        with open(args.strategy_params) as inf:
            for line in inf:
                curr_executors = int(line.strip())+1
                executor_count.append(curr_executors)

    curr_time = 0
    task_history = []
    executor_hist = []
    query_runtimes = []
    while(curr_time <= max(last_query_time, len(executor_count)-5) or len(running_queries) > 0):
        if curr_time % 1000 == 0:
            pass
            print(curr_time, len(executors))
        #Add New Queries
        if curr_time in query_start_times:
            running_queries+=[Query(query_data[x], curr_time) for x in query_start_times[curr_time]]
        max_tasks = 0
        for executor_idx in range(len(executors)):
            executor = executors[executor_idx]
            if executor is not None and executor.end_time == curr_time:
                executors[executor_idx] = None
                executor.complete(curr_time)
                max_tasks+=1
            elif executor is None:
                max_tasks+=1

        #Adjust Executors
        needed_curr_executors = len(executors)
        if args.strategy == "given":
            needed_curr_executors = executor_count[curr_time]
        if len(executors) < needed_curr_executors:
            max_tasks+=needed_curr_executors-len(executors)
            executors+=[None for x in range(needed_curr_executors-len(executors))]

        elif len(executors) > needed_curr_executors:
            for x in range(len(executors)-needed_curr_executors):
                if None in executors:
                    executors.remove(None)
                    max_tasks-=1
                else:
                    print(curr_time, last_query_time)

        tasks_to_assign = []
        remaining_tasks = max_tasks
        for query in running_queries:
            query_tasks = query.get_next_tasks(remaining_tasks)
            remaining_tasks-=len(query_tasks)
            tasks_to_assign+=query_tasks
            if remaining_tasks == 0:
                break

        #Add New Tasks
        num_tasks = 0
        task_idx = 0
        for executor_idx in range(len(executors)):
            executor = executors[executor_idx]
            if executor is None:
                if len(tasks_to_assign) > task_idx:
                    next_task = tasks_to_assign[task_idx]
                    next_task.set_end_time(curr_time)
                    executors[executor_idx] = next_task
                    num_tasks+=1
                    task_idx+=1
            else:
                num_tasks+=1
        #Remove Completed Queries and Collect Data
        assert task_idx == len(tasks_to_assign)
        last_completed_query = 0
        for query_idx in range(len(running_queries)):
            query = running_queries[query_idx]
            if query.complete:
                last_completed_query+=1
                query_runtimes.append(query.end_time-query.start_time)
            else:
                break
        running_queries = running_queries[last_completed_query:]

        #update_histories
        task_history.append(num_tasks)
        executor_hist.append(len(executors))

        curr_time+=1
    if False:
        plt.clf()
        plt.xlim(1775, 1785)
        plt.plot(range(len(executor_hist)), task_history, label="running tasks")
        plt.plot(range(len(executor_hist)), executor_hist, label="available executors")
        plt.legend()
        plt.show()
    query_runtimes.sort()
    query_runtimes = np.array(query_runtimes)
    y_vals = np.array(range(len(query_runtimes)))/len(query_runtimes)
    #plt.plot(query_runtimes,y_vals)
    #plt.show()
    vm_cost_s = 0.0400/(60*60)
    executor_cost = np.array(executor_hist).sum()
    exec_cost = executor_cost*vm_cost_s
    percentile_latency =np.percentile(query_runtimes, percentile)
    return exec_cost, percentile_latency

def main(args):
    percentile = 95.0
    costs = []
    latencies = []
    fixed_results = []
    ax = plt.gca()
    for x in range(100, 451, 5):
        print(x)
        args.strategy="Work-Delaying Fixed"
        args.strategy="fixed"
        args.strategy_params=x
        cost, latency = get_latency_cost(args, percentile)
        latency = int(latency)
        costs.append(cost)
        latencies.append(latency)
        if x % 50==0:
            ax.annotate("{}".format(x), xy=(latency, cost), xytext=(latency+0.5, cost+10), arrowprops=dict(facecolor='black', width=1, headlength=8, headwidth=7, shrink=0.05))
    plt.scatter(latencies, costs, label="Work-Delaying Fixed")
    plt.xlim(17.5, 32)
    plt.ylim(40, 245)
    plt.xlabel("95th percentile Query Latency(s)")
    plt.ylabel("Cost($)")
    args.strategy="given"
    args.strategy_params="perfect.prov.all_vm"
    plt.scatter([latency], [56.223], label="Cackle Oracle", marker="+")
    plt.scatter([latency], [84.526], label="Cackle Oracle Without Elastic Pool", marker="x")
    plt.scatter([latency], [63.818], label="Cackle Cost-Based Dynamic Strategy", marker="*")
    plt.legend()
    curr_size = plt.gcf().get_size_inches()
    plt.gcf().set_size_inches((curr_size[0], curr_size[1]*(3/4)))
    plt.gcf().tight_layout()
    plt.savefig("cost_latency_plot.pdf")


if __name__ == "__main__":
    random.seed(198243)
    np.random.seed(19823197)
    parser = argparse.ArgumentParser()
    parser.add_argument("--query_file", "-q")
    parser.add_argument("--strategy", "-st", choices=["fixed", "given", "spark"], default="fixed")
    parser.add_argument("--strategy_params", "-sp", default=200)
    args = parser.parse_args()
    main(args)
