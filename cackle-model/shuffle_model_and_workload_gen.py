import argparse
import numpy as np
import pandas as pd
import random
import matplotlib
#matplotlib.use("Qt5Agg")
import matplotlib.pyplot as plt
import ast
import sys
import gzip
import os
import heapq
from scipy.stats import cosine
from collections import defaultdict

s3_write_cost = 0.005*2/1000
s3_read_cost = 0.0004/1000
cache_node_cost_per_second = 0.0800/(60*60)
cache_size_per_node_b = 4*1024*1024*1024
new_node_start_time = 180
minimum_billing_time = 60
config_output_folder = "configs"
start_buff = 600
 
def eprint(*args, **kwargs):
    pass
    #print(*args, file=sys.stderr, **kwargs)


class CacheNode:
    cache_node_id = None
    remaining_capacity = cache_size_per_node_b
    shuffles = None
    decommissioning = False
    start_time = None

    def __init__(self, cache_node_id, start_time):
        self.cache_node_id = cache_node_id
        self.shuffles = defaultdict(lambda : [0, None])
        self.start_time = start_time

    #returns ammount written
    def try_to_write(self, stage, out_partition, size, can_evict):
        if self.decommissioning:
            print("tried to write to decommissioning node", self)
            exit(1)
        num_writes = 0
        max_write = min(self.remaining_capacity, size)
        while can_evict and max_write < size:
            largest_partition_size = 0
            largest_shuffle = None
            largest_partition_key = None
            for shuffle_key in self.shuffles:
                shuffle = self.shuffles[shuffle_key]
                if shuffle[1].partition_size > 2*stage.partition_size:
                    largest_shuffle = shuffle[1]
                    largest_partition_size = shuffle[1].partition_size
                    largest_partition_key = shuffle_key
            if largest_shuffle is None:
                break
            num_writes += 1
            largest_shuffle.consumer_stage.num_reads_to_execute+=1
            self.drop_shuffle_partition(largest_shuffle, largest_partition_key)
            max_write = min(self.remaining_capacity, size)

        shuffle_key = (stage.name[0],stage.name[1], out_partition)
        if max_write > 0:
            self.shuffles[shuffle_key][0]+=max_write
            self.shuffles[shuffle_key][1] = stage
            self.remaining_capacity-=max_write
            stage.add_cache_node(self, shuffle_key)

        return (max_write, num_writes)

    def drop_shuffle_partition(self, stage, partition):
        freed_capacity = self.shuffles.pop(partition)
        self.remaining_capacity += freed_capacity[0]
        return freed_capacity[0]

    def __repr__(self):
        return "CacheNode:{} {}".format(self.cache_node_id, -self.remaining_capacity+cache_size_per_node_b)

    def __lt__(self, other):
        return self.cache_node_id < other.cache_node_id


class Cache:
    current_cache_nodes = []
    decommissioning_cache_nodes = []
    starting_cache_nodes = []
    current_kb_in_flight = 0
    total_shuffle_size = 0
    total_starting_nodes = 0
    desired_cache_nodes = 0
    next_cache_node_id = 0

    def get_total_shuffle_size(self):
        return self.total_shuffle_size

    def get_current_kb_in_flight(self):
        return self.current_kb_in_flight

    def get_current_cache_capacity(self):
        return len(self.current_cache_nodes)*cache_size_per_node_b

    def set_desired_num_cache_nodes(self, desired_num_cache_nodes, curr_time):
        desired_num_cache_nodes = max(desired_num_cache_nodes, args.min_cache_nodes)

        if len(self.current_cache_nodes)+self.total_starting_nodes < desired_num_cache_nodes:
            cache_nodes_to_add = desired_num_cache_nodes-(len(self.current_cache_nodes)+self.total_starting_nodes)
            if len(self.decommissioning_cache_nodes) > 0:
                #print(self.decommissioning_cache_nodes)
                num_dcn_to_recommission = min(len(self.decommissioning_cache_nodes), cache_nodes_to_add)
                for node in self.decommissioning_cache_nodes[:num_dcn_to_recommission]:
                    if not node.decommissioning:
                        print("node is already set to active", node)
                    node.decommissioning=False
                #print(self.current_cache_nodes, self.decommissioning_cache_nodes)
                self.current_cache_nodes+=self.decommissioning_cache_nodes[:num_dcn_to_recommission]
                self.decommissioning_cache_nodes = self.decommissioning_cache_nodes[num_dcn_to_recommission:]
                cache_nodes_to_add -= num_dcn_to_recommission
                #print(self.current_cache_nodes, self.decommissioning_cache_nodes)
            self.starting_cache_nodes.append([cache_nodes_to_add, new_node_start_time])
            self.total_starting_nodes+=cache_nodes_to_add

        elif len(self.current_cache_nodes)+self.total_starting_nodes > desired_num_cache_nodes:
            #print("before", desired_num_cache_nodes, self.get_total_cache_nodes(), self.total_starting_nodes)
            #print(self.current_cache_nodes)
            #print(self.decommissioning_cache_nodes)
            #TODO: decomission nodes with least cache
            num_to_remove = (len(self.current_cache_nodes)+self.total_starting_nodes)-desired_num_cache_nodes
            num_to_remove = max(num_to_remove, 0)
            if num_to_remove > 0:
                self.current_cache_nodes.sort(key=lambda x: x.remaining_capacity)
                nodes_with_data = []
                for node in self.current_cache_nodes[-num_to_remove:]:
                    if node.decommissioning:
                        print("node should not already be decommissioned", node)
                    if node.remaining_capacity < cache_size_per_node_b or curr_time-node.start_time < minimum_billing_time:
                        nodes_with_data.append(node)
                        node.decommissioning = True
                self.decommissioning_cache_nodes+=nodes_with_data
                self.current_cache_nodes = self.current_cache_nodes[:-num_to_remove-1]
            #print("after", desired_num_cache_nodes, self.get_total_cache_nodes(), self.total_starting_nodes)
            #print(self.current_cache_nodes)
            #print(self.decommissioning_cache_nodes)

    def increment_time(self, curr_time, time_jump):
        for entry in self.starting_cache_nodes:
            entry[1] -= time_jump
        drop_decomm_cache_nodes_idx = []
        for node_idx in range(len(self.decommissioning_cache_nodes)):
            node = self.decommissioning_cache_nodes[node_idx]
            if node.remaining_capacity == cache_size_per_node_b and curr_time-node.start_time >= minimum_billing_time:
                drop_decomm_cache_nodes_idx.append(node_idx)
        for idx in drop_decomm_cache_nodes_idx[::-1]:
            del self.decommissioning_cache_nodes[idx]
        if len(self.starting_cache_nodes) >= 1 and self.starting_cache_nodes[0][1] == 0:
            entry = self.starting_cache_nodes[0]

            for x in range(entry[0]):
                self.current_cache_nodes.append(CacheNode(self.next_cache_node_id, curr_time))
                self.next_cache_node_id+=1
            self.total_starting_nodes -= entry[0]
            self.starting_cache_nodes = self.starting_cache_nodes[1:]


    def get_total_cache_nodes(self):
        return len(self.current_cache_nodes)+self.total_starting_nodes+len(self.decommissioning_cache_nodes)

    def get_billable_cache_nodes(self):
        return len(self.current_cache_nodes)+len(self.decommissioning_cache_nodes)


    def attempt_write_to_cache(self, stage, time):
        current_write = stage.write_size*stage.num_tasks
        self.total_shuffle_size += current_write
        self.current_kb_in_flight += current_write
        amount_written_to_cache = 0

        if args.cache_type=="perfect" and stage.size_written_to_cache < current_write:
            num_extra_cache_nodes_needed = (current_write-amount_written_to_cache)//cache_size_per_node_b
            if current_write-amount_written_to_cache % cache_size_per_node_b > 0:
                num_extra_cache_nodes_needed += 1
            time_cache_needed = stage.consumer_stage.end_time - stage.start_time
            cost_of_extra_cache = num_extra_cache_nodes_needed*(max(time_cache_needed,minimum_billing_time))*cache_node_cost_per_second
            remaining_reads = int((1-(amount_written_to_cache//current_write))*(stage.num_tasks*stage.consumer_stage.num_tasks))
            amount_written_to_cache = current_write
            cost_of_s3_left = stage.num_tasks*s3_write_cost+remaining_reads*s3_write_cost
            if (cost_of_s3_left > cost_of_extra_cache):
                for x in range(num_extra_cache_nodes_needed):
                    self.current_cache_nodes.append(CacheNode(self.next_cache_node_id, time))
                    self.next_cache_node_id+=1
                max_needed=self.current_cache_nodes
                #need to look back and see if we can keep cache around
                '''
                startup_window = available_cache[max(0, time-new_node_start_time):time+1].copy()
                startup_window[startup_window > max_needed*cache_size_per_node_b] = max_needed*cache_size_per_node_b
                loc_max_from_end = startup_window[::-1].argmax()
                max_from_last_seconds = startup_window[-1*loc_max_from_end-1]/cache_size_per_node_b
                if max_from_last_seconds > current_cache_nodes-num_extra_cache_nodes_needed:
                    nodes_to_keep = startup_window*-1+max_needed*cache_size_per_node_b
                    nodes_to_keep[:-1*loc_max_from_end-1] = 0
                    available_cache[max(0, time-new_node_start_time):time+1] += nodes_to_keep
                    total_cache_nodes[max(0, time-new_node_start_time):time] += (nodes_to_keep//cache_size_per_node_b)[:-1]
                    num_extra_cache_nodes_needed = int(max_needed-max_from_last_seconds)
                total_cache_nodes[max(0, time-new_node_start_time):time]+=num_extra_cache_nodes_needed
                '''
                for x in range(num_extra_cache_nodes_needed):
                    self.current_cache_nodes.append(CacheNode(self.next_cache_node_id, time))
                    self.next_cache_node_id+=1

            else:
                pass
                #print("SOMETHING WRONG")
                #print(cost_of_s3_left, cost_of_extra_cache)
                #exit(1)


        num_writes = 0

        if stage.consumer_stage:
            for consumer_partition in range(stage.consumer_stage.num_tasks):
                saved_seed = random.seed()
                random.seed(hash((stage, consumer_partition)))
                write_cache_nodes = None
                if args.cache_type == "perfect":
                    write_cache_nodes = random.sample(self.current_cache_nodes, k=len(self.current_cache_nodes))
                else:
                    write_cache_nodes = random.sample(self.current_cache_nodes, k=min(5, len(self.current_cache_nodes)))
                random.seed(saved_seed)
                total_write_size = max(current_write//stage.consumer_stage.num_tasks, 1)

                if consumer_partition == 0:
                    total_write_size += current_write % stage.consumer_stage.num_tasks
                total_in_cache = 0
                node_count = 0
                for node in write_cache_nodes:
                    if total_in_cache == total_write_size:
                        break
                    (amount_written, curr_writes)= node.try_to_write(stage, consumer_partition, total_write_size-total_in_cache, True if args.enable_eviction and node_count >1 else False)
                    num_writes+= curr_writes
                    total_in_cache += amount_written
                    node_count+=1
                stage.size_written_to_cache+=total_in_cache
                read_tasks = stage.consumer_stage.num_tasks
                remaining_reads = read_tasks-int(read_tasks*(total_in_cache/total_write_size))

                stage.consumer_stage.num_reads_to_execute+=remaining_reads


        if stage.size_written_to_cache < current_write:
            return (stage.num_tasks+num_writes, 0)
        else:
            return (num_writes, 0)

    def complete_stage(self, stage):
        num_partitions_read_from_s3 = 0
        for producer_stage in stage.producer_stages:
            size_producer_wrote_to_cache = producer_stage.size_written_to_cache
            total_producer_size = producer_stage.write_size*producer_stage.num_tasks
            self.current_kb_in_flight -= total_producer_size
            for partition, cache_node_list in producer_stage.get_cache_nodes().items():
                for cache_node in cache_node_list:
                    cache_node.drop_shuffle_partition(producer_stage, partition)
                    if cache_node.decommissioning and cache_node.remaining_capacity == cache_size_per_node_b:
                        self.decommissioning_cache_nodes.remove(cache_node)
            producer_stage.clear_cache_nodes()
            num_partitions_read_from_s3 += producer_stage.num_reads_to_execute
        return num_partitions_read_from_s3


class Stage:
    num_reads_to_execute = 0
    size_written_to_cache = 0
    total_partitions = 0
    partition_size = 0
    consumer_stage = None
    used_cache_nodes = None

    def __init__(self, name, start_time, end_time, num_tasks, num_reads, read_size, write_size, producer_stages, tasks):
        self.name = name
        self.start_time = start_time
        self.end_time = end_time
        self.num_tasks = num_tasks
        self.num_reads = num_reads
        self.read_size = read_size
        self.write_size = write_size
        self.producer_stages = producer_stages
        self.used_cache_nodes = defaultdict(lambda: [])
        self.tasks = tasks

    def __repr__(self):
        #return "{}-{} tasks:{} reads:{} read_size:{} write_size:{}\n".format(self.start_time, self.end_time, self.num_tasks, self.num_reads, self.read_size, self.write_size)
        return "{};{};{};{};{};{};{};{};{};{}".format("{}-{}".format(self.name[0], self.name[1]),self.start_time,self.end_time,self.num_tasks,self.num_reads,self.read_size,self.write_size,self.total_partitions,self.partition_size,"["+",".join(["{}-{}".format(x.name[0], x.name[1]) for x in self.producer_stages])+"]")

    def get_cache_heap_entry(self):
        return (self.partition_size, self)

    def add_cache_node(self, cacheNode, partition):
        self.used_cache_nodes[partition].append(cacheNode)

    def get_cache_nodes(self):
        return self.used_cache_nodes

    def clear_cache_nodes(self):
        self.used_cache_nodes.clear()

    def __lt__(self, other):
        return self.name < other.name



def main(args):
    min_billing_time=args.min_billing_time
    new_node_start_time=args.cache_startup_time
    arg_dict = vars(args).copy()
    workload_arg_list = list(arg_dict)[:-10]
    filename_base = ",".join([str(arg_dict[x]) for x in workload_arg_list])
    total_workload_filename = filename = "cached_workloads/{}.workload.gz".format(filename_base)
    task_filename = "cached_workloads/{}.task.gz".format(filename_base)
    stages = []
    if os.path.exists(total_workload_filename) and (not args.generate_only):
        with gzip.open(total_workload_filename, "rt") as twf:
            header_count = 0
            total_header = 3
            num_stages = 0
            stage_map = {}
            curr_stage_num = 0
            for line in twf:
                if header_count < total_header:
                    header_count+=1
                else:
                    sl = line.split(";")
                    name = sl[0]
                    name = tuple([int(x) for x in name.split("-")])
                    start_time = int(sl[1])
                    end_time = int(sl[2])
                    num_tasks = int(sl[3])
                    num_reads = int(sl[4])
                    read_size = int(sl[5])
                    write_size = int(sl[6])
                    producer_stages = []
                    if len(sl[9].strip()[1:-1]) > 0:
                        producer_stages = [stage_map[tuple([int(y) for y in x.split("-")])] for x in sl[9].strip()[1:-1].split(",")]
                    curr_stage = Stage(name, start_time, end_time, num_tasks, num_reads, read_size, write_size, producer_stages, None)
                    curr_stage.total_partitions = int(sl[7])
                    curr_stage.partition_size = float(sl[8])
                    for stage in producer_stages:
                        stage.consumer_stage = curr_stage
                    stage_map[name] =  curr_stage
                    stages.append(curr_stage)
                    curr_stage_num += 1
    else:

        if args.smoothness_experiment:
            args.queries = int(args.queries // (args.max_shuffle_size_per_producer/512))
            args.reduction = args.max_shuffle_size_per_producer/512
            args.max_shuffle_size_per_producer = 512*1024
        eprint(args.queries)
        eprint(args.max_shuffle_size_per_producer)
        arg_dict = vars(args).copy()
        query_nums = list(range(1, 23))
        all_queries = []
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
        
        for query in range(args.queries):
            if query%1000 == 0:
                eprint(query, args.queries, query/args.queries*100)
                pass
            start_time = 0
            if random.uniform(0, 1) < args.baseline_load:
                start_time = random.randint(0, args.length)
            else:
                normalized_start = (cosine.rvs()+np.pi)/(2*np.pi)
                period_num = random.randint(0, args.length//args.period-1)
                start_time = period_num*args.period+int(normalized_start*args.period)
            which_query = random.choice(query_nums)
            all_queries.append((start_time, which_query))
            start_time+=start_buff

            if args.simulated_queries:
                num_stages = random.randint(1, args.max_joins_per_query)
                input_stage = None
                for stage in range(num_stages):
                    consumers = random.randint(args.min_num_workers, args.max_num_workers)
                    consumer_time = random.randint(1, args.max_task_runtime)
                    shuffle_size = int(np.exp(random.uniform(1, np.log(args.max_shuffle_size_per_producer))))
                    shuffle_size = max(int(shuffle_size*args.reduction), 1)
                    num_reads = 0
                    read_size = 0
                    if input_stage is not None:
                        num_reads = input_stage.num_tasks * consumers
                        read_size = input_stage.num_tasks * input_stage.write_size
                        input_stage.total_partitions = num_reads
                        input_stage.partition_size = num_reads/shuffle_size
                    write_size = shuffle_size
                    curr_stage = Stage((query,stage),start_time, start_time+consumer_time, consumers, num_reads, read_size, write_size, [] if input_stage is None else [input_stage], None)
                    if input_stage is not None:
                        input_stage.consumer_stage = curr_stage
                    input_stage = curr_stage
                    stages.append(curr_stage)
                    start_time+=consumer_time
            else:
                num_stages = len(query_data[which_query])
                qdata = query_data[which_query]
                stage_map = {}
                for stage in range(num_stages):
                    start_time = start_time+qdata[stage][0]
                    deps = qdata[stage][2]
                    consumers = len(qdata[stage][3])
                    consumer_time = qdata[stage][3][-1]
                    stage_tasks = qdata[stage][3]
                    shuffle_size = max(1, qdata[stage][1]//consumers)
                    num_reads=0
                    read_size=0
                    input_stages = []
                    for dep_stage in deps:
                        input_stage = stage_map[dep_stage]
                        num_reads = input_stage.num_tasks * consumers
                        read_size = input_stage.num_tasks * input_stage.write_size
                        input_stage.total_partitions = num_reads
                        input_stage.partition_size = num_reads/shuffle_size
                        input_stages.append(input_stage)
                    write_size = shuffle_size
                    curr_stage = Stage((query,stage),start_time, start_time+consumer_time, consumers, num_reads, read_size, write_size, input_stages, stage_tasks)
                    for dep_stage in deps:
                        input_stage = stage_map[dep_stage]
                        input_stage.consumer_stage = curr_stage
                    stages.append(curr_stage)
                    stage_map[stage] = curr_stage
                    
            input_stage.write_size = 0
        all_queries.sort(key = lambda x : x[0])
        query_filename = "cached_workloads/{}.queries.gz".format(filename_base)
        with gzip.open(query_filename, "wt") as qf:
            for query in all_queries:
                qf.write("{},q{}\n".format(query[0], query[1]))

        total_workload_filename = "cached_workloads/{}.workload.gz".format(filename_base)
        task_filename = "cached_workloads/{}.task.gz".format(filename_base)
        if not os.path.exists(total_workload_filename) or not os.path.exists(task_filename):
            task_filename = "cached_workloads/{}.task.gz".format(filename_base)
            num_tasks = np.zeros(start_buff+args.length+start_buff, dtype=np.int64)
            with gzip.open(total_workload_filename, "wt") as twf:
                twf.write(str(args.length+start_buff))
                twf.write("\n")

                twf.write(",".join(["{}:{}".format(workload_arg_list[x], arg_dict[workload_arg_list[x]]) for x in range(len(workload_arg_list))]))
                twf.write("\n")
                twf.write(str("name,start_time,end_time,num_tasks,num_reads,read_size,write_size,producers"))
                twf.write("\n")

                for stage in stages:
                    twf.write(str(stage))
                    twf.write("\n")
                    if args.simulated_queries:
                        num_tasks[stage.start_time:stage.end_time]+=stage.num_tasks
                    else:
                        for task in stage.tasks:
                            num_tasks[stage.start_time:stage.start_time+task]+=1



            with gzip.open(task_filename, "wt") as tf:
                tf.write(str(args.length+start_buff))
                tf.write("\n")
                tf.write(str(workload_arg_list))
                tf.write("\n")
                tf.write(str("name,start_time,end_time,num_tasks,num_reads,read_size,write_size,producers"))
                tf.write("\n")
                tf.write(str(len(num_tasks)))
                tf.write("\n")
                tf.write(str("\n".join([str(x) for x in num_tasks])))
    if args.generate_only:
        exit(0)

    stages.sort(key=lambda x: x.start_time)
    end_stages = sorted(stages, key=lambda x: x.end_time)

    exp_length = args.length+args.period

    kb_in_flight = np.zeros(exp_length, dtype=np.int64)
    total_reads = 0
    total_writes = 0

    lookback = args.lookback
    start_stage_index = 0
    end_stage_index = 0

    total_cache_nodes = np.zeros(start_buff+args.length+args.period, dtype=np.int64)
    available_cache = np.zeros(start_buff+args.length+args.period, dtype=np.int64)
    #contains available cache
    cache = Cache()

    time_jump = 1
    for time in range(0, exp_length, time_jump):
        if time % 100 == 0:
            eprint(time, exp_length, time/exp_length*100)
            pass
        desired_num_cache_nodes = 0
        if args.cache_type == "perfect":
            desired_num_cache_nodes = 0
            args.enable_eviction = False
        elif args.cache_type == "fixed":
            if args.cache_param is None:
                args.cache_param = 1
            desired_num_cache_nodes = args.cache_param
            if int(args.cache_param) == 0:
                args.enable_eviction = False
        elif args.cache_type == "mean":
            if args.cache_param is None:
                args.cache_param = 1
            lookback_mean = kb_in_flight[max(time-lookback, 0):time].mean()*args.cache_param if time > 0 else 0.0
            desired_num_cache_nodes = int(lookback_mean/cache_size_per_node_b)
        elif args.cache_type == "percentile":
            if args.cache_param is None:
                args.cache_param = 90.0
            lookback_mean = np.percentile(kb_in_flight[max(time-lookback, 0):time], args.cache_param) if time > 0 else 0.0
            lookback_mean *= 2
            desired_num_cache_nodes = int(lookback_mean/cache_size_per_node_b)
        cache.set_desired_num_cache_nodes(int(desired_num_cache_nodes), time)
        total_cache_nodes[time] = cache.get_billable_cache_nodes() 

        cache.increment_time(time, time_jump)

        while end_stage_index < len(end_stages) and end_stages[end_stage_index].end_time == time:
            stage = end_stages[end_stage_index]
            total_reads += cache.complete_stage(stage)
            end_stage_index+=1

        while start_stage_index < len(stages) and stages[start_stage_index].start_time == time:
            stage = stages[start_stage_index]
            (new_writes, new_reads) = cache.attempt_write_to_cache(stage, time)
            total_writes+=new_writes
            total_reads+= new_reads
            start_stage_index+=1
        kb_in_flight[time] = cache.get_current_kb_in_flight()
        available_cache[time] = cache.get_current_cache_capacity()


    #cache.print_remaining_nodes()
    read_cost = total_reads*s3_read_cost
    write_cost = total_writes*s3_write_cost
    cache_cost = total_cache_nodes.sum()*cache_node_cost_per_second
    total_cost = read_cost+write_cost+cache_cost
    #print("S3 Write: {}".format(write_cost))
    #print("S3 Read:  {}".format(read_cost))
    #print("Cache  :  {}".format(cache_cost))
    #print("Total  :  {}".format(total_cost))
    #print("{},{},{},{},{}".format(write_cost,read_cost,cache_cost,total_cost,cache.get_total_shuffle_size()), file=sys.stdout)
    print("{},{},{},{}".format(write_cost,read_cost,cache_cost,total_cost,cache.get_total_shuffle_size()), file=sys.stdout)
    #print("{}".format(total_cost), file=sys.stdout)

    if args.write_plot:
        plt.plot(np.array(range(len(kb_in_flight)))/3600, kb_in_flight/(1024*1024*1024), label="GB In Flight")
        plt.plot(np.array(range(len(total_cache_nodes)))/3600, total_cache_nodes*3, label="Cache Running/Starting")
        plt.plot(np.array(range(len(available_cache)))/3600, available_cache/(1024*1024*1024), label="Available Cache")
        plt.xlabel("Time(Hours)")
        plt.ylabel("GB shuffling")
        plt.legend()
        #plt.show()
        #plt.savefig("plots/{}_shuffle.png".format(filename_base))
        plt.savefig("plots/{}_shuffle.pdf".format(filename_base))



if __name__ == "__main__":
    random.seed(198243)
    np.random.seed(19823197)
    parser = argparse.ArgumentParser()
    parser.add_argument("--length", "-l", default=60*60*24, type=int)
    parser.add_argument("--max_joins_per_query", "-j", default=10, type=int)
    parser.add_argument("--queries", "-q", default=256, type=int)
    parser.add_argument("--max_num_workers", "-w", default=1024, type=int)
    parser.add_argument("--min_num_workers", "-m", default=32, type=int)
    parser.add_argument("--max_task_runtime", "-t", default=30, type=int)
    parser.add_argument("--period", "-p", default=60*60, type=int)
    parser.add_argument("--baseline_load", "-b", default=.3, type=float)
    parser.add_argument("--reduction", "-r", default=1, type=float)
    parser.add_argument("--max_shuffle_size_per_producer", "-s", type=float, default=1024*1024*1024/2)
    parser.add_argument("--smoothness_experiment", "-se", default=False, action="store_true")
    parser.add_argument("--simulated_queries", "-sq", default=False, action="store_true")
    #Kilobytes
    parser.add_argument("--min_billing_time", "-mb", default=60, type=int)
    parser.add_argument("--cache_type", "-c", default="percentile", type=str)
    parser.add_argument("--cache_param", "-cp", type=float)
    parser.add_argument("--cost_mult", "-cm", type=float, default=1)
    parser.add_argument("--min_cache_nodes", "-mc", type=int, default=0)
    parser.add_argument("--enable_eviction", "-e", default=False, action="store_true")
    parser.add_argument("--cache_startup_time", "-v", default=30, type=int)
    parser.add_argument("--lookback", "-lb", default=5*60, type=int)
    parser.add_argument("--generate_only", "-go", default=False, action="store_true")
    parser.add_argument("--write_plot", "-wp", default=False, action="store_true")

    args = parser.parse_args()
    s3_write_cost*=args.cost_mult
    s3_read_cost*=args.cost_mult
    main(args)
