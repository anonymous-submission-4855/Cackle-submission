import argparse
import numpy as np
import pandas as pd
import random
import matplotlib.pyplot as plt
import ast
import gzip
from scipy.stats import cosine

vm_startup_time = 150
min_billing_time = 60
vm_cost_per_second = (0.0400/(60*60))
def parse_trace_file_old(filename):
    min_time = 0
    max_time = float("-inf")
    with open(filename) as infile:
        for line in infile:
            split_line = line.split(",")
            if len(split_line) < 2:
                continue
            max_time = max(max_time, int(split_line[1]))
    demand = np.zeros(max_time+120, dtype=np.int32)
    with open(filename) as infile:
        for line in infile:
            split_line = line.split(",")
            if len(split_line) < 2:
                continue
            demand[int(split_line[0])+vm_startup_time:int(split_line[1])+vm_startup_time]+=1
    return max_time, demand

def parse_trace_file(filename):
    demand = None
    curr_line = 0
    skip_lines = 3
    with gzip.open(filename, "rt") as infile:
        first_line = True
        index = 0
        for line in infile:
            if curr_line < skip_lines:
                curr_line+=1
            elif curr_line == skip_lines:
                demand = np.zeros(int(line), dtype=np.int32)
                curr_line+=1
            else:
                demand[index] = int(line)
                index+=1
    #demand*=50
    return len(demand)+120, demand

def parse_busy_file(filename):
    max_time = 0
    executors = []
    with open(filename, 'r') as infile:
        for line in infile:
            executors.append(ast.literal_eval(line))
            if executors[-1][-1][1] > max_time:
                max_time = executors[-1][-1][1]
    length = max_time
    demand = np.zeros(length+120)
    for executor in executors:
        for busy in executor:
            demand[busy[0]+vm_startup_time:busy[1]+vm_startup_time]+=1
    return length, demand

def main(args):
    length = args.length
    start_buff = 180
    demand = np.zeros(length+start_buff, dtype=np.int32)
    if args.in_file is not None:
        length, demand = parse_busy_file(args.in_file)

    elif args.trace is not None:
        length, demand = parse_trace_file(args.trace)
    else:
        total_tasks = 0
        for stage in range(args.stages):
            period_len = args.period
            start_time = 0
            if random.uniform(0, 1) < args.baseline_load:
                start_time = random.randint(1,length)+start_buff
            else:

                #start_time = random.randint(start_buff, start_buff+length-1)
                normalized_start = (cosine.rvs()+np.pi)/(2*np.pi)
                period_num = random.randint(0, length//period_len)
                start_time = start_buff+period_num*period_len+int(normalized_start*period_len)
            
            num_tasks = random.randint(1,args.max_num_tasks)
            total_tasks+=num_tasks
            avg_task_length = random.randint(1, args.max_task_length)
            task_lengths = np.random.normal(loc=avg_task_length, scale=avg_task_length/10, size=num_tasks)
            task_lengths[task_lengths<0] = 1
            for task_len in task_lengths:
                demand[start_time:min(len(demand), start_time+int(task_len))]+=1
    if args.plot:
        plt.plot(np.array(range(len(demand)))/60, demand, label="Compute Demand")
    max_demand = np.max(demand)
    small_duration = None
    large_duration = None
    if args.strategy == "fixed":
        small_duration, large_duration = fixed_executor_cost(demand, int(args.strategy_params))
    elif args.strategy == "dynamic":
        small_duration, large_duration = dynamic_prov(demand, True, args.vm_startup_time, args.cost_premium, args.change_strat_freq)
    elif args.strategy == "mean":
        small_duration, large_duration = mean_prov(demand, args.plot, args.lookback, args.strategy_params)
    elif args.strategy == "perfect":
        small_duration, large_duration = perfect_prov(demand, True, args.cost_premium)
    else:
        print("incorrect strategy name", args.strategy)

    
    '''
    for percentile in [25, 50, 75, 100]:
        small, large = static_prov(demand, args.plot, percentile)
        vals.append(small)
        vals.append(large)
    '''

    print(",".join([str(x*vm_cost_per_second) for x in [small_duration*args.cost_premium, large_duration, (small_duration*args.cost_premium+large_duration)]]))

    if args.plot:
        #plt.plot(range(len(demand)), demand, label="demand")
        plt.legend()
        #plt.show()
        #plt.gcf().set_size_inches(10, 8)
        #plt.savefig("sweep_plots/{}-{}-{:05d}-{:05d}.pdf".format(args.strategy, args.strategy_params, args.vm_startup_time, args.vm_min_billing_time))
        #plt.savefig("plots/{}_compute.pdf".format(args.trace.split("/")[1]))
        plt.xlabel("Time(m)")
        plt.ylabel("Concurrent Tasks")
        plt.gcf().tight_layout()
        plt.savefig("plots/{}_compute.pdf".format(args.trace.split("/")[1]))

    vals = []
    return vals

def fixed_executor_cost(demand, fixed):
    print(len(demand))
    demand = demand.copy()
    large_duration = fixed*len(demand)
    small_duration = demand-fixed
    small_duration[small_duration<0] = 0
    return small_duration.sum(), large_duration

def ideal_time(demand, infile):
    starting = np.zeros(len(demand), dtype=int)
    running = np.zeros(len(demand), dtype=int)
    with open(infile, 'r') as read:
        for line in read:
            split = line.split(" ")
            if split[0] == "starting":
                starting[int(split[1])+vm_startup_time:int(split[2])+vm_startup_time]+=1
            else:
                print(split)
                running[int(split[1])+vm_startup_time:int(split[2])+vm_startup_time]+=1
    plt.plot(range(len(demand)), running, label="running")
    plt.plot(range(len(demand)), starting, label="starting")
    large_cost = starting.sum()+running.sum()
    remaining_small = demand.copy()
    remaining_small = remaining_small-running
    remaining_small[remaining_small<0] = 0
    small_cost = remaining_small.sum()
    return large_cost+small_cost


def static_prov(demand, plot, percentile):
    starting = np.zeros(len(demand), dtype=int)
    num_running = int(np.percentile(demand, percentile))
    running = np.full(len(demand), num_running)



    large_cost = starting.sum()+running.sum()
    remaining_small = demand.copy()
    remaining_small = remaining_small-running
    remaining_small[remaining_small<0] = 0
    small_cost = remaining_small.sum()
    return small_cost, large_cost

def perfect_prov(demand, plot, cost_premium):
    small_cost_prem = cost_premium
    starting = np.zeros(len(demand), dtype=int)
    running = np.zeros(len(demand), dtype=int)
    target_cap = np.zeros(len(demand), dtype=int)
    shutdown_capacity = np.zeros(len(demand), dtype=int)
    interventions = (np.concatenate((demand, np.full(1, 0)))-np.concatenate((np.full(1, 0), demand)))[:-1]
    interventions[interventions<0] = 0
    interventions[interventions>1] = 1
    interventions_list = list(interventions.nonzero()[0])
    for interv in interventions_list:
        if interv+min_billing_time < len(interventions):
            interventions[interv+min_billing_time]=1
    interventions = list(interventions.nonzero()[0])
    interventions = list(range(len(starting)))
    for interv_idx in range(len(interventions)):
        intervention = interventions[interv_idx]
        next_intervention = None if interv_idx == len(interventions)-1 else interventions[interv_idx+1]
        until = intervention+min_billing_time

        lookforward_demand = demand[intervention: min(len(running), until)]
        total_cost = sum(lookforward_demand)*small_cost_prem
        target = 0
        start_search = min(demand[intervention], running[intervention])
        end_search = max(demand[intervention]+1, running[intervention]+1)
        candidate_targets = np.array(range(start_search, end_search))
        remaining = lookforward_demand.reshape((1, lookforward_demand.shape[0]))-candidate_targets.reshape((candidate_targets.shape[0], 1))
        remaining[remaining<0] = 0
        big_time = candidate_targets*len(lookforward_demand)
        small_time = np.sum(remaining, axis=1)
        candidate_cost = big_time+small_time*small_cost_prem
        candidate_target_idx = np.argmin(candidate_cost)
        if candidate_cost[candidate_target_idx] < total_cost:
            total_cost = candidate_cost[candidate_target_idx]
            target = candidate_targets[candidate_target_idx]
        #target = max(target)
        current_total = running[intervention]
        target_cap[intervention:until] = target
        if current_total <= target:
            num_to_start = int(target-current_total)
            starting[intervention-vm_startup_time:intervention]+=num_to_start
            if intervention+min_billing_time < len(running):
                running[intervention]+=num_to_start
                shutdown_capacity[intervention+min_billing_time]+=num_to_start
        elif target < running[intervention] and running[intervention] > demand[intervention]:
            #shutdown running if more than needed
            #TODO needs work to match shutdown curve
            next_period = int(max(target, demand[intervention]))
            num_to_shutdown = running[intervention]-next_period
            num_to_shutdown = min(shutdown_capacity[intervention], num_to_shutdown)
            shutdown_capacity[intervention] -= num_to_shutdown
            running[intervention] -= num_to_shutdown
        until = len(interventions) if next_intervention is None else next_intervention+1
        running[intervention+1:until]+=running[intervention]
        shutdown_capacity[intervention+1:until]+=shutdown_capacity[intervention]
    
    # post process
    usable = None
    usable = (np.concatenate((demand, np.full(1, 0)))-np.concatenate((np.full(1, 0), demand)))[:-1]
    usable[usable < 0] = 0
    usable = np.minimum(usable, running)
    running_neg = (np.concatenate((running, np.full(1, 0)))-np.concatenate((np.full(1, 0), running)))[:-1]
    running_neg[running_neg > 0] = 0
    usable = usable+running_neg
    usable[usable < 0] = 0
    usable = np.cumsum(usable)
    usable = np.minimum(running, usable)
    if plot:
        if False:
            if usable is None:
                plt.plot(range(len(demand)), running, label="running")
            else:
                plt.plot(range(len(demand)), usable, label="running")
        plt.plot(range(len(demand)), running, label="running")
        #plt.plot(range(len(demand)), target_cap, label="target")
        #plt.plot(range(len(demand)), starting, label="starting")
        plt.plot(range(len(demand)), shutdown_capacity, label="shutdown_capacity")

    large_cost = running.sum()
    remaining_small = demand.copy()
    remaining_small = remaining_small-usable
    remaining_small[remaining_small<0] = 0
    small_cost = remaining_small.sum()
    with open("sample_output", 'w') as outf:
        for running in running:
            outf.write("{}\n".format(running))
    return small_cost, large_cost

def mean_prov(demand, plot, lookback, multiplier):
    starting = np.zeros(len(demand), dtype=int)
    running = np.zeros(len(demand), dtype=int)
    shutdown_capacity = np.zeros(len(demand), dtype=int)
    freq = 5
    lookback_window= lookback
    for intervention in range(freq, len(demand), freq):
        lookback_demand = demand[max(intervention-lookback_window, 0): intervention].copy()
        if len(lookback_demand) == lookback_window:
            lookback_demand[0:10*freq] = np.max(lookback_demand)
        current_percentile = -1
        
        target = int(np.mean(lookback_demand)*multiplier)
        #target = max(target)
        current_total = starting[intervention]+running[intervention]
        if current_total < target:
            num_to_start = int(target-current_total)
            starting[intervention:intervention+vm_startup_time]+=num_to_start
            if intervention+vm_startup_time < len(running):
                running[intervention+vm_startup_time]+=num_to_start
                if intervention+vm_startup_time+min_billing_time < len(running):
                    shutdown_capacity[intervention+vm_startup_time+min_billing_time]+=num_to_start
            if vm_startup_time>0 and intervention+vm_startup_time< intervention+freq:
                running[1+intervention+vm_startup_time:intervention+freq+1]+=num_to_start
        elif target < running[intervention] and running[intervention] > demand[intervention]:
            #shutdown running if more than needed
            next_period = int(max(target, demand[intervention]))
            num_to_shutdown = running[intervention]-next_period
            num_to_shutdown = min(shutdown_capacity[intervention], num_to_shutdown)
            shutdown_capacity[intervention] -= num_to_shutdown
            running[intervention] -= num_to_shutdown
        running[intervention+1:1+intervention+freq]+=running[intervention]
        shutdown_capacity[intervention+1:1+intervention+freq]+=shutdown_capacity[intervention]
            

    # post process
    usable = None
    usable = (np.concatenate((demand, np.full(1, 0)))-np.concatenate((np.full(1, 0), demand)))[:-1]
    usable[usable < 0] = 0
    usable = np.minimum(usable, running)
    running_neg = (np.concatenate((running, np.full(1, 0)))-np.concatenate((np.full(1, 0), running)))[:-1]
    running_neg[running_neg > 0] = 0
    usable = usable+running_neg
    usable[usable < 0] = 0
    usable = np.cumsum(usable)
    usable = np.minimum(running, usable)
    if plot:
        if False:
            if usable is None:
                plt.plot(range(len(demand)), running, label="running")
            else:
                plt.plot(range(len(demand)), usable, label="running")
        plt.plot(range(len(demand)), running, label="running")
        #plt.plot(range(len(demand)), starting, label="starting")
        #plt.plot(range(len(demand)), shutdown_capacity, label="shutdown_capacity")

    large_cost = running.sum()
    remaining_small = demand.copy()
    remaining_small = remaining_small-usable
    remaining_small[remaining_small<0] = 0
    small_cost = remaining_small.sum()
    return small_cost, large_cost

def dynamic_prov(demand, plot, vm_startup_time, cost_premium, change_strat_frequency):
    small_cost_prem = cost_premium
    starting = np.zeros(len(demand), dtype=int)
    running = np.zeros(len(demand), dtype=int)
    multipliers = [1.0+x/10 for x in range(10)]+[1.0+x for x in range(21)]
    percentiles = list(range(0, 101, 1))+[80 for x in range(len(multipliers))]
    multipliers = [1.0 for x in range(101)] + multipliers
    percentiles = np.array(percentiles)
    #mean_multipliers = np.array([0.5, 1, 1.5, 2, 3, 4, 5])
    #fixed_strategies = np.array([x*50 for x in range(40)])
    mean_multipliers = np.array([])
    fixed_strategies = np.array([])
    #fixed_strategies = np.array([x*50 for x in range(1)])
    num_strategies = len(percentiles)+len(mean_multipliers)+len(fixed_strategies)
    #multipliers = [1.0+x/10 for x in range(200)]
    multipliers = np.array(multipliers)
    freq = change_strat_frequency
    num_interventions = len(demand)//freq-1

    #pctl_alloc_history = np.ze lros((len(percentiles)+1, num_interventions), dtype=int)
    #pctl_cost_history = np.zeros((len(percentiles)+1, num_interventions))
    lookback_windows = [0, 10, 20, 30, 60, 120, 150, 180, 240, 270, 300, 400, 500, 600, 900, 1200, 1800, 3600, 5400, 7200][::-1]
    lookback_windows = [0, 10, 20, 30, 60, 120, 300, 600, 900, 1200, 1800, 3600, 5400, 7200][::-1]
    lookback_windows = [x for x in lookback_windows if x >= vm_startup_time*1.5]
    lookback_windows = [180, 300, 600, 900, 1200, 1800]
    #lookback_windows = [0, 10, 20, 30, 60, 120, 180, 300, 600, 900, 1200, 1800, 3600, 5400, 7200]
    pctl_target_history = np.zeros((len(lookback_windows), num_interventions, num_strategies), dtype=int)
    pctl_cost_history = np.zeros((len(lookback_windows), num_interventions, num_strategies), dtype=int)
    shutdown_capacity = np.zeros(num_interventions, dtype=int)
    for intervention_idx in range(num_interventions):
        intervention = (intervention_idx+1)*freq
        current_total = starting[intervention]+running[intervention]
        last_idx = (intervention_idx-vm_startup_time//freq)
        for lookback_window_idx in range(len(lookback_windows)):
            lookback_window = lookback_windows[lookback_window_idx]
            lookback_demand = demand[max(0,intervention-lookback_window) :intervention+1]
            #lookback_demand = np.concatenate((np.zeros(lookback_window+1-len(lookback_demand)), np.full(1, max(200, np.max(lookback_demand)*1.5)), lookback_demand[3:]))
            lookback_demand = np.concatenate((np.zeros(lookback_window+1-len(lookback_demand)), lookback_demand))
            #candidate_targets = np.mean(lookback_demand)*multipliers
            pctl_targets = np.percentile(lookback_demand, percentiles)*multipliers
            mean_targets = np.mean(lookback_demand)*mean_multipliers
            candidate_targets = np.concatenate((pctl_targets, mean_targets, fixed_strategies))
            pctl_target_history[lookback_window_idx, intervention_idx, :]=candidate_targets
            last_segment = demand[intervention-freq+1: intervention+1]
            possible_curr_nodes = None
            if last_idx >= 0:
                possible_curr_nodes = pctl_target_history[lookback_window_idx, last_idx, :]
                if lookback_window_idx == 5:
                    pass
                    #print(intervention, last_idx, list(possible_curr_nodes))
            else:
                possible_curr_nodes = np.zeros(num_strategies)
            remaining = np.subtract(last_segment.reshape((1, last_segment.shape[0])), possible_curr_nodes.reshape((possible_curr_nodes.shape[0], 1)))
            remaining[remaining<0] = 0
            big_times = possible_curr_nodes*len(last_segment)
            small_times = np.sum(remaining, axis=1)
            costs = big_times+small_times*small_cost_prem
            pctl_cost_history[lookback_window_idx, intervention_idx, :] = costs
        historical_costs = np.sum(pctl_cost_history[:, 0:intervention_idx+1, :], axis=1)
        if pctl_target_history[:, last_idx, :].sum() == 0.0:
            target_idx = (len(lookback_windows)-1, list(percentiles).index(99))
        else:
            target_idx = np.unravel_index(np.argmin(historical_costs), historical_costs.shape)
        target = pctl_target_history[:, intervention_idx, :][target_idx]

        if current_total <= target:
            num_to_start = int(target-current_total)
            if vm_startup_time > 0:
                starting[intervention:intervention+vm_startup_time]+=num_to_start
            if intervention+vm_startup_time < len(running):
                running[intervention+vm_startup_time]+=num_to_start
                if intervention_idx+(vm_startup_time+min_billing_time)//freq < len(shutdown_capacity):
                    shutdown_capacity[intervention_idx+(vm_startup_time+min_billing_time)//freq]+=num_to_start
            if vm_startup_time > 0 and intervention+vm_startup_time< intervention+freq:
                running[1+intervention+vm_startup_time:intervention+freq+1]+=num_to_start
        elif target < running[intervention] and running[intervention] > demand[intervention]:
            #shutdown running if more than needed
            next_period = int(max(target, demand[intervention]))
            num_to_shutdown = running[intervention]-next_period
            num_to_shutdown = min(shutdown_capacity[intervention_idx], num_to_shutdown)
            shutdown_capacity[intervention_idx] -= num_to_shutdown
            running[intervention] -= num_to_shutdown
        running[intervention+1:1+intervention+freq]+=running[intervention]
        if intervention_idx+1 < len(shutdown_capacity):
            shutdown_capacity[intervention_idx+1]+=shutdown_capacity[intervention_idx]
            

    # post process
    usable = None
    usable = (np.concatenate((demand, np.full(1, 0)))-np.concatenate((np.full(1, 0), demand)))[:-1]
    usable[usable < 0] = 0
    usable = np.minimum(usable, running)
    running_neg = (np.concatenate((running, np.full(1, 0)))-np.concatenate((np.full(1, 0), running)))[:-1]
    running_neg[running_neg > 0] = 0
    usable = usable+running_neg
    usable[usable < 0] = 0
    usable = np.cumsum(usable)
    usable = np.minimum(running, usable)
    if plot:
        if False:
            if usable is None:
                plt.plot(range(len(demand)), running, label="running")
            else:
                plt.plot(range(len(demand)), usable, label="running")
        plt.plot(np.array(range(len(demand)))/60, running, label="Available VMs")
        #plt.plot(np.array(range(len(demand)))/60, running, label="Available VMs")
        plt.ylim(0, 1200)
        plt.gcf().tight_layout()
        #plt.plot(freq*np.array(range(pctl_target_history.shape[1]))+1200, pctl_target_history[target_idx[0], :, target_idx[1]] , label="target")
        #plt.plot(range(len(demand)), starting, label="starting")
        #plt.plot((np.array(range(len(shutdown_capacity)))+1)*freq, shutdown_capacity, label="shutdown_capacity")

    large_cost = running.sum()
    remaining_small = demand.copy()
    remaining_small = remaining_small-usable
    remaining_small[remaining_small<0] = 0
    small_cost = remaining_small.sum()
    with open("sample_output.2", 'w') as outf:
        for running in running:
            outf.write("{}\n".format(running))
    return small_cost, large_cost

if __name__ == "__main__":
    random.seed(198243)
    np.random.seed(19823197)
    parser = argparse.ArgumentParser()
    parser.add_argument("--length", "-l", default=604800, type=int)
    parser.add_argument("--stages", "-s", default=1024, type=int)
    parser.add_argument("--max_num_tasks", "-n", default=1024, type=int)
    parser.add_argument("--mult", "-m", default=1, type=int)
    parser.add_argument("--max_task_length", "-t", default=90, type=int)
    parser.add_argument("--in_file", "-i", default=None, type=str)
    parser.add_argument("--trace", "-r", default=None, type=str)
    parser.add_argument("--plot", "-p", action="store_true")
    parser.add_argument("--period", "-d", default=86400, type=int)
    parser.add_argument("--baseline_load", "-b", default=.3, type=float)
    parser.add_argument("--cost_premium", "-c", default=5, type=float)
    parser.add_argument("--change_strat_freq", "-a", default=10, type=int)
    parser.add_argument("--lookback", "-o", default=300, type=int)
    parser.add_argument("--vm_startup_time", "-v", default=30, type=int)
    parser.add_argument("--vm_min_billing_time", "-mb", default=60, type=int)
    parser.add_argument("--strategy", "-st", choices=["perfect", "mean", "dynamic", "fixed"], default="dynamic")
    parser.add_argument("--strategy_params", "-sp", default=None, type=float)
    args = parser.parse_args()
    mult = args.mult
    vm_startup_time=args.vm_startup_time
    min_billing_time=args.vm_min_billing_time

    main(args)
