use std::collections::{HashMap, HashSet};
use std::sync::{Arc};
use std::sync::atomic::{AtomicI64, AtomicBool, Ordering};
use std::time::SystemTime;
use std::fs;
use std::io::{BufReader, BufRead};
use rand::{distributions::Alphanumeric, Rng};
use inc_stats::Percentiles;


struct History {
    history : Vec::<i64>,
}

fn worker_allocator(task_history : History, strat_hist : Vec::<(usize, usize)>){
    let vm_startup_time = 180;
    let freq :usize= 5;

    let mut target_pctl = Vec::new();
    let mut multipliers = Vec::new();
    for x in 0..101 {
        target_pctl.push(x as f64 /100.0);
        multipliers.push(1.0);
    }
    for x in 1..11 {
        target_pctl.push(80 as f64 /100.0);
        multipliers.push(1.0+0.1*(x as f64));
    }
    for x in 1..21 {
        target_pctl.push(80 as f64 /100.0);
        multipliers.push(1.0*(x as f64));
    }
    //let mean_multipliers = vec![0.5, 1.0, 1.5, 2.0, 3.0];
    let mean_multipliers = vec![];
    let mut fixed_strategies = Vec::<i64>::new();
    for x in 0..15 {
        //fixed_strategies.push(50*x);
    }
    let num_strats = multipliers.len()+mean_multipliers.len()+fixed_strategies.len();
    let mut lookbacks = vec![180, 300, 600, 900, 1200, 1800];
    let mut hist_alloc = vec![vec![vec![0 ; num_strats]; lookbacks.len()];  (vm_startup_time/freq) as usize];
    let mut historical_costs = vec![vec![0.0; num_strats]; lookbacks.len()];
    let mut intervention = 0;
    let mut curr_time = 5;
    while curr_time < task_history.history.len() {
        let mut best_target = 0;
        let mut lowest_cost = f64::MAX;
        let min_num_workers = 0;
        let mut total_spot_time = 0;
        let mut chosen_lookback_idx = 0;
        let mut chosen_strat_idx = 0;
        for lookback_idx in 0..lookbacks.len() {
            let lookback = lookbacks[lookback_idx];
            let mut history;
            {

                let begin= std::cmp::max(0, curr_time as i64 -lookback) as usize;
                history = Vec::from_iter(task_history.history[begin..curr_time+1].iter().cloned());
            }
            if history.len() < freq {
                continue;
            }
            if history.len() < lookback as usize{
                let num_to_add = lookback as usize -history.len();
                let mut temp = vec![0; num_to_add];
                temp.append(&mut history);
                history = temp;
            }
            let lambda_cost_s : f64 = 0.00005;
            let spot_cost_s : f64 = 0.0400/(60.0*60.0);
            let mut curr_options = Vec::<i64>::new();
            let pctl_data_struct : Percentiles<f64> = history.iter().map(|v| *v as f64).collect();
            let mut percentiles = pctl_data_struct.percentiles(&target_pctl).unwrap().unwrap();
            for mult_idx in 0..multipliers.len(){
                curr_options.push((percentiles[mult_idx]*multipliers[mult_idx]) as i64);
            }
            let mean: f64 = history.iter().sum::<i64>() as f64/history.len() as f64;
            for mult in &mean_multipliers{
                curr_options.push((mean*mult) as i64);
            }
            for strat in &fixed_strategies{
                curr_options.push(strat.clone())
            }
            for strat_idx in 0..curr_options.len(){
                let possible_curr_node = hist_alloc[(intervention%(vm_startup_time/freq)) as usize][lookback_idx][strat_idx];
                let spot_time = possible_curr_node * freq as i64;
                total_spot_time+=spot_time;
                let spot_cost = spot_time as f64 * spot_cost_s;
                let lambda_cost :f64 = history[history.len()-freq..].iter().map(|x| std::cmp::max(x-possible_curr_node, 0)).sum::<i64>() as f64* lambda_cost_s;
                historical_costs[lookback_idx][strat_idx] += lambda_cost + spot_cost;
                if historical_costs[lookback_idx][strat_idx] < lowest_cost {
                    lowest_cost = historical_costs[lookback_idx][strat_idx];
                    best_target = curr_options[strat_idx];
                    chosen_strat_idx = strat_idx;
                    chosen_lookback_idx = lookback_idx;

                }
                hist_alloc[(intervention%(vm_startup_time/freq))as usize][lookback_idx][strat_idx] = curr_options[strat_idx];
            }
        }
        if total_spot_time == 0{
            best_target = hist_alloc[(intervention%(vm_startup_time/freq))as usize][1][99];
        }
        let choose_strat = strat_hist[curr_time/freq-1];
        //best_target = hist_alloc[(intervention%(vm_startup_time/freq))as usize][choose_strat.0][choose_strat.1];
        //if intervention == 504 {
        //println!("{} {}", historical_costs[chosen_lookback_idx][chosen_strat_idx], historical_costs[choose_strat.0][choose_strat.1]);
        let num_workers = std::cmp::max(best_target, min_num_workers);
        for _ in 0..5{
            println!("{}", num_workers);
        }
        intervention+=1;
        curr_time+=5
    }
} 

fn parse_task_demand(filename : &str) -> History {
    let file = fs::File::open(filename).unwrap();
    let mut  hist = History {
                history: Vec::<i64>::new()
            };
    for line in BufReader::new(file).lines(){
        let line = line.unwrap();
        let curr_demand = line.parse::<i64>().unwrap();
        hist.history.push(curr_demand);

    }
    hist
}

fn parse_strat(filename : &str) -> Vec<(usize, usize)> {
    let file = fs::File::open(filename).unwrap();
    let mut hist = Vec::<(usize, usize)>::new();
    for line in BufReader::new(file).lines(){
        let line = line.unwrap();
        let split_line = line.split(" ").collect::<Vec<&str>>();
        let first_idx = split_line[0].parse::<usize>().unwrap();
        let second_idx = split_line[1].parse::<usize>().unwrap();
        hist.push((first_idx, second_idx));

    }
    hist
}

fn main() {
    worker_allocator(parse_task_demand("750_demand_curve.2"), parse_strat("750_worker_strat.2"));
}
