use aws_sdk_lambda::{Client, Error, Endpoint};
use aws_sdk_lambda::types::{Blob};

use aws_sdk_s3::model::{ObjectIdentifier, Delete};

use futures::future::join_all;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc};
use std::sync::atomic::{AtomicI64, AtomicBool, Ordering};
use std::time::SystemTime;
use std::fs;
use chrono::offset::Local;
use std::io::{BufReader, BufRead};
use tokio::time::{self, Duration, Instant};
use tokio::net::{TcpListener};
use tokio::sync::{mpsc, oneshot, watch, Mutex};
use rand::{distributions::Alphanumeric, Rng};
use inc_stats::Percentiles;

use cackle_cache::cache_client::{CacheClient};
use cackle_cache::{*};

pub mod cackle_cache {
    tonic::include_proto!("cackle_cache");
}

struct History {
    history : Vec::<i64>,
}

#[derive(Debug)]
struct QueryExecution {
    start_time : Instant,
    duration : Duration,
    query_id : String,
    num_attempts : u32,
    overflow_exec : bool, 
    is_error : bool,
}

#[derive(PartialEq)]
enum WorkerState {
    Ready,
    Busy,
    Closed
}

struct Worker {
    client : Client,
    state: WorkerState,
    start_time : SystemTime,
    close_time : SystemTime,
}

#[derive(Debug)]
struct StageIntermediateData {
    s3_keys_written : Vec<String>,
    // out_partition -> (node -> vec(in_partition))
    cache_written :  HashMap::<u32, HashSet<u32>>,
    shuffle_size : i64
}

#[derive(Debug)]
struct QueryExecData {
    stage_shuffle_map : Mutex<HashMap<u32, StageIntermediateData>>,
    watch_channels : HashMap<u32, watch::Receiver<bool>>,
    use_overflow : bool,
}

#[derive(Debug, Clone)]
struct QueryData {
    query_name : String,
    query_deps : HashMap<u32, Vec<u32>>,
    query_nd_map : HashMap<u32,Vec<u32>>,
    query_drop_map : HashMap<u32, Vec<u32>>,
    query_num_partitions : HashMap<u32, u32>,
    task_nums : Vec<u32>,
    constants : String,
}

struct CacheNode {
    running_queries : AtomicI64,
    state: WorkerState,
    start_time : SystemTime,
    close_time : SystemTime,
}

type TaskResponder = oneshot::Sender<Option<String>>;
type CacheStrResponder = oneshot::Sender<Vec<String>>;
type FinishedResponder = oneshot::Sender<bool>;

#[derive(Debug)]
enum CacheCommand {
    AddCacheNode {
        ipaddr: String,
    },
    QueryComplete {
        cache_node_addr: String,
    },
    SetCacheNodeTarget{
        target: i64,
        should_terminate: bool,
    },
    GetCacheString {
        resp: CacheStrResponder,
    },
    ClearCaches {
        resp: FinishedResponder,
    },
    CaptureCacheTime {
        resp: FinishedResponder,
    },
    RemoveFromRetired {
        cache_node_addr:String,
    }
}

#[derive(Debug)]
enum WorkerCommand {
    AddWorker {
        ipaddr: String,
    },
    FreeWorker {
        ipaddr: String,
    },
    RemoveBusyWorker {
        ipaddr: String,
    },
    SetInstanceTarget{
        target: i64,
        should_terminate : bool,
    },
    MakeRequest {
        request_str: String,
        resp: TaskResponder,
    },
    CaptureWorkerTime {
        resp: FinishedResponder,
    },
    RemoveFromRetired {
        worker_node_addr:String,
    }
}


struct CackleCoordinator {
    client : Vec<Arc<Client>>,
    task_history : Mutex::<History>,
    local_task_history : Mutex::<History>,
    shuffle_history : Mutex::<History>,
    worker_internal_target_history : Mutex::<History>,
    worker_spot_target_history : Mutex::<History>,
    worker_active_history : Mutex::<History>,
    cache_internal_target_history : Mutex::<History>,
    cache_spot_target_history : Mutex::<History>,
    cache_active_history : Mutex::<History>,
    curr_interval_max_num_tasks : AtomicI64,
    curr_num_tasks: AtomicI64,
    local_num_tasks : AtomicI64,
    curr_interval_max_local_tasks : AtomicI64,
    curr_interval_max_shuffle : AtomicI64,
    curr_shuffle_volume: AtomicI64,
    worker_manager_ch : mpsc::Sender<WorkerCommand>,
    cache_manager_ch : mpsc::Sender<CacheCommand>,
    worker_instance_internal_target: AtomicI64,
    worker_instance_spot_target: AtomicI64,
    active_workers: AtomicI64,
    cache_instance_internal_target: AtomicI64,
    cache_instance_spot_target: AtomicI64,
    active_cache :AtomicI64,
    s3_writes :AtomicI64,
    s3_reads :AtomicI64,
    lambda_duration :AtomicI64,
    worker_duration :AtomicI64,
    cache_duration :AtomicI64,
    shutting_down: AtomicBool,

    query_data : HashMap<u32, QueryData>,
    query_name_map : HashMap<String, u32>,
}

impl CackleCoordinator {

    async fn new() -> Arc::<CackleCoordinator>{
        let (tx_worker, rx_worker) = mpsc::channel(256);
        let (tx_cache, rx_cache) = mpsc::channel(256);

        let mut lambda_clients = Vec::with_capacity(128);
        for _ in 0..128 {
            let shared_config = aws_config::from_env().load().await;
            let client = Arc::new(Client::new(&shared_config));
            lambda_clients.push(client);
        }
        let (qdata, qmap) = Self::parse_queries();
        let n = Arc::new(CackleCoordinator{
            client : lambda_clients,
            task_history : Mutex::new(History {
                history: Vec::<i64>::new()
            }),
            local_task_history : Mutex::new(History {
                history: Vec::<i64>::new()
            }),
            shuffle_history : Mutex::new(History {
                history: Vec::<i64>::new()
            }),
            worker_internal_target_history : Mutex::new(History {
                history: Vec::<i64>::new()
            }),
            worker_spot_target_history : Mutex::new(History {
                history: Vec::<i64>::new()
            }),
            worker_active_history : Mutex::new(History {
                history: Vec::<i64>::new()
            }),
            cache_internal_target_history : Mutex::new(History {
                history: Vec::<i64>::new()
            }),
            cache_spot_target_history : Mutex::new(History {
                history: Vec::<i64>::new()
            }),
            cache_active_history : Mutex::new(History {
                history: Vec::<i64>::new()
            }),
            curr_interval_max_num_tasks : AtomicI64::new(0),
            curr_num_tasks: AtomicI64::new(0),
            local_num_tasks : AtomicI64::new(0),
            curr_interval_max_local_tasks : AtomicI64::new(0),
            curr_interval_max_shuffle : AtomicI64::new(0),
            curr_shuffle_volume: AtomicI64::new(0),
            cache_manager_ch: tx_cache,
            worker_manager_ch: tx_worker,
            worker_instance_internal_target: AtomicI64::new(0),
            worker_instance_spot_target: AtomicI64::new(0),
            active_workers:AtomicI64::new(0),
            cache_instance_internal_target: AtomicI64::new(0),
            cache_instance_spot_target: AtomicI64::new(0),
            active_cache:AtomicI64::new(0),
            s3_writes: AtomicI64::new(0),
            s3_reads: AtomicI64::new(0),
            lambda_duration: AtomicI64::new(0),
            worker_duration :AtomicI64::new(0),
            cache_duration :AtomicI64::new(0),
            shutting_down : AtomicBool::new(false),
            query_data : qdata,
            query_name_map : qmap,
        });
        tokio::spawn(n.clone().history_recorder());
        tokio::spawn(n.clone().vm_startup_listener());
        tokio::spawn(n.clone().cache_startup_listener());
        tokio::spawn(n.clone().worker_manager(rx_worker));
        tokio::spawn(n.clone().cache_manager(rx_cache));
        tokio::spawn(n.clone().worker_allocator());
        tokio::spawn(n.clone().shuffle_allocator());
        n
    }


    async fn terminate_nodes(self: Arc<CackleCoordinator>, nodes : Vec::<String>){
        let shared_config = aws_config::from_env().load().await;
        let client = aws_sdk_ec2::Client::new(&shared_config);
        let req = client.describe_instances().set_filters(Some(vec![aws_sdk_ec2::model::Filter::builder().set_name(Some("private-ip-address".to_string())).set_values(Some(nodes)).build()])).send().await.unwrap();
        println!("{:?}", req);
        let mut instance_ids = Vec::new();
        for reservation in req.reservations.unwrap().iter(){
            for instance in reservation.instances.as_ref().unwrap() {
                instance_ids.push(instance.instance_id.as_ref().unwrap().clone());
            }
        }
        for instance_id in instance_ids.iter() {
            println!("Terminating {}", instance_id);
        }
        let terminate_resp = client.terminate_instances().set_instance_ids(Some(instance_ids)).send().await;
        println!("Terminate Response: {:?}",terminate_resp);

    }

    async fn retired_cache_node_cleanup(self : Arc<CackleCoordinator>, cache_node_addr :String){
        time::sleep(Duration::from_secs(90)).await;
        let ret = self.cache_manager_ch.send(CacheCommand::RemoveFromRetired{cache_node_addr:cache_node_addr.clone()}).await;
        match ret {
            Ok(_) => {},
            Err(x) => {println!("Removing From Retired errored on sending request {:?}", x);}
        };
    }

    async fn cache_manager(self : Arc<CackleCoordinator>, mut rx: mpsc::Receiver<CacheCommand>){
        let mut cache_nodes = HashMap::<String, CacheNode>::new();
        let mut retired_nodes = HashMap::<String, CacheNode>::new();

        while let Some(cmd) = rx.recv().await {
            use CacheCommand::*;
            match cmd {
                AddCacheNode {ipaddr} => {
                    let node_addr = ipaddr+":7827";
                    if !(cache_nodes.contains_key(&node_addr) || retired_nodes.contains_key(&node_addr)){
                        println!("Adding Cache Node: {:?}", node_addr);
                        cache_nodes.insert(node_addr, CacheNode{
                            running_queries : AtomicI64::new(0),
                            state : WorkerState::Ready,
                            start_time : SystemTime::now(),
                            close_time : SystemTime::now(),
                        });
                        self.active_cache.fetch_add(1, Ordering::Relaxed);
                    }
                },
                QueryComplete {cache_node_addr} => {
                    let node = match cache_nodes.get(&cache_node_addr){
                        Some(x) => {x},
                        None => {continue;}
                    };
                    let num_queries = node.running_queries.fetch_sub(1, Ordering::Relaxed) -1;
                    if num_queries == 0 && node.state == WorkerState::Closed {
                        let mut terminate_nodes = Vec::<String>::new();
                        let mut node = cache_nodes.remove(&cache_node_addr).unwrap();
                        terminate_nodes.push(cache_node_addr[0..cache_node_addr.len()-5].to_string());
                        node.state = WorkerState::Closed;
                        node.close_time = SystemTime::now();
                        self.cache_duration.fetch_add(node.start_time.elapsed().unwrap().as_millis() as i64, Ordering::Relaxed);
                        retired_nodes.insert(cache_node_addr.clone(), node);
                        tokio::spawn(self.clone().retired_cache_node_cleanup(cache_node_addr));
                        self.active_cache.fetch_sub(1, Ordering::Relaxed);
                        tokio::spawn(self.clone().terminate_nodes(terminate_nodes));
                    }
                },
                RemoveFromRetired {cache_node_addr} => {
                    retired_nodes.remove(&cache_node_addr);
                },
                GetCacheString {resp} => {
                    let mut vec = Vec::new();
                    let num_cache_nodes = 100000;
                    for cache_node in cache_nodes.iter() {
                        if cache_node.1.state == WorkerState::Ready {
                            if vec.len() == num_cache_nodes {
                                break;
                            }
                            vec.push(cache_node.0.clone());
                            cache_node.1.running_queries.fetch_add(1, Ordering::Relaxed);
                        }
                    }

                    let ret = resp.send(vec);
                    //let ret = resp.send(vec!["172.31.67.248:7827".to_string()]);
                    //let ret = resp.send(vec!["172.31.79.128:7827".to_string()]);
                    match ret {
                        Ok(_) => {},
                        Err(x) => {println!("GetCacheString Got error on sending response: {:?}", x);}
                    };

                },
                SetCacheNodeTarget {target, should_terminate} => {
                    println!("Setting Cache Node Target : {:?}", target);
                    self.cache_instance_internal_target.store(target, Ordering::SeqCst);
                    if target != self.cache_instance_spot_target.load(Ordering::SeqCst) {
                        tokio::spawn(self.clone().set_cache_instance_target(target, should_terminate));
                    }
                    let target = target as usize;
                    if cache_nodes.len() > target {
                        let cache_nodes_to_remove = cache_nodes.len()-target;
                        let mut terminate_nodes = Vec::<String>::new();
                        let mut retire_nodes = Vec::<String>::new();
                        for entry in cache_nodes.iter(){
                            if entry.1.start_time.elapsed().unwrap().as_secs() >= 60{
                                retire_nodes.push(entry.0.clone());
                                if retire_nodes.len() == cache_nodes_to_remove{
                                    break;
                                }
                            }
                        }
                        for entry in retire_nodes.iter() {
                            let mut node = cache_nodes.get_mut(entry).unwrap();
                            node.state = WorkerState::Closed;
                            if node.running_queries.load(Ordering::Relaxed) == 0 {
                                let node = cache_nodes.remove(entry).unwrap();
                                println!("terminate node {}", entry[0..entry.len()-5].to_string());
                                terminate_nodes.push(entry[0..entry.len()-5].to_string());
                                self.cache_duration.fetch_add(node.start_time.elapsed().unwrap().as_millis() as i64, Ordering::Relaxed);
                                self.active_cache.fetch_sub(1, Ordering::Relaxed);
                                retired_nodes.insert(entry.clone(), node);
                                tokio::spawn(self.clone().retired_cache_node_cleanup(entry.clone()));
                            }
                            if terminate_nodes.len() == 25 {
                                tokio::spawn(self.clone().terminate_nodes(terminate_nodes));
                                terminate_nodes = Vec::<String>::new();
                            }
                        }
                        if terminate_nodes.len() >0 {
                            tokio::spawn(self.clone().terminate_nodes(terminate_nodes));
                        }
                    }
                },
                ClearCaches {resp} => {
                    for cache_node in cache_nodes.iter() {
                        let conn_str = format!("{}{}", "http://",cache_node.0);
                        let mut client = CacheClient::connect(conn_str).await.unwrap();
                        let request = tonic::Request::new(ClearRequest {});
                        let _response = client.clear(request).await;
                    }
                    let ret = resp.send(true);
                    match ret {
                        Ok(_) => {},
                        Err(x) => {println!("ClearCaches Got error on sending response: {:?}", x);}
                    };
                },
                CaptureCacheTime {resp} => {
                    let mut running_node_time = 0;
                    for node in cache_nodes.iter() {
                        running_node_time += node.1.start_time.elapsed().unwrap().as_millis();
                    }
                    self.cache_duration.fetch_add(running_node_time as i64, Ordering::Relaxed);
                    let ret = resp.send(true);
                    match ret {
                        Ok(_) => {},
                        Err(x) => {println!("CaptureCacheTime Got error on sending response: {:?}", x);}
                    };
                }
            }

        }
        println!("exiting cache manager");
    }

    async fn clear_caches(self: Arc<CackleCoordinator>){
        let (tx, rx) = oneshot::channel::<bool>();
        let ret = self.cache_manager_ch.send(CacheCommand::ClearCaches{resp:tx}).await;
        match ret {
            Ok(_) => {},
            Err(x) => {println!("clear_caches Got error on sending request: {:?}", x);}
        };
        let ret = rx.await;
        match ret {
            Ok(_) => {},
            Err(x) => {println!("clear_caches Got error on fetching response for : {:?}", x);}
        };
    }

    async fn retired_worker_node_cleanup(self : Arc<CackleCoordinator>, worker_node_addr :String){
        time::sleep(Duration::from_secs(90)).await;
        let ret = self.worker_manager_ch.send(WorkerCommand::RemoveFromRetired{worker_node_addr:worker_node_addr.clone()}).await;
        match ret {
            Ok(_) => {},
            Err(x) => {println!("Removing From Retired errored on sending request {:?}", x);}
        };
    }

    async fn worker_manager(self : Arc<CackleCoordinator>, mut rx: mpsc::Receiver<WorkerCommand>){
        let mut idle_workers = HashMap::<String, Worker>::new();
        let mut busy_workers = HashMap::<String, Worker>::new();
        let mut retired_workers = HashMap::<String, Worker>::new();

        while let Some(cmd) = rx.recv().await {
            use WorkerCommand::*;
            match cmd {
                AddWorker {ipaddr} => {
                    let shared_config = aws_config::from_env().endpoint_resolver(Endpoint::immutable(format!("http://{}:3001", ipaddr).parse().expect("valid URI"))).load().await;
                    if !(idle_workers.contains_key(&ipaddr) || busy_workers.contains_key(&ipaddr) || retired_workers.contains_key(&ipaddr)) {
                        println!("Adding Worker Node: {:?}", ipaddr);
                        idle_workers.insert(ipaddr, Worker{
                            client : Client::new(&shared_config),
                            state : WorkerState::Ready,
                            start_time : SystemTime::now(),
                            close_time : SystemTime::now(),
                        });
                        self.active_workers.fetch_add(1, Ordering::Relaxed);
                    }
                },
                FreeWorker {ipaddr} => {
                    let mut worker = busy_workers.remove(&ipaddr).unwrap();
                    worker.state = WorkerState::Ready;
                    idle_workers.insert(ipaddr, worker);

                },
                RemoveBusyWorker {ipaddr} => {
                    let mut worker = busy_workers.remove(&ipaddr).unwrap();
                    worker.state = WorkerState::Ready;
                    retired_workers.insert(ipaddr.clone(), worker);
                    tokio::spawn(self.clone().retired_worker_node_cleanup(ipaddr));
                    self.active_workers.fetch_sub(1, Ordering::Relaxed);
                },
                RemoveFromRetired {worker_node_addr} => {
                    retired_workers.remove(&worker_node_addr);
                },
                SetInstanceTarget{target, should_terminate} => {
                    println!("Setting Worker Target : {:?}", target);
                    self.worker_instance_internal_target.store(target, Ordering::SeqCst);
                    if target != self.worker_instance_spot_target.load(Ordering::SeqCst) {
                        tokio::spawn(self.clone().set_spot_instance_target(target, should_terminate));
                    }
                    let target = target as usize;
                    if idle_workers.len()+busy_workers.len() > target {
                        let workers_to_remove = idle_workers.len()+busy_workers.len()-target;
                        let mut remove_workers = Vec::<String>::new();
                        for entry in idle_workers.iter(){
                            if entry.1.start_time.elapsed().unwrap().as_secs() >= 60{
                                remove_workers.push(entry.0.clone());
                                if remove_workers.len() == workers_to_remove{
                                    break;
                                }
                            }
                        }
                        for worker in remove_workers.iter() {
                            let mut worker_obj = idle_workers.remove(worker).unwrap();
                            worker_obj.close_time = SystemTime::now();
                            worker_obj.state = WorkerState::Closed;
                            self.worker_duration.fetch_add(worker_obj.start_time.elapsed().unwrap().as_millis() as i64, Ordering::Relaxed);
                            tokio::spawn(self.clone().retired_worker_node_cleanup(worker.to_string()));
                            retired_workers.insert(worker.to_string(), worker_obj);
                            self.active_workers.fetch_sub(1, Ordering::Relaxed);
                        }
                        let mut terminate_workers = Vec::<String>::new();
                        let num_remove_workers = remove_workers.len();
                        for idx in 0..num_remove_workers{
                            terminate_workers.push(remove_workers.pop().unwrap());
                            if terminate_workers.len() == 25 {
                                tokio::spawn(self.clone().terminate_nodes(terminate_workers));
                                terminate_workers = Vec::<String>::new();
                            }
                        }
                        if terminate_workers.len() > 0 {
                            tokio::spawn(self.clone().terminate_nodes(terminate_workers));
                        }
                    }
                },
                MakeRequest{request_str,  resp} => {
                    //println!("Got Make Request! {:?}", request_str);
                    let mut found_worker = None;
                    for worker in idle_workers.iter() {
                        found_worker = Some(worker.0.clone());
                        break;
                    }
                    match found_worker {
                        Some(addr) => {
                            let mut worker = idle_workers.remove(&addr).unwrap();
                            let client = worker.client.clone();
                            worker.state = WorkerState::Busy;
                            busy_workers.insert(addr.clone(), worker);
                            let me  = self.clone();
                            tokio::spawn(async move {
                                let response = client.clone().invoke().function_name("lambda_q12").payload(Blob::new(request_str.clone())).send().await;
                                let response = match response{
                                    Ok(x) => {x},
                                    Err(x) => {
                                        println!("MakeRequest Got error on response trying again: {:?}", x);
                                        let ret = me.worker_manager_ch.send(WorkerCommand::RemoveBusyWorker{ipaddr:addr}).await;
                                        let ret = me.worker_manager_ch.send(WorkerCommand::MakeRequest{request_str:request_str, resp:resp}).await;
                                        return
                                    }
                                };
                                let response = match response.payload(){
                                    Some(x) => std::str::from_utf8(x.as_ref()),
                                    None => {
                                        println!("MakeRequest Got error parsing response 1: {:?}", response );
                                        return;
                                    }
                                };
                                let response = match response{
                                    Ok(x) => x,
                                    Err(x) => {
                                        println!("MakeRequest Got error parsing response 2: {:?}", x);
                                        return;
                                    }
                                };

                                let ret = resp.send(Some(response.to_string()));
                                match ret {
                                    Ok(_) => {},
                                    Err(x) => {println!("MakeRequest Got error on sending response: {:?}", x);}
                                };
                                let ret = me.worker_manager_ch.send(WorkerCommand::FreeWorker{ipaddr:addr}).await;
                                match ret {
                                    Ok(_) => {},
                                    Err(x) => {println!("MakeRequest Got error on sending FreeWorker request: {:?}", x);}
                                };
                            });
                        },
                        None => {
                            //println!("Sending Empty");
                            let ret = resp.send(None);
                            match ret {
                                Ok(_) => {},
                                Err(x) => {println!("MakeRequest Got error on sending response: {:?}", x);}
                            };
                        }

                    }
                },
                CaptureWorkerTime {resp} => {
                    let mut running_node_time = 0;
                    for node in idle_workers.iter() {
                        running_node_time += node.1.start_time.elapsed().unwrap().as_millis();
                    }
                    for node in busy_workers.iter() {
                        running_node_time += node.1.start_time.elapsed().unwrap().as_millis();
                    }
                    self.worker_duration.fetch_add(running_node_time as i64, Ordering::Relaxed);
                    let ret = resp.send(true);
                    match ret {
                        Ok(_) => {},
                        Err(x) => {println!("CaptureWorkerTime Got error on sending response: {:?}", x);}
                    };
                }
            }

        }
        println!("exiting worker manager");


    }

    async fn vm_startup_listener(self : Arc<CackleCoordinator>) {
        let listener = TcpListener::bind("0.0.0.0:4545").await.unwrap();

        loop {
            let (socket, _) = listener.accept().await.unwrap();
            //tokio::spawn(self.clone().process_startup_socket(socket));
            let addr = socket.peer_addr().unwrap().ip().to_string();
            let ret = self.worker_manager_ch.send(WorkerCommand::AddWorker{ipaddr:addr}).await;
            match ret {
                Ok(_) => {},
                Err(x) => {println!("vm_startup_listener Got error on sending request: {:?}", x);}
            };
        }
    }

    async fn cache_startup_listener(self : Arc<CackleCoordinator>) {
        let listener = TcpListener::bind("0.0.0.0:4546").await.unwrap();

        loop {
            let (socket, _) = listener.accept().await.unwrap();
            //tokio::spawn(self.clone().process_startup_socket(socket));
            let addr = socket.peer_addr().unwrap().ip().to_string();
            let ret = self.cache_manager_ch.send(CacheCommand::AddCacheNode{ipaddr:addr}).await;
            match ret {
                Ok(_) => {},
                Err(x) => {println!("cache_startup_listener Got error on sending request: {:?}", x);}
            };
        }
    }
    async fn history_recorder(self: Arc<CackleCoordinator>){
        let mut timer = time::interval(Duration::from_secs(1));
        loop {
            timer.tick().await;
            let cur_shuffle = self.curr_shuffle_volume.load(Ordering::SeqCst);
            let max_shuffle = self.curr_interval_max_shuffle.swap(cur_shuffle, Ordering::SeqCst);
            let cur_local_tasks = self.local_num_tasks.load(Ordering::SeqCst);
            let max_local_tasks = self.curr_interval_max_local_tasks.swap(cur_local_tasks, Ordering::SeqCst);
            let cur_tasks = self.curr_num_tasks.load(Ordering::SeqCst);
            let max_tasks = self.curr_interval_max_num_tasks.swap(cur_tasks, Ordering::SeqCst);
            let curr_cache_internal_target = self.cache_instance_internal_target.load(Ordering::Relaxed);
            let curr_cache_spot_target = self.cache_instance_spot_target.load(Ordering::Relaxed);
            let curr_cache_nodes = self.active_cache.load(Ordering::Relaxed);
            let curr_worker_internal_target = self.worker_instance_internal_target.load(Ordering::Relaxed);
            let curr_worker_spot_target = self.worker_instance_spot_target.load(Ordering::Relaxed);
            let curr_worker_nodes = self.active_workers.load(Ordering::Relaxed);
            println!("current local tasks: {:?}", max_local_tasks);
            {
                let mut history = self.local_task_history.lock().await;
                history.history.push(max_local_tasks);
            }

            println!("current tasks: {:?}", max_tasks);
            {
                let mut history = self.task_history.lock().await;
                history.history.push(max_tasks);
            }
            println!("current shuffle: {:?}KiB", max_shuffle/1024 );
            {
                let mut history = self.shuffle_history.lock().await;
                history.history.push(max_shuffle);
            }
            println!("current cache internal target: {:?}", curr_cache_internal_target);
            {
                let mut history = self.cache_internal_target_history.lock().await;
                history.history.push(curr_cache_internal_target);
            }
            println!("current cache spot target: {:?}", curr_cache_spot_target);
            {
                let mut history = self.cache_spot_target_history.lock().await;
                history.history.push(curr_cache_spot_target);
            }
            println!("current cache nodes: {:?}", curr_cache_nodes);
            {
                let mut history = self.cache_active_history.lock().await;
                history.history.push(curr_cache_nodes);
            }
            println!("current worker internal target: {:?}", curr_worker_internal_target);
            {
                let mut history = self.worker_internal_target_history.lock().await;
                history.history.push(curr_worker_internal_target);
            }
            println!("current worker spot target: {:?}", curr_worker_spot_target);
            {
                let mut history = self.worker_spot_target_history.lock().await;
                history.history.push(curr_worker_spot_target);
            }
            println!("current worker nodes: {:?}", curr_worker_nodes);
            {
                let mut history = self.worker_active_history.lock().await;
                history.history.push(curr_worker_nodes);
            }
        }
    }

    async fn shuffle_allocator(self: Arc<CackleCoordinator>){
        let mut timer = time::interval(Duration::from_secs(5));
        timer.tick().await;
        let target_pctl = vec![0.98];
        while !self.shutting_down.load(Ordering::Relaxed) {
            {
                let history;
                {
                    let history_orig = &self.shuffle_history.lock().await.history;
                    let lookback : i64 = 2400;
                    let begin= std::cmp::max(0, history_orig.len() as i64-lookback) as usize;
                    history = Vec::from_iter(history_orig[begin..].iter().cloned());
                }
                if history.len() == 0{
                    continue;
                }
                let node_size = 3*1024*1024*1024+512*1024*1024;
                let shuffle_mean = history.iter().sum::<i64>()/history.len() as i64;
                let pctl_data_struct : Percentiles<f64> = history.iter().map(|v| *v as f64).collect();
                let mut percentiles = pctl_data_struct.percentiles(&target_pctl).unwrap().unwrap();
                let mult = 2;
                let min_num_workers = 2;
                let mut num_workers :i64 = percentiles[0] as i64 * mult / node_size + 1;
                num_workers = std::cmp::max(num_workers, min_num_workers);
                let ret = self.cache_manager_ch.send(CacheCommand::SetCacheNodeTarget{target: num_workers, should_terminate: false}).await;
                match ret {
                    Ok(_) => {},
                    Err(x) => {println!("shuffle_allocator Got error on sending request: {:?}", x);}
                };
            }
            timer.tick().await;
        }
        println!("Shuffle Allocator Has Stopped");
    }   

    async fn worker_allocator(self: Arc<CackleCoordinator>){
        let vm_startup_time = 180;
        let freq :usize= 5;
        let mut timer = time::interval(Duration::from_secs(freq as u64));
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
        timer.tick().await;
        let mut intervention = 0;
        while !self.shutting_down.load(Ordering::Relaxed) {
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
                    let history_orig = &self.task_history.lock().await.history;
                    let begin= std::cmp::max(0, history_orig.len() as i64-lookback) as usize;
                    history = Vec::from_iter(history_orig[begin..].iter().cloned());
                }
                if history.len() < freq {
                    continue;
                }
                if history.len() < lookback as usize{
                    let num_to_add = lookback as usize - history.len();
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
                    hist_alloc[(intervention%(vm_startup_time/freq))as usize][lookback_idx][strat_idx] = curr_options[strat_idx]
                }
            }
            if total_spot_time == 0{
                best_target = hist_alloc[(intervention%(vm_startup_time/freq))as usize][1][99];
            }
            println!("Worker allocator chose {} {}", chosen_lookback_idx, chosen_strat_idx);
            let num_workers = std::cmp::max(best_target, min_num_workers);
            let ret = self.worker_manager_ch.send(WorkerCommand::SetInstanceTarget{target: num_workers, should_terminate : false}).await;
            match ret {
                Ok(_) => {},
                Err(x) => {println!("worker_allocator Got error on sending request: {:?}", x);}
            };
            intervention+=1;
            timer.tick().await;
        }
        println!("Worker Allocator Has Stopped");
    } 



    fn process_result(self: Arc<CackleCoordinator>, is_lambda :bool, result : String, req_str : &String) -> (i64, bool, Option<String>) {
        /*
           println!("Printing Result lambda? :{:?}", is_lambda);
           println!("result str {:?}", result);
           */
        let split = result.split(" ");
        let vec : Vec<&str>= split.collect();
        let mut errored = false;
        if vec.len() < 10 {
            println!("Unexpected response from worker: {:?} start args {:?}", result, req_str);
            return (-1, true, None);
        }
        //println!("split result {:?}", vec);
        let duration_ms = vec[3].parse::<i64>();
        let s3_reads = vec[7].parse::<i64>();
        let s3_writes = vec[8].parse::<i64>();
        let shuffle_size = vec[9].parse::<i64>();
        let parsed_ints = vec![&duration_ms, &s3_reads, &s3_writes, &shuffle_size];
        for val in parsed_ints {
            match val {
                Ok(_) =>{},
                Err(x) => {
                    println!("{:?}", x);
                    errored = true;
                },
            }
        }
        if errored {
            println!("Unexpected response from worker: {:?} start args {:?}", result, req_str);
            return (-1, true, None);
        }
        let cached_results = if errored {
            None
        }else if vec.len() >= 11{
            Some(vec[10][..vec[10].len()-1].to_string())
        } else {
            None
        };
        let duration_ms = duration_ms.unwrap();
        let s3_reads = s3_reads.unwrap();
        let s3_writes = s3_writes.unwrap();
        let shuffle_size = shuffle_size.unwrap();
        println!("Successful response from worker: {:?} start args {:?}", result, req_str);

        if is_lambda {
            self.lambda_duration.fetch_add(duration_ms, Ordering::Relaxed);
        }
        self.s3_reads.fetch_add(s3_reads, Ordering::Relaxed);
        self.s3_writes.fetch_add(s3_writes, Ordering::Relaxed);
        let curr_shuffle = self.curr_shuffle_volume.fetch_add(shuffle_size, Ordering::Relaxed) +shuffle_size;
        if curr_shuffle > self.curr_interval_max_shuffle.load(Ordering::SeqCst) {
            self.curr_interval_max_shuffle.store(curr_shuffle, Ordering::SeqCst);
        }
        (shuffle_size, s3_writes > 0, cached_results)

    }

    // Returns shuffle size written, and if written to cache
    async fn run_task(self : Arc<CackleCoordinator>, qdata: Arc<QueryData>, q_exec_data: Arc<QueryExecData>, stage: u32, query_id: Arc::<String>, cache_string: Arc::<String>, cache_inputs: String ,task_num: u32) -> Result<(i64, Option<String>, u32, Option<String>), Error>{
        static LOCAL_ARN :&str = "arn:aws:lambda:us-east-1:762196318022:function:lambda_q12";
        static OVERFLOW_ARN :&str = "arn:aws:lambda:us-east-1:762196318022:function:query_overflow";
        let cur_tasks = self.curr_num_tasks.fetch_add(1, Ordering::Relaxed) +1;
        if cur_tasks > self.curr_interval_max_num_tasks.load(Ordering::SeqCst) {
            self.curr_interval_max_num_tasks.store(cur_tasks, Ordering::SeqCst);
        }
        //let req_str = format!("\"./q12 {} {} 8 4 2 {}\"" ,stage, query_id, task_num);
        let req_str = format!("\"./qall {} {} {} {} {} {} {} \"" , qdata.query_name, stage, query_id, qdata.constants, task_num, cache_string, cache_inputs);
        println!("Task Starting: {}", req_str);
        let mut run_by_large = None;
        if !q_exec_data.use_overflow {
            let local_tasks = self.local_num_tasks.fetch_add(1, Ordering::Relaxed) +1;
            if local_tasks > self.curr_interval_max_local_tasks.load(Ordering::SeqCst) {
                self.curr_interval_max_local_tasks.store(cur_tasks, Ordering::SeqCst);
            }
            let (tx, rx) = oneshot::channel::<Option<String>>();
            let ret = self.worker_manager_ch.send(WorkerCommand::MakeRequest {request_str:req_str.clone(), resp:tx}).await;
            match ret {
                Ok(_) => {},
                Err(x) => {println!("run_task Got error on sending MakeRequest: {:?}", x);}
            };
            let run_by_large_tmp = rx.await;
            run_by_large = {
            //run_by_large :Option<String> = {
                match run_by_large_tmp {
                    Ok(x) => {x},
                    Err(x) => {println!("{:?}", x); None}
                }
            };
        }
        let s3_str = Some(format!("{}_pt{}_{}_", task_num, stage, query_id));

        let parsed_result = match run_by_large {
            Some(x) => {
                self.curr_num_tasks.fetch_sub(1, Ordering::Relaxed);
                self.local_num_tasks.fetch_sub(1, Ordering::Relaxed);
                self.process_result(false, x, &req_str)
            },
            None => {
                let arn = if q_exec_data.use_overflow {
                    OVERFLOW_ARN
                } else {
                    LOCAL_ARN
                };
                let num = rand::thread_rng().gen_range(0..128);
                let response = match self.client[num].invoke().function_name(arn).payload(Blob::new(req_str.clone())).send().await {
                    Ok(x) => {x},
                    Err(_) => {
                        let num = rand::thread_rng().gen_range(0..128);
                        time::sleep(Duration::from_secs(3)).await;
                        match self.client[num].invoke().function_name(arn).payload(Blob::new(req_str.clone())).send().await {
                            Ok(x) => {x},
                            Err(x) => {
                                self.curr_num_tasks.fetch_sub(1, Ordering::Relaxed);
                                if !q_exec_data.use_overflow {
                                    self.local_num_tasks.fetch_sub(1, Ordering::Relaxed);
                                }
                                println!("error task_result: {} {}", req_str, x);
                                return Ok((-1, s3_str, task_num, None ))
                            },
                        }
                    }
                };
                if !q_exec_data.use_overflow {
                    self.local_num_tasks.fetch_sub(1, Ordering::Relaxed);
                }
                self.curr_num_tasks.fetch_sub(1, Ordering::Relaxed);
                let response = match response.payload(){
                    Some(x) => {x},
                    None => {
                        println!("error task_result: {}", req_str);
                        return Ok((-1, s3_str, task_num, None))
                    },
                };

                self.process_result(true, std::str::from_utf8(response.as_ref()).unwrap().to_string(), &req_str)
            }
        };
        println!("run_task parsed_result.2 {:?}", parsed_result.2);
        Ok((parsed_result.0, 
            match parsed_result.1 {
                true => {
                    s3_str
                }, false => {
                    None
                },
            },
             task_num ,parsed_result.2))
    }


    async fn get_cache_inputs(self: Arc<CackleCoordinator>, curr_stage :u32, qdata: Arc<QueryData>, q_exec_data : Arc<QueryExecData>) -> String {
        let mut dep_vec = Vec::new();
        for dep in &qdata.query_deps[&curr_stage] {
            let map = q_exec_data.stage_shuffle_map.lock().await;
            let cache_map = &map.get(&dep).unwrap().cache_written;
            let mut out_vec = Vec::new();
            for cache_entry in cache_map {
                out_vec.push(format!("{}:{}", cache_entry.0, cache_entry.1.iter().map(|x| x.to_string()).collect::<Vec<String>>().join(",")));
            }
            if out_vec.len() > 0 {
                dep_vec.push(format!("{}%{}",dep, out_vec.join(";")));
            }
        }
        dep_vec.join("#")
    }

    async fn run_stage(self: Arc<CackleCoordinator>, tx : watch::Sender<bool>, qdata: Arc<QueryData>, q_exec_data : Arc<QueryExecData>, query_id: Arc<String>, cache_str: Arc<String>, curr_stage: u32){
        let mut errored = false;
        println!("trying to run stage {} from query {} with id {}", curr_stage, qdata.query_name, query_id );
        //wait on stages we depend on
        for dep_id in qdata.query_deps.get(&curr_stage).unwrap().iter() {
            let mut rx = q_exec_data.watch_channels.get(&dep_id).unwrap().clone();
            let ret = rx.changed().await;
            match ret {
                Ok(_) => {},
                Err(x) => {println!("run_stage Got error on fetching watch: {:?}", x);}
            };
            let success = *rx.borrow_and_update();
            if !success{
                errored = true;
            }
        }
        let start_time = Instant::now();
        let mut tasks = Vec::new();
        let cache_inputs = self.clone().get_cache_inputs(curr_stage, qdata.clone(), q_exec_data.clone()).await;
        println!("starting stage {} from query {} with id {} cache_inputs {}", curr_stage, qdata.query_name, query_id, cache_inputs );
        
        if !errored{
            for task_num in 0..qdata.task_nums[curr_stage as usize] {
                tasks.push(self.clone().run_task(qdata.clone(), q_exec_data.clone(), curr_stage, query_id.clone(),  cache_str.clone(), cache_inputs.clone(), task_num));
            }
        }
        let task_res = join_all(tasks).await;
        let mut task_bytes = 0;
        let mut s3_written = Vec::new();
        // node_id -> partitions
        let mut cache_map =  HashMap::<u32, HashSet<u32>>::new();
        
        for result in task_res {
            let inner_result = match result.as_ref(){
                Ok(x) => {x.clone()},
                Err(x) => {
                    println!("got error from task {:?}", x);
                    errored=true;
                    (-1, None, 0, None)
                }
            };
            if inner_result.0 < 0 {
                errored = true;
            }else {
                task_bytes += inner_result.0
            }
            if let Some(x) = inner_result.1 {
                s3_written.push(x);
            }
            let in_partition = inner_result.2;
            if let Some(cache_str) = inner_result.3 {
                println!("stage {} cache_str {}", curr_stage, cache_str);
                let node_str_vec = cache_str.split(";").filter(|x| x.len() > 0).map(|x| x.to_string()).collect::<Vec<String>>();
                for node_str in node_str_vec {
                    let split_node_str = node_str.split(":").filter(|x| x.len() > 0).map(|x| x.to_string()).collect::<Vec<String>>();
                    let cache_node_id = split_node_str[0].parse::<u32>().unwrap();
                    let out_part_ids = split_node_str[1].split(",").filter(|x| x.len() > 0).map(|x| x.parse::<u32>().unwrap()).collect::<Vec<u32>>();
                    for out_part_id in out_part_ids {
                        cache_map.entry(cache_node_id).and_modify(|x| {x.insert(out_part_id);})
                            .or_insert({let mut partitions = HashSet::new();partitions.insert(out_part_id);partitions});
                    }
                }
            }
        }
        println!("stage {} cache_written {:?}", curr_stage, cache_map);

        let written_data = StageIntermediateData{
            s3_keys_written: s3_written,
            cache_written : cache_map,
            shuffle_size : task_bytes,
        };

        {
            let mut exec_map = q_exec_data.stage_shuffle_map.lock().await;
            exec_map.insert(curr_stage, written_data);
        }
        let nd_drop = qdata.query_nd_map.get(&curr_stage);
        for drop_stage in qdata.query_drop_map.get(&curr_stage).unwrap() {
            let should_drop_cache = errored || match nd_drop{
                Some(x)=> {x.contains(drop_stage)},
                None => true
            };
            let int_data;
            {
                let mut exec_map = q_exec_data.stage_shuffle_map.lock().await;
                int_data = exec_map.remove(&drop_stage).unwrap();
            }
            println!("stage {} dropping stage {} output should drop cache? {}", curr_stage, drop_stage, should_drop_cache);
            tokio::spawn(self.clone().drop_shuffle(int_data, cache_str.clone(), should_drop_cache, *drop_stage, query_id.to_string()));
        }


        if errored {
            println!("errored stage {} from query {} with id {} duration {:?} num_tasks {}", curr_stage, qdata.query_name, query_id , start_time.elapsed(), qdata.task_nums[curr_stage as usize]);
        } else {
            println!("completed stage {} from query {} with id {} duration {:?} num_tasks {}", curr_stage, qdata.query_name, query_id , start_time.elapsed(), qdata.task_nums[curr_stage as usize]);
        }
        let ret = tx.send(!errored);
        match ret {
            Ok(_) => {},
            Err(x) => {println!("run_stage Got error on sending watch: {:?}", x);}
        };
    }


    async fn drop_shuffle(self : Arc<CackleCoordinator>, int_data : StageIntermediateData , cache_str: Arc<String>, drop_cache : bool, stage_id: u32, query_id: String){
        self.curr_shuffle_volume.fetch_sub(int_data.shuffle_size, Ordering::SeqCst);
        if drop_cache {
            let cache_nodes = cache_str.split(";").filter(|x| x.len()>0).collect::<Vec<&str>>();
            let mut futs = Vec::new();
            for node_id in 0..cache_nodes.len() {
                let conn_str = format!("http://{}", cache_nodes.get(node_id).unwrap());
                let try_client = CacheClient::connect(conn_str.clone()).await;
                match try_client {
                    Ok(client) => {
                        let node_data = int_data.cache_written.get(&(node_id as u32));
                        if let Some(node_data) = node_data {
                            for out_partition in node_data {
                                let key = format!("{}_pt{}_{}",query_id, stage_id, out_partition);
                                let mut client = client.clone();
                                futs.push(tokio::spawn(async move {
                                    let mut tries : u32 = 5;
                                    let mut request = tonic::Request::new(DropRequest {key:Some(key.clone()), should_write:Some(false)});

                                    while tries > 0 {
                                        let rep = client.drop(request).await;
                                        match rep {
                                            Ok(_) => {
                                                println!("drop of {} succeeded", key);
                                                break;
                                            },
                                            Err(x) => {
                                                time::sleep(Duration::from_millis(200)).await;
                                                request = tonic::Request::new(DropRequest {key:Some(key.clone()), should_write:Some(false)});
                                                println!("reply from {} dropping {:?}", key, x);
                                            }
                                        }
                                        tries-=1;
                                    }
                                }));
                            }
                        }
                    }, Err(_) => {}
                };
            }
            join_all(futs).await;
        }
        let shared_config = aws_config::from_env().load().await;
        let client = aws_sdk_s3::Client::new(&shared_config);
        let mut delete_objects: Vec<ObjectIdentifier> = vec![];
        println!("trying to delete : {:?}", int_data.s3_keys_written);
        for key in int_data.s3_keys_written.iter() {
            for extra in 0..2{
                let obj_id = ObjectIdentifier::builder().set_key(Some(format!("{}{}", key,extra).to_string())).build();
                delete_objects.push(obj_id);
            }
        }
        if delete_objects.len() > 0{
            println!("before_delete");
            let ret = client.delete_objects().bucket("mit-lambda-networking-exp-2").delete(Delete::builder().set_objects(Some(delete_objects)).build()).send().await;
            match ret {
                Ok(_) => {},
                Err(x) => {println!("drop_suffle Got error on delete request: {:?}", x);}
            };
            println!("after_delete");
        }


    }

    fn get_query_id() -> String{
        rand::thread_rng()
                        .sample_iter(&Alphanumeric)
                        .take(10)
                        .map(char::from)
                        .collect::<String>()
    }

    async fn run_named_query(self :Arc<CackleCoordinator>, name :String) -> QueryExecution {
        let qdata = self.query_data[&self.query_name_map[&name.to_string()]].clone();
        let start_time = Instant::now();
        let mut query_id = Self::get_query_id();
        let mut attempt = 0;
        let mut failure = true;
        let mut run_on_overflow = true;
        while failure && attempt < 5 {
            let resp = self.clone().run_query(Arc::new(query_id.clone()), Arc::new(qdata.clone())).await;
            (failure, run_on_overflow) = resp;
            attempt += 1;
            if failure && attempt < 5 {
                let new_query_id = Self::get_query_id();
                println!("Query {} with id {} Failed, retrying attempt {} with new id {}", name, query_id, attempt, new_query_id);
                query_id = new_query_id;
                time::sleep(Duration::from_secs(5)).await;
            }
        }
        let ret = QueryExecution{
            start_time:start_time,
            duration:start_time.elapsed(),
            num_attempts : attempt,
            query_id: name,
            overflow_exec: run_on_overflow,
            is_error : failure,
        };
        println!("{:?}", ret);
        ret
    }

    async fn run_query(self :Arc<CackleCoordinator>, query_id: Arc<String>, qdata :Arc<QueryData>) -> (bool, bool){
        //let should_use_overflow = self.local_num_tasks.load(Ordering::SeqCst) - self.active_workers.load(Ordering::SeqCst) > 200;
        let should_use_overflow = false;
        println!("Starting Query {} with id {} on overflow {}", qdata.query_name, query_id, should_use_overflow);
        let (tx, rx) = oneshot::channel::<Vec<String>>();
        let ret = self.cache_manager_ch.send(CacheCommand::GetCacheString{resp : tx}).await;
        match ret {
            Ok(_) => {},
            Err(x) => {println!("run_query Got error sending to cache string: {:?}", x);}
        };
        let str_vec = rx.await.unwrap();
        let cache_string = {if should_use_overflow {
                Arc::new("".to_string())
            }else {
                Arc::new(str_vec.join(";"))
            }
        };
        //TODO: Remove
        //let cache_string = Arc::new("172.31.67.248:7827;172.31.67.248:7828;172.31.67.248:7826;172.31.67.248:7829".to_string());
        //let cache_string = Arc::new("".to_string());
        let num_tasks = qdata.task_nums.len();
        let mut senders = Vec::new();
        let mut dep_waits = HashMap::new();
        for task in 0..num_tasks{
            let (tx, rx) = watch::channel(false);
            senders.push(tx);
            dep_waits.insert(task as u32, rx);
        }
        let mut last_task_recv = dep_waits.get(&(num_tasks as u32 -1)).unwrap().clone();
        let q_exec_data = Arc::new(QueryExecData{ stage_shuffle_map : Mutex::new(HashMap::new()), watch_channels : dep_waits, use_overflow :should_use_overflow});
        for task in (0..num_tasks).rev() {
            tokio::spawn(self.clone().run_stage(senders.pop().unwrap(), qdata.clone(), q_exec_data.clone(), query_id.clone(), cache_string.clone(), task as u32));
        }
        let ret = last_task_recv.changed().await;
        match ret {
            Ok(_) => {},
            Err(x) => {println!("run_query Got error waiting for last stage: {:?}", x);}
        };
        let query_succeeded = *last_task_recv.borrow_and_update();
        if query_succeeded {
            println!("Query Succeeded {} with id {}", qdata.query_name, query_id);
        } else {
            println!("Query Failed: {:?}", query_id);

        }
        for cache_node in str_vec {
            let ret = self.cache_manager_ch.send(CacheCommand::QueryComplete{cache_node_addr:cache_node}).await;
            match ret {
                Ok(_) => {},
                Err(x) => {println!("run_query Got error sending query complete request to cache node: {:?}", x);}
            };
        }
        (!query_succeeded, should_use_overflow)
    }


    async fn run_schedule(self: Arc<CackleCoordinator>,schedule: Vec<(u64, Vec<String>)>) -> Vec<QueryExecution>{
        let start_time = Instant::now();
        let queries_size = schedule.iter().map(|x| x.1.len() as u64).sum::<u64>();
        let mut queries = Vec::with_capacity(queries_size.try_into().unwrap());
        for tup in schedule {
            time::sleep_until(start_time+Duration::from_secs(tup.0)).await;
            for qname in tup.1{
                println!("starting query {}", qname);
                queries.push(tokio::spawn(self.clone().run_named_query(qname))) ;
            }
        }
        let resp = join_all(queries).await;
        let mut ret = Vec::new();
        for rep in resp{
            match rep {
                Ok(x) => {
                    ret.push(x);
                },
                Err(_) => {},
            }
        }
        ret
    }

    async fn set_cache_instance_target(self: Arc<CackleCoordinator>, num_instances : i64, terminate :bool){
        println!("Trying to set Cache instance target to {:?}", num_instances);
        //let num_instances = std::cmp::min(num_instances, 2);
        let num_instances = std::cmp::min(num_instances, 50);
        let shared_config = aws_config::from_env().load().await;
        let client = aws_sdk_ec2::Client::new(&shared_config);
        let req = client.modify_spot_fleet_request().spot_fleet_request_id("sfr-fb992da1-baf1-41ec-8dde-f27ab5ebb59e").set_target_capacity(Some(num_instances as i32));
        let built;
        if !terminate {
            built = req.set_excess_capacity_termination_policy(Some(aws_sdk_ec2::model::ExcessCapacityTerminationPolicy::NoTermination));
        } else {
            built = req;
        }
        let resp = built.send().await;
        println!("Got Response {:?}", resp);
        match resp {
            Ok(_) => {
                self.cache_instance_spot_target.store(num_instances, Ordering::SeqCst);
            },
            Err(_) => {},
        }
    }
    
    async fn set_spot_instance_target(self: Arc<CackleCoordinator>, num_instances : i64, terminate :bool){
        println!("Trying to set worker instance target to {:?}", num_instances);
        //let num_instances = std::cmp::min(num_instances, 0);
        let num_instances = std::cmp::min(num_instances, 700);
        let shared_config = aws_config::from_env().load().await;
        let client = aws_sdk_ec2::Client::new(&shared_config);
        let req = client.modify_spot_fleet_request().spot_fleet_request_id("sfr-0bc80050-d94b-48c3-a7ff-d6de41251d4f").set_target_capacity(Some(num_instances as i32));
        let built;
        if !terminate {
            built = req.set_excess_capacity_termination_policy(Some(aws_sdk_ec2::model::ExcessCapacityTerminationPolicy::NoTermination));
        } else {
            built = req;
        }
        let resp = built.send().await;
        match resp {
            Ok(_) => {
                self.worker_instance_spot_target.store(num_instances, Ordering::SeqCst);
            },
            Err(_) => {},
        }
        println!("{:?}", resp);
    }

    async fn print_finished(self :Arc<CackleCoordinator>) {
        let (tx, rx) = oneshot::channel::<bool>();
        let ret = self.cache_manager_ch.send(CacheCommand::CaptureCacheTime{resp:tx}).await;
        match ret {
            Ok(_) => {},
            Err(x) => {println!("print_finished Got error sending CaptureCacheTime Request: {:?}", x);}
        };
        let ret = rx.await;
        match ret {
            Ok(_) => {},
            Err(x) => {println!("print_finished Got error receiving CaptureCacheTime response: {:?}", x);}
        };
        let (tx, rx) = oneshot::channel::<bool>();
        let ret = self.worker_manager_ch.send(WorkerCommand::CaptureWorkerTime{resp:tx}).await;
        match ret {
            Ok(_) => {},
            Err(x) => {println!("print_finished Got error sending CaptureWorkerTime Request: {:?}", x);}
        };
        let ret = rx.await;
        match ret {
            Ok(_) => {},
            Err(x) => {println!("print_finished Got error sending CaptureWorkerTime Response: {:?}", x);}
        };

        {
        let task_history = &self.task_history.lock().await.history;
        println!("task_history: {:?}", task_history); 
        }
        {
        let shuffle_history = &self.shuffle_history.lock().await.history;
        println!("shuffle_history: {:?}", shuffle_history); 
        }
        {
        let worker_internal_target_history = &self.worker_internal_target_history.lock().await.history;
        println!("worker_internal_target_history: {:?}", worker_internal_target_history);
        }
        {
        let worker_spot_target_history = &self.worker_spot_target_history.lock().await.history;
        println!("worker_spot_target_history: {:?}", worker_spot_target_history);
        }
        {
        let worker_active_history = &self.worker_active_history.lock().await.history;
        println!("worker_active_history: {:?}", worker_active_history); 
        }
        {
        let cache_internal_target_history = &self.cache_internal_target_history.lock().await.history;
        println!("cache_internal_target_history: {:?}", cache_internal_target_history);
        }
        {
        let cache_spot_target_history = &self.cache_spot_target_history.lock().await.history;
        println!("cache_spot_target_history: {:?}", cache_spot_target_history);
        }
        {
        let cache_active_history = &self.cache_active_history.lock().await.history;
        println!("cache_active_history: {:?}", cache_active_history); 
        }
        let lambda_cost_s : f64 = 0.00005;
        let spot_cost_s : f64 = 0.04/(60.0*60.0);
        let s3_write_price : f64 = 0.005/1000.0;
        let s3_read_price : f64 = 0.0004/1000.0;
        
        println!("");

        let num_s3_write = self.s3_writes.load(Ordering::Relaxed);
        let s3_write_cost = s3_write_price * num_s3_write as f64;
        println!("s3_writes: {:?} {:?}", num_s3_write, s3_write_cost);
        let num_s3_read = self.s3_reads.load(Ordering::Relaxed);
        let s3_read_cost = s3_read_price * num_s3_read as f64;
        println!("s3_reads: {:?} {:?}", num_s3_read, s3_read_cost);
        let cache_duration_s = self.cache_duration.load(Ordering::Relaxed)/1000;
        let cache_cost = cache_duration_s as f64 * spot_cost_s;
        println!("cache_duration_s: {:?} {:?}", cache_duration_s, cache_cost);

        println!("");
        let worker_duration_s = self.worker_duration.load(Ordering::Relaxed)/1000;
        let worker_cost = worker_duration_s as f64 * spot_cost_s;
        println!("worker_duration_s: {:?} {:?}", worker_duration_s, worker_cost);
        let lambda_duration_s = self.lambda_duration.load(Ordering::Relaxed)/1000;
        let lambda_cost = lambda_duration_s as f64 * lambda_cost_s;
        println!("lambda_duration_s: {:?} {:?}", lambda_duration_s, lambda_cost);
        println!("");

        let shuffle_cost = s3_write_cost + s3_read_cost + cache_cost;
        let compute_cost = lambda_cost + worker_cost;
        let total_cost = shuffle_cost + compute_cost;
        println!("shuffle_cost: {:?}", shuffle_cost);
        println!("compute_cost: {:?}", compute_cost);
        println!("total_cost : {:?}", total_cost);


    }

    async fn shutdown(self : Arc::<CackleCoordinator>){
        self.shutting_down.store(true, Ordering::Relaxed);
        time::sleep(Duration::from_secs(10)).await;
        let ret = self.worker_manager_ch.send(WorkerCommand::SetInstanceTarget{target: 0, should_terminate : true}).await;
        match ret {
            Ok(_) => {},
            Err(x) => {println!("shutdown Got error sending SetInstanceTarget Response: {:?}", x);}
        };

        let ret = self.cache_manager_ch.send(CacheCommand::SetCacheNodeTarget{target: 0, should_terminate : true}).await;
        match ret {
            Ok(_) => {},
            Err(x) => {println!("shutdown Got error sending SetCacheNodeTarget Response: {:?}", x);}
        };

        time::sleep(Duration::from_secs(60)).await;
        let ret = self.worker_manager_ch.send(WorkerCommand::SetInstanceTarget{target: 0, should_terminate : true}).await;
        match ret {
            Ok(_) => {},
            Err(x) => {println!("shutdown Got error sending SetInstanceTarget Response: {:?}", x);}
        };
        let ret = self.cache_manager_ch.send(CacheCommand::SetCacheNodeTarget{target: 0, should_terminate : true}).await;
        match ret {
            Ok(_) => {},
            Err(x) => {println!("shutdown Got error sending SetCacheNodeTarget Response: {:?}", x);}
        };
        time::sleep(Duration::from_secs(60)).await;

    }

    fn generate_drop_map_helper(dep_map : &HashMap<u32, Vec<u32>>, total_deps: &HashMap<u32, u32>, drop_map : &mut HashMap<u32,Vec<u32>>, curr_stage : u32) -> HashMap::<u32,u32>{
        let mut dep_count = HashMap::<u32,u32>::new();

        if drop_map.contains_key(&curr_stage) {
            return dep_count
        }
        drop_map.insert(curr_stage, Vec::new());
        for dep in dep_map[&curr_stage].iter() {
            let dep_dep_count = Self::generate_drop_map_helper(dep_map, total_deps, drop_map, *dep);
            for pair in dep_dep_count.iter(){
                dep_count.entry(*pair.0).and_modify(|x| *x+=*pair.1).or_insert(*pair.1);
                if total_deps[pair.0] > *pair.1 && dep_count[pair.0] == total_deps[pair.0] {
                    drop_map.get_mut(&curr_stage).unwrap().push(*pair.0);
                }
            }
        }
        for dep in dep_map[&curr_stage].iter() {
            dep_count.entry(*dep).and_modify(|x| *x+=1).or_insert(1);
            if dep_count[dep] == total_deps[dep]{
                drop_map.get_mut(&curr_stage).unwrap().push(*dep);
            }
        }
        dep_count
    }

    fn generate_drop_map(dep_map : &HashMap<u32, Vec<u32>>) -> HashMap<u32,Vec<u32>> {
        let mut total_deps = HashMap::<u32, u32>::new();
        let mut drop_map = HashMap::<u32,Vec<u32>>::new();
        for stage in dep_map{
            for dep in stage.1{
                total_deps.entry(*dep).and_modify(|x| *x+=1).or_insert(1);
            }
        }
        for entry in total_deps.iter() {
            if entry.1 > &1 {
                //println!("{:?}: {:?}", entry.0, entry.1);
            }
        }
        let last_stage = dep_map.len() as u32 - 1;
        Self::generate_drop_map_helper(dep_map, &mut total_deps, &mut drop_map, last_stage);
        drop_map.get_mut(&last_stage).unwrap().push(last_stage);
        drop_map
    }

    fn parse_queries() -> (HashMap<u32, QueryData>, HashMap<String, u32>){
        let mut query_data = HashMap::<u32, QueryData>::new();
        let mut query_name_map = HashMap::<String ,u32>::new();
		let dir_path = "./queries/";
        let mut internal_qnum = 0;
		if let Ok(entries) = fs::read_dir(dir_path) {
            let mut entries = entries.collect::<Result<Vec<_>, _>>().unwrap();
            entries.sort_by_key(|x| x.file_name());
			for entry in entries {
                let path = entry.path();
                if path.is_file() {
                    let file_name = path.file_stem().unwrap().to_string_lossy().to_string();
                    if file_name.len() > 3 {
                        continue;
                    }
                    let file_contents = fs::read_to_string(&path).unwrap();
                    let mut section = 0;
                    let mut vars = HashMap::<String, u32>::new();
                    let mut constant_string = "".to_string();
                    let mut dep_map = HashMap::<u32,Vec<u32>>::new();
                    let mut dep_nd_map = HashMap::<u32,Vec<u32>>::new();
                    let mut task_num_vec = Vec::<u32>::new();
                    for line in file_contents.split("\n"){
                        if line == "".to_string(){
                            section += 1;
                            continue;
                        }
                        match section{
                            0 => {
                                let sl :Vec<&str>= line.split("=").collect();
                                let num = sl[1].parse::<u32>().unwrap();
                                vars.insert(sl[0].to_string(), num);
                            },
                            1 => {
                                let right_str : &str = line.split("=").collect::<Vec<&str>>()[1];
                                constant_string = right_str.split(" ").map(|x| vars[x].to_string()).collect::<Vec<String>>().join(" ");
                            },
                            2 => {
                                let sl :Vec<&str> = line.split(":").collect();
                                let stage_num = sl[0].parse::<u32>().unwrap();
                                let num_tasks = match sl[1].parse::<u32>(){
                                    Ok(x) => {
                                        x
                                    },
                                    Err(_) => {
                                        vars[sl[1]]
                                    }
                                };
                                let deps = sl[2].split(" ").filter(|x| x.len()>0).map(|x| x.parse::<u32>().unwrap()).collect::<Vec<u32>>();
                                let deps_nd= sl[3].split(" ").filter(|x| x.len()>0).map(|x| x.parse::<u32>().unwrap()).collect::<Vec<u32>>();
                                assert!(dep_map.contains_key(&stage_num) == false);
                                dep_map.insert(stage_num, deps);
                                assert!(dep_nd_map.contains_key(&stage_num) == false);
                                dep_nd_map.insert(stage_num, deps_nd);
                                assert!(task_num_vec.len() as u32==stage_num);
                                task_num_vec.push(num_tasks);
                            },
                            _ => {
                                break;
                            }
                        }

                    }
                    assert!(query_data.contains_key(&internal_qnum) == false);
                    let drop_map = Self::generate_drop_map(&dep_map);
                    let mut num_partitions = HashMap::<u32, u32>::new();
                    for entry in dep_map.iter(){
                        for dep in entry.1.iter() {
                            num_partitions.entry(*dep).or_insert(*task_num_vec.get(*entry.0 as usize).unwrap());
                        }
                    }
                    num_partitions.entry(task_num_vec.len() as u32 -1).or_insert(1);
                    let last_stage = task_num_vec.len() as u32 -1;
                    dep_nd_map.get_mut(&last_stage).unwrap().push(last_stage);
                    query_name_map.insert(file_name.clone(), internal_qnum);
                    query_data.insert(internal_qnum, QueryData{
                        query_name : file_name,
                        constants : constant_string,
                        query_deps : dep_map,
                        query_nd_map : dep_nd_map,
                        query_drop_map : drop_map,
                        query_num_partitions: num_partitions,
                        task_nums : task_num_vec,
                    });
                    internal_qnum += 1;
                }
			}
		}
        (query_data, query_name_map)
    }

    fn parse_schedule(filename : &str) -> Vec<(u64, Vec<String>)> {
        let file = fs::File::open(filename).unwrap();
        let mut sched = Vec::new();
        let mut curr_time = 0;
        let mut curr_vec = Vec::new();
        for line in BufReader::new(file).lines(){
            let line = line.unwrap();
            let split_line = line.split(",").collect::<Vec<&str>>();
            let time = split_line[0].parse::<u64>().unwrap();
            let query = split_line[1].to_string();
            if time > curr_time {
                if curr_vec.len() > 0 {
                    sched.push((curr_time, curr_vec));
                    curr_vec = Vec::new();
                }
                curr_time = time;
            }
            curr_vec.push(query);
        }
        if curr_vec.len() > 0 {
            sched.push((curr_time, curr_vec));
        }
        sched
    }

    fn print_drop_schedule(&self, query : &str){
        let qdata = &self.query_data[&self.query_name_map[query]];
        println!("{:?}", qdata);
        let num_stages = qdata.query_deps.len() as u32;
        for stage_id in 0..num_stages {
            println!("{:?}: {:?}", stage_id, qdata.query_drop_map[&stage_id]);
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let sm = CackleCoordinator::new().await;
    let mut my_sched = Vec::<(u64, Vec<String>)>::new();
    //for x  in 0..3{
    for x  in 0..15{
        //let num_queries = (120-(120 as i64-x as i64).abs())/30 +1;
        let num_queries = 4;
        //let qvec= vec!["q8".to_string() ; num_queries as usize];
        let qvec= vec!["q8", "q2", "q3", "q9","q18", "q8", "q2", "q3", "q9","q18",].iter().map(|x| x.to_string()).collect::<Vec<String>>();
        //let qvec= vec!["q8",].iter().map(|x| x.to_string()).collect::<Vec<String>>();
        //let qvec= vec!["q8",].iter().map(|x| x.to_string()).collect::<Vec<String>>();
        //let qvec= vec!["q12",].iter().map(|x| x.to_string()).collect::<Vec<String>>();
        my_sched.push((x*60 as u64, qvec));
    }
    //let mut sched = CackleCoordinator::parse_schedule("workloads/3600,10,60,128,1,20,1200,0.3,1,102400.0,False,False.queries");
    //let mut sched = CackleCoordinator::parse_schedule("workloads/3600,10,1000,128,1,20,1200,0.3,1,102400.0,False,False.queries");
    //let mut sched = CackleCoordinator::parse_schedule("workloads/3600,10,1000,128,1,20,1200,1.0,1,102400.0,False,False.queries");
    let mut sched = CackleCoordinator::parse_schedule("test_workload.work");
    println!("{:?}", Local::now());
    println!("{:?}", sched);
    time::sleep(Duration::from_secs(180)).await;
    //time::sleep(Duration::from_secs(30)).await;
    sm.clone().clear_caches().await;

    /*
    let x = 21;
    let res = sm.clone().run_named_query(format!("q{}", x)).await;
    println!("{:?}", res);
    */
    /*
    time::sleep(Duration::from_secs(2)).await;
    for x in 1..23 {
        let res = sm.clone().run_named_query(format!("q{}", x)).await;
        println!("{:?}", res);
    }
    time::sleep(Duration::from_secs(6)).await;
    */
    let rets = sm.clone().run_schedule(sched).await;
    println!("-----------------------------------");
    for ret in rets {
        println!("{:?}",ret);
    }
    
    sm.clone().shutdown().await;
    sm.print_finished().await;

    println!("{:?}", Local::now());
    println!("Done!");
    Ok(())
}
