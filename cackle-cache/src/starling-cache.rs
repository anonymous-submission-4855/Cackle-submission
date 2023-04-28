use tonic::{transport::Server, Request, Response, Status};

use cackle_cache::cache_server::{Cache, CacheServer};
use cackle_cache::{*};
use tokio::time::{interval, Instant, Duration};
use std::collections::{HashMap};
use std::env;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;
use tokio::sync::{Mutex, mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use parse_size::parse_size;
use prost::Message;

pub mod cackle_cache {
    tonic::include_proto!("cackle_cache");
}

#[derive(Debug)]
struct Reservation {
    reservation_size : u64,
    reservation_exp : Instant,
}

#[derive(Debug)]
pub struct MyCacheData {
    table : Vec<Mutex<HashMap::<String, mpsc::Sender<ListCommand>>>>,
    reservations : Vec<Mutex<HashMap::<u64, Reservation>>>,
    reservation_id : AtomicU64,
    current_size : AtomicU64,
    capacity : u64,
}

pub struct MyCache {
    state : Arc<MyCacheData>,
}

enum ListCommand {
    PushValue{
        val : Partition
    },
    GetList{
        resp : oneshot::Sender<Arc<Vec<Partition>>>,
    },
}

impl MyCache{
    fn new(capacity: u64) -> MyCache {
        let num_stripes = 64;
        let mut lock_stripes = Vec::with_capacity(num_stripes);
        let mut res_stripes = Vec::with_capacity(num_stripes);
        for _ in 0..num_stripes {
            lock_stripes.push(Mutex::new(HashMap::new()));
            res_stripes.push(Mutex::new(HashMap::new()));
        }
        let me = MyCache{
            state : Arc::new(MyCacheData {
                table : lock_stripes,
                reservations : res_stripes,
                reservation_id : AtomicU64::new(0),
                current_size : AtomicU64::new(0),
                capacity : capacity,
            }),
        };
        tokio::spawn(MyCache::cleanup_reservations(me.state.clone()));
        me
    }

    async fn cleanup_reservations(state: Arc<MyCacheData>) {
        let mut timer = interval(Duration::from_secs(1));
        timer.tick().await;
        loop {
            timer.tick().await;
            for res_map_lock in &state.reservations {
                let mut res_map = res_map_lock.lock().await;
                let mut remove_keys = Vec::new();
                for pair in res_map.iter_mut(){
                    if pair.1.reservation_exp < Instant::now() || pair.1.reservation_size == 0{
                        remove_keys.push(pair.0.clone());
                        state.current_size.fetch_sub(pair.1.reservation_size, Ordering::SeqCst);
                    }
                }
                for key in remove_keys {
                    res_map.remove(&key);
                    //println!("dropping reservation {}", key);
                }
            }
        }

    }

    fn get_hash(instr : &String) -> usize{
        let mut s = DefaultHasher::new();
        instr.hash(&mut s);
        s.finish() as usize
    }


    async fn list_manager_read(state: Arc<MyCacheData>, key :String, size: u64, read_vec: Arc<Vec<Partition>>, mut rx : mpsc::Receiver<ListCommand>){
        while let Some(cmd) = rx.recv().await {
            use ListCommand::*;
            match cmd {
                PushValue {..} => {
                    assert!(false);
                },
                GetList {resp} => {
                    resp.send(read_vec.clone());
                },
            }
        }
        //println!("dropping {}", key);
        state.current_size.fetch_sub(size, Ordering::SeqCst);
    }

    async fn list_manager_write(state: Arc<MyCacheData>, key :String, mut rx : mpsc::Receiver<ListCommand>){
        let mut write_vec = Vec::<Partition>::new();
        let read_vec :Arc<Vec<Partition>> ;
        let mut size :u64 = 0;
        while let Some(cmd) = rx.recv().await {
            use ListCommand::*;
            match cmd {
                PushValue {val} => {
                    size += val.encoded_len() as u64;
                    write_vec.push(val);
                },
                GetList {resp} => {
                    read_vec = Arc::new(write_vec);
                    resp.send(read_vec.clone());
                    tokio::spawn(Self::list_manager_read(state, key, size, read_vec, rx));
                    return;
                },
            }
        }
        //println!("dropping {} : should only be called if dropped without readling", key);
        state.current_size.fetch_sub(size, Ordering::SeqCst);
    }
}

#[tonic::async_trait]
impl Cache for MyCache {
    async fn push(
        &self,
        request: Request<PushRequest>,
    ) -> Result<Response<PushReply>, Status> {
        //println!("Got a request: {:?}", request);
        let inner_request = request.into_inner();
        let key = inner_request.key.unwrap();
        let value = inner_request.value.unwrap();
        let res_id = inner_request.reservation_id.unwrap();
        let size :u64 = value.encoded_len() as u64;
        let key_hash = MyCache::get_hash(&key)%self.state.table.len();
        let curr_size = self.state.current_size.load(Ordering::SeqCst);
        let part = value.in_partition.unwrap();
        
        let err_code;
        {
            let mut allow_push = false;
            let res_stripe = res_id as usize % self.state.reservations.len();
            {
                let mut res_map = self.state.reservations[res_stripe].lock().await;
                let res_opt = res_map.get_mut(&res_id);
                match res_opt {
                    Some(res) => {
                        //println!("res_size {} obj_size {}", res.reservation_size, size);
                        if res.reservation_size >= size &&
                        Instant::now() < res.reservation_exp  {
                            res.reservation_size -= size;
                            allow_push = true;
                        }
                    }, None=> { 
                        //println!("res_id {} not found", res_id);

                    }
                };
            }
            if allow_push{
                let tx;
                {
                    let ref mut table= self.state.table[key_hash].lock().await;
                    if !table.contains_key(&key){
                        let (tx, rx) = mpsc::channel::<ListCommand>(1);
                        tokio::spawn(MyCache::list_manager_write(self.state.clone(), key.clone(), rx));
                        table.insert(key.clone(), tx);
                    }
                    tx = table.get(&key).unwrap();
                    tx.send(ListCommand::PushValue{val :value}).await;
                }
                //println!("pushing key: {:?} with size {:?} in_op {:?} from {:?} to {:?} bytes", key, size, part, curr_size, curr_size+size);
                err_code = CacheErrorCode::Ok;
            } else {
                err_code = CacheErrorCode::CouldNotWrite;
                //println!("rejecting push: {:?} with size {:?} curr_size {:?} capacity {:?}" , key, size, curr_size, self.state.capacity);
            }
        }

        let reply = PushReply {
            err: Some(err_code as i32),
            error: None,
        };

        Ok(Response::new(reply))
    }

    type GetStream = ReceiverStream<Result<GetReply, Status>>;

    async fn get(
        &self,
        request: Request<GetRequest>,
    ) -> Result<Response<Self::GetStream>, Status> {
        //println!("Got a request: {:?}", request);
        let inner_request = request.into_inner();
        let key = inner_request.key.unwrap();
        let should_drop = match inner_request.should_drop{Some(x) => x , None => false};
        let list_tx;
        let key_hash = MyCache::get_hash(&key)%self.state.table.len();
        {
            let ref mut table = self.state.table[key_hash].lock().await;
            if should_drop {
                list_tx = table.remove(&key);
                //println!("dropping key {}", key);
            }else{
                list_tx = match table.get(&key) {
                    Some(x) => Some(x.clone()),
                    None => None,
                }
            }
        }
        
        let (resp_tx, resp_rx) = mpsc::channel(1);
        match list_tx {
            Some(list_tx) => {
                tokio::spawn(async move {
                        let (ch_tx, ch_rx) = oneshot::channel::<Arc<Vec<Partition>>>();
                        list_tx.send(ListCommand::GetList{resp:ch_tx}).await;
                        let vec :Arc<Vec<Partition>> = ch_rx.await.unwrap();
                        for result in (*vec).iter() {
                            let ret = resp_tx.send(Ok(GetReply {
                                err : Some(CacheErrorCode::Ok as i32),
                                value : Some(result.clone()),
                                error : None,
                            })).await;
                            match ret{
                                Ok(_) => {},
                                Err(x) => {
                                    println!("Channel send Err {:?}", x);
                                }
                            }
                        }
                    });
            },
            None => {
                let ret = resp_tx.send(Ok(GetReply {
                    err : Some(CacheErrorCode::MissingKey as i32),
                    value : None,
                    error : None,
                })).await;
                match ret{
                    Ok(_) => {},
                    Err(x) => {
                        println!("Channel send Err {:?}", x);
                    }
                };
            }
        }
        Ok(Response::new(ReceiverStream::new(resp_rx)))
    }


    
    async fn drop(
        &self,
        request: Request<DropRequest>,
    ) -> Result<Response<DropReply>, Status> {
        //println!("Got a request: {:?}", request);
        //TODO: need to write to S3 if requested
        let inner = request.into_inner();
        let key = inner.key.unwrap();
        let key_hash = MyCache::get_hash(&key)%self.state.table.len();

        let reply = DropReply {
            err: {
                let ref mut table = self.state.table[key_hash].lock().await;
                match table.remove(&key) {
                    Some(_x) => {
                        //println!("dropping key: {}", key);
                        Some(CacheErrorCode::Ok as i32)
                    },
                    None => Some(CacheErrorCode::MissingKey as i32)
                }
            },
            error : None,
        };

        Ok(Response::new(reply))
    }

    async fn drop_reservation(
        &self,
        request: Request<DropReservationRequest>,
    ) -> Result<Response<DropReservationReply>, Status> {
        //println!("Got a request: {:?}", request);
        //TODO: need to write to S3 if requested
        let inner = request.into_inner();
        let res_id = inner.reservation_id.unwrap();
        let key_hash = res_id as usize % self.state.reservations.len();
        {
            let ref mut table = self.state.reservations[key_hash].lock().await;
            match table.remove(&res_id) {
                Some(x) => {
                    self.state.current_size.fetch_sub(x.reservation_size, Ordering::SeqCst);
                    //println!("dropping key: {:?} with size {:?} from {:?} to {:?} bytes", key, size, curr_size, curr_size-size);
                },
                None => {}
            }
        }
        let reply = DropReservationReply{};

        Ok(Response::new(reply))
    }

    async fn clear(
        &self,
        _request: Request<ClearRequest>,
    ) -> Result<Response<ClearReply>, Status> {
        //println!("Got a request: {:?}", request);
        //TODO: need to write to S3 if requested
        for x in 0..self.state.table.len(){
            let mut table = self.state.table[x].lock().await;
            table.clear();
        }


        let reply = ClearReply {
            err: Some(CacheErrorCode::Ok as i32),
            error : None,
        };

        Ok(Response::new(reply))
    }

    async fn reserve(
        &self,
        request: Request<ReservationRequest>,
    ) -> Result<Response<ReservationReply>, Status> {
        //println!("Got a request: {:?}", request);
        let ir = request.into_inner();
        let request_size = ir.size.unwrap();
        let current_size = self.state.current_size.load(Ordering::SeqCst);
        let reply;
        if current_size + request_size > self.state.capacity {
            reply = ReservationReply{
                err : Some(CacheErrorCode::CouldNotWrite as i32),
                reservation_id : None,
                reserved_size : None,
                reservation_seconds : None,
            };
        } else {
            if let Ok(_) = self.state.current_size.compare_exchange(current_size, current_size+request_size, Ordering::SeqCst, Ordering::SeqCst) {
                let res_id = self.state.reservation_id.fetch_add(1, Ordering::SeqCst);
                let reservation = Reservation{
                    reservation_size : request_size,
                    reservation_exp : Instant::now()+Duration::new(12, 0),
                };

                {
                    let mut res = self.state.reservations[res_id as usize %self.state.reservations.len()].lock().await;
                    res.insert(res_id, reservation);
                }
                //println!("set reservation {} with size {} in stripe {}", res_id, request_size, res_id as usize % self.state.reservations.len());
                reply = ReservationReply{
                    err : Some(CacheErrorCode::Ok as i32),
                    reservation_id : Some(res_id),
                    reserved_size: Some(request_size),
                    reservation_seconds: Some(10),
                }

            } else {
                reply = ReservationReply{
                    err : Some(CacheErrorCode::CouldNotWrite as i32),
                    reservation_id : None,
                    reserved_size : None,
                    reservation_seconds : None,
                };
            }
        }

        Ok(Response::new(reply))
    }

    async fn get_size(
        &self,
        _request: Request<SizeRequest>,
    ) -> Result<Response<SizeReply>, Status> {
        //println!("Got a request: {:?}", request);
        //TODO: need to write to S3 if requested

        let reply = SizeReply {
            capacity: Some(self.state.capacity),
            current_size : Some(self.state.current_size.load(Ordering::SeqCst)),
        };

        Ok(Response::new(reply))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    let addr = args[1].parse()?;
    let cache_size = parse_size(args[2].clone()).unwrap()*5/10;
    println!("{:?} {:?} ", addr, cache_size);
    
    let cache = MyCache::new(cache_size);

    Server::builder()
        .max_concurrent_streams(64)
        .add_service(CacheServer::new(cache))
        .serve(addr)
        .await?;

    Ok(())
}
