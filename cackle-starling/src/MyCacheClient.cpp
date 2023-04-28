#include <iostream>
#include <random>
#include <sstream>
#include <string>
#include <MyCacheClient.h>

using namespace starling;
using grpc::Channel;
using grpc::ChannelArguments;
using grpc::ClientContext;
using grpc::ClientReader;
using grpc::ClientReaderWriter;
using grpc::ClientWriter;
using grpc::CompletionQueue;
using grpc::ResourceQuota;
using grpc::Status;

MyCacheClient::MyCacheClient(){
  push_request_id = 0;
}


void MyCacheClient::set_cache_servers(char * cache_servers){
  std::stringstream stream(cache_servers);
  std::string token;
  while(std::getline(stream, token, ';')){
    cache_nodes.push_back(token);
  }
}

void MyCacheClient::set_cache_inputs(char * cache_inputs){
  std::stringstream stream(cache_inputs);
  std::string token;
  while(std::getline(stream, token, '#')){
    std::stringstream dep_stream(token);
    std::string dep_token;
    int dep;
    int dep_count=0;
    std::unordered_map<int, std::unordered_set<int>> cache_part_map;
    while(std::getline(dep_stream, dep_token, '%')){
      if (dep_count == 0) {
        dep = stoi(dep_token);
        std::cout << "Got dep: " << dep << std::endl;
      } else if (dep_count == 1) {
        std::stringstream cache_parts_stream(dep_token);
        std::string cps_token;
        while(std::getline(cache_parts_stream, cps_token, ';')){
          std::stringstream cache_part_stream(cps_token);
          std::string cp_token;
          int cp_count = 0;
          int cache_id;
          std::unordered_set<int> out_partitions;
          while(std::getline(cache_part_stream, cp_token, ':')){
            if (cp_count == 0) {
              cache_id = stoi(cp_token);
            } else if (cp_count == 1) {
              std::stringstream part_stream(cp_token);
              std::string part_token;
              while(std::getline(part_stream, part_token, ',')){
                out_partitions.insert(stoi(part_token));
                std::cout << "got cache: " << cache_id << " partition " << part_token << std::endl;
              }
            }
            cp_count++;
          }
          cache_part_map[cache_id]=  std::move(out_partitions);
        }
      }
      dep_count++;
    }
    cache_input_map[dep] = std::move(cache_part_map);
  }
}

int MyCacheClient::get_stub_idx(const std::string &key, int ordinal){
  thread_local static std::default_random_engine random_engine;
  auto seed = std::hash<std::string>{}(key);
  auto size = cache_nodes.size();
  auto vec = std::vector<int>(size, 0);
  for (auto x = 0; x < size; ++x) {
    vec[x] = x;
  }
  random_engine.seed(seed);
  std::shuffle(vec.begin(), vec.end(), random_engine);
  if (ordinal >= size){
    return -1;
  }
  int stub_idx = vec[ordinal];

  if (stubs.find(stub_idx) == stubs.end()){
     auto channelArgs =ChannelArguments();
     ResourceQuota rq;
     rq.SetMaxThreads(1);
     channelArgs.SetResourceQuota(rq);
     channelArgs.SetMaxReceiveMessageSize(100*1024*1024);
     channelArgs.SetMaxSendMessageSize(100*1024*1024);
     stubs[stub_idx] = Cache::NewStub(grpc::CreateCustomChannel(cache_nodes[stub_idx], grpc::InsecureChannelCredentials(), channelArgs));
     processed_until[stub_idx] = -1;
     last_submitted[stub_idx] = -1;
  }
  return stub_idx;
}

bool MyCacheClient::can_push_to_cache(int stub_idx, uint64_t size){
  if (size > 100*1024*1024) {
    return false;
  }
  auto res = reservations.find(stub_idx);
  bool need_new_res = false;
  if (res == reservations.end()){
    need_new_res = true;
  } else if (res->second.perm_fail){
    std::cout << "perm fail set " << stub_idx << " " << size << " " << std::endl;
    return false;
  }else if (res->second.res_exp < std::chrono::steady_clock::now() || res->second.res_size < size ) {
    //wait for previous to finish
    if (process_until(stub_idx, last_submitted[stub_idx]) && push_requests.find(last_submitted[stub_idx]) != push_requests.end()) {
      push_requests[last_submitted[stub_idx]]->completed = true;
    }
    ClientContext ctx;
    cackle_cache::DropReservationRequest drop_res_req;
    cackle_cache::DropReservationReply drop_res_reply;
    drop_res_req.set_reservation_id(res->second.res_id);

    Status status;
    auto rpc = stubs[stub_idx]->AsyncDropReservation(&ctx, drop_res_req, &cq);
    long req_id = push_request_id++;
    rpc->Finish(&drop_res_reply, &status, (void*)req_id);
    if (!process_until(stub_idx, req_id) || !status.ok()) {
      res->second.perm_fail = true;
      std::cout << "setting perm failure " << stub_idx << " " << size << std::endl;
      return false;
    }
    reservations.erase(stub_idx);
    need_new_res = true;
  }
  if (need_new_res) {
    ClientContext ctx;
    cackle_cache::ReservationRequest res_req;
    cackle_cache::ReservationReply res_reply;
    auto requested_reservation_size = std::max(256L*1024, (long)size);
    res_req.set_size(requested_reservation_size);
    auto rpc = stubs[stub_idx]->AsyncReserve(&ctx, res_req, &cq);
    long req_id = push_request_id++;
    Status status;
    rpc->Finish(&res_reply, &status, (void*)req_id);
    if (!process_until(stub_idx, req_id) || !status.ok()) {
      std::cout << "new reservation failed " << stub_idx << " " << size <<  std::endl;
      Reservation new_res{};
      new_res.perm_fail = true;
      reservations[stub_idx] = std::move(new_res);
      return false;
    }
    if (res_reply.err() == cackle_cache::CacheErrorCode::OK) {
      Reservation new_res{};
      new_res.res_id = res_reply.reservation_id();
      new_res.res_size = res_reply.reserved_size();
      new_res.res_exp = std::chrono::steady_clock::now()+std::chrono::seconds(res_reply.reservation_seconds());
      new_res.perm_fail = false;
      std::cout << "got new reservation " << new_res.res_id << " with size " << new_res.res_size << " " <<stub_idx << " " <<  size << std::endl;

      reservations[stub_idx] = std::move(new_res);
      res = reservations.find(stub_idx);
    } else{
      Reservation new_res{};
      new_res.perm_fail = true;
      std::cout << "new reservation rejected " << stub_idx << " " << size << std::endl;
      reservations[stub_idx] = std::move(new_res);
      return false;
    }
  }
  std::cout << "reducing size of reservation " << res->second.res_id << " to " << res->second.res_size - size << " " << stub_idx << " " << size <<  std::endl;
  res->second.res_size -= size;
  return true;
}

bool MyCacheClient::push(std::string query, int in_partition, int out_partition, int ordinal, const char * partition_data, int partition_data_len,  const char * data, int data_len){

 ClientContext ctx;
 cackle_cache::PushRequest pushreq;
 cackle_cache::PushReply pushreply;
 std::string key = query+std::to_string(out_partition);
 auto stub_idx = get_stub_idx(key, ordinal);

 pushreq.set_key(key);
 if (partition_data_len > 0){
   pushreq.mutable_value()->set_header(std::string(partition_data, partition_data_len));
 }
 pushreq.mutable_value()->set_data(std::string(data, data_len));
 pushreq.mutable_value()->set_in_partition(in_partition);
 if (stub_idx < 0) {
   return false;
 }
 auto push_size = pushreq.mutable_value()->ByteSizeLong();
 if (!can_push_to_cache(stub_idx, push_size)){
   return false;
 }
 pushreq.set_reservation_id(reservations[stub_idx].res_id);
 auto status = stubs[stub_idx]->Push(&ctx, pushreq, &pushreply);
 bool result = status.ok() && pushreply.err() == cackle_cache::CacheErrorCode::OK;
 if (result) {
    if (node_partition_map.find(stub_idx) == node_partition_map.end()){
      node_partition_map[stub_idx] = std::vector<int>();
    }
   node_partition_map[stub_idx].push_back(out_partition);
 }
 std::cout << "push sync " << key << " " << result << std::endl;
 return result;
}

long MyCacheClient::async_push(std::string query, int in_partition, int out_partition, int ordinal, const char * partition_data, int partition_data_len,  const char * data, int data_len){

 cackle_cache::PushRequest pushreq;
 auto context = std::unique_ptr<PushRequestContext>(new PushRequestContext());
 std::string key = query+std::to_string(out_partition);
 pushreq.set_key(key);
 if (partition_data_len > 0){
   pushreq.mutable_value()->set_header(std::string(partition_data, partition_data_len));
 }
 pushreq.mutable_value()->set_data(std::string(data, data_len));
 pushreq.mutable_value()->set_in_partition(in_partition);
 auto stub_idx = get_stub_idx(key, ordinal);
 if (stub_idx < 0) {
    //std::cout << "stub rejected" << std::endl;
   return -1;
 }
 std::cout << "async_push of key " << key << " to stub_idx " << stub_idx << " ordinal " << ordinal << std::endl;
 auto push_size = pushreq.mutable_value()->ByteSizeLong();
 if (!can_push_to_cache(stub_idx, push_size)){
    //std::cout << "push to cache rejcted" << std::endl;
   return -1;
 }
 pushreq.set_reservation_id(reservations[stub_idx].res_id);
 auto rpc = stubs[stub_idx]->AsyncPush(&context->ctx, pushreq, &cq);
 long req_id = push_request_id++;
 std::cout << "async push req_id " <<req_id<<  " stub " << stub_idx << std::endl;
 rpc->Finish(&context->rep, &context->status, (void*)req_id);
 last_submitted[stub_idx] = req_id;
 context->node = stub_idx;
 context->partition = out_partition;
 push_requests.insert(std::make_pair(req_id, std::move(context)));
 return req_id;
}

bool MyCacheClient::process_until(int stub_idx, long req_id) {
  if (req_id <= processed_until[stub_idx]){
    return true;
  }
  std::cout << "process_until stub_idx " << stub_idx << " " << req_id << std::endl;
  bool ok = false;
  void * got_tag;
  while(true) {
    cq.Next(&got_tag, &ok);
    if (!ok) {
      return false;
    } else {
      if (got_tag == (void*) req_id){
        processed_until[stub_idx] = req_id;
        return true;
      } else {
        std::cout << "got_tag " << (long) got_tag << std::endl;
        auto req_stub_idx = push_requests[(long) got_tag]->node;
        push_requests[(long) got_tag]->completed = true;
        processed_until[req_stub_idx] = std::max(processed_until[req_stub_idx], (long) got_tag);
      }
    }
  }
}


bool MyCacheClient::async_push_result(long req_id){
  std::cout << "getting result " << req_id << std::endl;
  auto found = push_requests.find(req_id);
  int stub_idx = found->second->node;
  bool result;
  if (found->second->completed){
    result = found->second->status.ok() && found->second->rep.err() == cackle_cache::CacheErrorCode::OK;
  }else{
    auto success = process_until(stub_idx, req_id);
    found->second->completed = true;
    result = found->second->status.ok() && found->second->rep.err() == cackle_cache::CacheErrorCode::OK;
  }
  if (result) {
    if (node_partition_map.find(found->second->node) == node_partition_map.end()){
      node_partition_map[found->second->node] = std::vector<int>();
    }
    node_partition_map[found->second->node].push_back(found->second->partition);
  }
  //push_requests.erase(found);
  std::cout << "erasing result " << req_id << std::endl;
  processed_until[stub_idx] = std::max(processed_until[stub_idx], req_id);
 std::cout << "push async " << req_id << " " << result << std::endl;
  return result;
}

std::string MyCacheClient::GetWrittenPartitionsStr(){
  std::stringstream ss;
  int part_map_count = node_partition_map.size();
  for (auto node : node_partition_map){
    int node_id = node.first;
    int part_count = node.second.size();
    if (part_count > 0) {
      ss << node_id << ":";
      for (auto partition : node.second) {
        ss << partition;
        part_count--;
        if (part_count > 0){
          ss<< ",";
        }
      }
    }
    part_map_count--;
    if (part_map_count > 0){
      ss<< ";";
    }
  }
  return ss.str();
}

//Get a list of results and then clear it out.
std::unique_ptr<ClientReader<cackle_cache::GetReply>> MyCacheClient::get(ClientContext &ctx, const std::string &query, int in_operator_idx, int out_partition, int ordinal, bool should_drop){
  auto key = query+"_pt"+std::to_string(in_operator_idx)+"_"+std::to_string(out_partition);
  cackle_cache::GetRequest getreq;
  getreq.set_key(key);
  getreq.set_should_drop(should_drop);
  auto stub_idx = get_stub_idx(key, ordinal);
  if (stub_idx < 0) {
    return nullptr;
  }
  if (cache_input_map.find(in_operator_idx) == cache_input_map.end() || 
      cache_input_map[in_operator_idx].find(stub_idx) == cache_input_map[in_operator_idx].end() ||
      cache_input_map[in_operator_idx][stub_idx].count(out_partition) == 0) {
    return nullptr;
  }
  std::chrono::system_clock::time_point deadline =
        std::chrono::system_clock::now() + std::chrono::seconds(20);
  //Should be good here
  ctx.set_wait_for_ready(true);
  ctx.set_deadline(deadline);
  return stubs[stub_idx]->Get(&ctx, getreq);
}
