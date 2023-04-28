#pragma once

#include <grpc/grpc.h>
#include <grpcpp/channel.h>
#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>
#include <proto/cackle_cache.grpc.pb.h>

namespace starling{
using grpc::Channel;
using grpc::ClientContext;
using grpc::ClientReader;
using grpc::ClientReaderWriter;
using grpc::ClientWriter;
using grpc::CompletionQueue;
using grpc::Status;
using cackle_cache::PushReply;
using cackle_cache::GetReply;
using cackle_cache::Cache;


class MyCacheClient{
  private:
    class PushRequestContext{
      public:
        ClientContext ctx;
        PushReply rep;
        Status status;
        int node;
        int partition;
        bool completed = false;
    };

    class Reservation{
      public:
      uint64_t res_id;
      uint64_t res_size;
      std::chrono::steady_clock::time_point res_exp;
      bool perm_fail;
    };

    std::vector<std::string> cache_nodes{};
    std::unordered_map<int, std::unique_ptr<Cache::Stub>> stubs{};
    std::unordered_map<int, Reservation> reservations{};
    std::unordered_map<int, long> processed_until{};
    std::unordered_map<int, long> last_submitted{};

    std::unordered_map<int,std::vector<int>> node_partition_map;

    std::unordered_map<int, std::unordered_map<int, std::unordered_set<int>>> cache_input_map;

    CompletionQueue cq;
    std::unordered_map<ulong, std::unique_ptr<PushRequestContext>> push_requests{};
    std::atomic_long push_request_id;

    bool process_until(int stub_idx, long req_id);

  public:
   MyCacheClient();

   void set_cache_servers(char * cache_servers);

   void set_cache_inputs(char * cache_inputs);

   int get_stub_idx(const std::string &query, int ordinal);

   bool can_push_to_cache(int stub_idx, uint64_t size);

   bool push(const std::string query, int in_partition, int out_partition, int ordinal, const char * partition_data, int partition_data_len,  const char * data, int data_len);

   long async_push(const std::string query, int in_partition, int out_partition, int ordinal, const char * partition_data, int partition_data_len,  const char * data, int data_len);

   bool async_push_result(long req_id);

   std::string GetWrittenPartitionsStr();

   //Get a list of results and then clear it out.
   std::unique_ptr<ClientReader<cackle_cache::GetReply>> get(ClientContext &ctx, const std::string &query, int in_operator_idx, int out_partition, int ordinal, bool should_drop);

};
}
