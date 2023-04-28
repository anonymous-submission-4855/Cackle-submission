#pragma once
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <iostream>
#include <iomanip>
#include <sstream>
#include <fstream>
#include <chrono>
#include <thread>
#include <unordered_set>


#include <aws/core/Aws.h>

#include <boost/asio/thread_pool.hpp>
#include <boost/asio/post.hpp>
#include <boost/bind/bind.hpp>

#include <S3Client.h>
#include <S3FileInputStream.h>
#include <MyCacheClient.h>

namespace starling{

class QueryExecutor{

  protected:
    static const int NUM_MUXS = 128;

    struct SinglePartitionData{
      int64_t numRecords;
      std::stringstream buf;
    };

    struct CompressedData{
      uint64_t size;
      std::unique_ptr<char[]> data;
    };

    std::mutex muxs[NUM_MUXS];
    std::unordered_set<int64_t> exists_set;
    std::string cache_servers;
    MyCacheClient cache;
    int cacheNodesToUse;
    long maxCacheToUse;
    int maxNumThreads;
    int currNumThreads;
    bool doubleWrite = true;

    std::string query_id;

    void writeToS3(S3Client &s3, const std::string& key, std::shared_ptr<std::stringstream> &sstream);

    void writeToS3(S3Client &s3, const std::string& key, const char * payload, int payloadSize);

    void writeToFile(const std::string& key, std::shared_ptr<std::stringstream> &sstream);

    std::unique_ptr<std::ifstream> readFromFile(const std::string& key);

    virtual void readFromS3(S3Client &s3, std::string key, int64_t start, int64_t end, char* buf);

    void setCacheServers(const char * server_list);

  public:

    virtual int handleRequest(int argc, char **argv) = 0;
};

} //namespace starling
