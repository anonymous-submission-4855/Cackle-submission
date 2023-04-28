#include <iostream>
#include <string>
#include <Query1Executor.h>
#include <Query2Executor.h>
#include <Query3Executor.h>
#include <Query4Executor.h>
#include <Query5Executor.h>
#include <Query6Executor.h>
#include <Query7Executor.h>
#include <Query8Executor.h>
#include <Query9Executor.h>
#include <Query10Executor.h>
#include <Query11Executor.h>
#include <Query12Executor.h>
#include <Query13Executor.h>
#include <Query14Executor.h>
#include <Query15Executor.h>
#include <Query16Executor.h>
#include <Query17Executor.h>
#include <Query18Executor.h>
#include <Query19Executor.h>
#include <Query20Executor.h>
#include <Query21Executor.h>
#include <Query22Executor.h>

using namespace starling;
// Map to associate the strings with the enum values

enum Query {
    Query1,
    Query2,
    Query3,
    Query4,
    Query5,
    Query6,
    Query7,
    Query8,
    Query9,
    Query10,
    Query11,
    Query12,
    Query13,
    Query14,
    Query15,
    Query16,
    Query17,
    Query18,
    Query19,
    Query20,
    Query21,
    Query22,
};

static std::map<std::string, Query> q_str_vals;

void InitializeMap() {
    q_str_vals["q1"]=Query1;
    q_str_vals["q2"]=Query2;
    q_str_vals["q3"]=Query3;
    q_str_vals["q4"]=Query4;
    q_str_vals["q5"]=Query5;
    q_str_vals["q6"]=Query6;
    q_str_vals["q7"]=Query7;
    q_str_vals["q8"]=Query8;
    q_str_vals["q9"]=Query9;
    q_str_vals["q10"]=Query10;
    q_str_vals["q11"]=Query11;
    q_str_vals["q12"]=Query12;
    q_str_vals["q13"]=Query13;
    q_str_vals["q14"]=Query14;
    q_str_vals["q15"]=Query15;
    q_str_vals["q16"]=Query16;
    q_str_vals["q17"]=Query17;
    q_str_vals["q18"]=Query18;
    q_str_vals["q19"]=Query19;
    q_str_vals["q20"]=Query20;
    q_str_vals["q21"]=Query21;
    q_str_vals["q22"]=Query22;
}

int main(int argc, char**argv){
  InitializeMap();
  Aws::SDKOptions options;
  Aws::InitAPI(options);

  std::unique_ptr<QueryExecutor> query;
  switch(q_str_vals[std::string(argv[1])]){
    case Query1:
      query = std::unique_ptr<Query1Executor>(new Query1Executor);
      break;
    case Query2:
      query = std::unique_ptr<Query2Executor>(new Query2Executor);
      break;
    case Query3:
      query = std::unique_ptr<Query3Executor>(new Query3Executor);
      break;
    case Query4:
      query = std::unique_ptr<Query4Executor>(new Query4Executor);
      break;
    case Query5:
      query = std::unique_ptr<Query5Executor>(new Query5Executor);
      break;
    case Query6:
      query = std::unique_ptr<Query6Executor>(new Query6Executor);
      break;
    case Query7:
      query = std::unique_ptr<Query7Executor>(new Query7Executor);
      break;
    case Query8:
      query = std::unique_ptr<Query8Executor>(new Query8Executor);
      break;
    case Query9:
      query = std::unique_ptr<Query9Executor>(new Query9Executor);
      break;
    case Query10:
      query = std::unique_ptr<Query10Executor>(new Query10Executor);
      break;
    case Query11:
      query = std::unique_ptr<Query11Executor>(new Query11Executor);
      break;
    case Query12:
      query = std::unique_ptr<Query12Executor>(new Query12Executor);
      break;
    case Query13:
      query = std::unique_ptr<Query13Executor>(new Query13Executor);
      break;
    case Query14:
      query = std::unique_ptr<Query14Executor>(new Query14Executor);
      break;
    case Query15:
      query = std::unique_ptr<Query15Executor>(new Query15Executor);
      break;
    case Query16:
      query = std::unique_ptr<Query16Executor>(new Query16Executor);
      break;
    case Query17:
      query = std::unique_ptr<Query17Executor>(new Query17Executor);
      break;
    case Query18:
      query = std::unique_ptr<Query18Executor>(new Query18Executor);
      break;
    case Query19:
      query = std::unique_ptr<Query19Executor>(new Query19Executor);
      break;
    case Query20:
      query = std::unique_ptr<Query20Executor>(new Query20Executor);
      break;
    case Query21:
      query = std::unique_ptr<Query21Executor>(new Query21Executor);
      break;
    case Query22:
      query = std::unique_ptr<Query22Executor>(new Query22Executor);
      break;
    default:
      std::cout << "Query not found: " <<argv[1] << std::endl;
      exit(1);
  }


  auto res = query->handleRequest(argc-1, argv+1);

  Aws::ShutdownAPI(options);
  return res;
}

