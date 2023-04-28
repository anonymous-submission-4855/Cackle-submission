#include <QueryExecutor.h>

using namespace starling;

//TODO super class executor that does this for us
void QueryExecutor::writeToS3(S3Client &s3, const std::string& key, std::shared_ptr<std::stringstream> &sstream){
  auto outstr = sstream->str();
  writeToS3(s3, key, outstr.data(), outstr.size());
}
void QueryExecutor::writeToS3(S3Client &s3, const std::string& key, const char * payload, int payloadSize){
  auto start = std::chrono::steady_clock::now();
  if (doubleWrite){
    //if (payloadSize < 500*1024*1024){
    if (true){
      s3.PutObject(key+"_0", payload, payloadSize);
      s3.ResetConnections();
      s3.PutObject(key+"_1", payload, payloadSize);
    }else{
      s3.PutObject(key+"_0", payload, payloadSize);
    }
  }else{

    s3.PutObject(key, payload, payloadSize);
  }

  auto end = std::chrono::steady_clock::now();
  std::cout << key << ": write took " << std::chrono::duration_cast<std::chrono::milliseconds>(end-start).count() << std::endl;
}

void QueryExecutor::writeToFile(const std::string& key, std::shared_ptr<std::stringstream> &sstream){
  std::ofstream s;
  s.open("/home/ec2-user/temp_data/"+key);
  s << sstream->rdbuf();
  s.close();
}

std::unique_ptr<std::ifstream> QueryExecutor::readFromFile(const std::string& key){
  std::unique_ptr<std::ifstream> s(new std::ifstream());
  s->open("/home/ec2-user/temp_data/"+key);
  return std::move(s);
}

void QueryExecutor::readFromS3(S3Client &s3, std::string key, int64_t start, int64_t end, char* buf){
  if (end == 0){
    end = LLONG_MAX;
  }
  std::string bucket = "INTERMEDIATE_BUCKET_NAME";
  //TODO: fix this
  int retry_count = 0; 
  if (doubleWrite){
    bool done = false;
    std::string keys[2] = {key+"_0", key+"_1"};
    while (!done){
      for (int i = 0; i < 2; i++){
        int returnCode = s3.GetObjectWithRange(keys[i], start, end-start, buf);
        if (returnCode < 200 || returnCode >=300){
          continue;
        }else{
          done = true;
          break;
        }
      }
      if (!done){
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        if (++retry_count == 200){
          std::cerr << "Exiting because we're fucked" << std::endl;
          exit(1);
        }
        s3.ResetConnections();
      }
    }
  }else{
    while (true){
      int returnCode = s3.GetObjectWithRange(key, start, end-start, buf);
      if (returnCode < 200 || returnCode >=300){
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        if (++retry_count == 200){
          std::cerr << "Exiting because we're fucked" << std::endl;
          exit(1);
        }
      }else{
        break;
      }
    }
  }
  return;
}

