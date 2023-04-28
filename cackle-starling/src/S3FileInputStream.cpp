#include <S3Client.h>
#include <S3FileInputStream.h>

using namespace starling;
S3FileInputStream::S3FileInputStream(const std::string &bucket, const std::string &key, int concurrentThreads) : bucket(bucket), key(key), s3(bucket, "us-east-1", concurrentThreads){
  fileLength=s3.GetObjectLength(key);
  filename = "s3://" + bucket + "/" + key;
}

S3FileInputStream::~S3FileInputStream(){}

uint64_t S3FileInputStream::getLength() const{
  return fileLength;
}
uint64_t S3FileInputStream::getNaturalReadSize() const {
  return 4*1024*1024;
}

int S3FileInputStream::getNumReads() const{
  return s3.GetNumRequests();
}

void S3FileInputStream::read(void* buf, uint64_t length, uint64_t offset){
  mux.lock();
  auto lb = fetchedData.lower_bound(offset);

  if (lb != fetchedData.begin()){
    --lb;
    assert(lb->first < offset);
    //check if we overlap we already know that this data starts before us.
    if (lb->second.offset+lb->second.length >= length+offset){
      std::memcpy(buf, lb->second.data.get()+(offset-lb->second.offset),length);
      mux.unlock();
      return;
    }
  }
  mux.unlock();

  uint64_t fetch_length = length;
  if (fetch_length < min_read_size){
    fetch_length = std::min(min_read_size, fileLength-offset); 
  } 
  int retry_count = 0; 
  std::unique_ptr<char[]> read_data(new char[fetch_length]); 
  while (true){
    //TODO: retry on error
    int returnCode = s3.GetObjectWithRange(key, offset, fetch_length, read_data.get());
    if (returnCode < 200 || returnCode >=300){
      std::this_thread::sleep_for(std::chrono::milliseconds(200));
      if (++retry_count == 600){
        std::cerr << "Exiting because we're fucked" << std::endl;
        exit(1);
      }
      /*
      if(retry_count % 10 ==0){
        s3.ResetConnections();
      }
      */

    }else{
      break;
    }
      
  }

  if (length >= min_read_size){
    std::memcpy((char *) buf, read_data.get(), fetch_length);
  }else{
    std::memcpy((char *) buf, read_data.get(), length);
    mux.lock();
    fetchedData.insert(std::make_pair(offset, S3Extant(offset, fetch_length, std::move(read_data))));
    mux.unlock();
  }
}

const std::string& S3FileInputStream::getName() const{
  return filename; 
}

const uint64_t S3FileInputStream::min_read_size = 256*1024;
