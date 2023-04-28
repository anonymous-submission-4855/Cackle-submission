/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "orc/orc-config.hh"
#include "orc/ColumnPrinter.hh"
#include "orc/Exceptions.hh"
#include "orc/OrcFile.hh"


#include <memory>
#include <string>
#include <iostream>
#include <string>

#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/HeadObjectRequest.h>

struct S3Extant{
  uint64_t offset;
  uint64_t length;
  std::unique_ptr<char> data;

  S3Extant(){}
  S3Extant(uint64_t offset, uint64_t length, std::unique_ptr<char>&& data) :offset(offset), length(length), data(std::move(data)){}

  
  S3Extant(S3Extant && other): offset(other.offset), length(other.length), data(std::move(other.data)){
  }
};

class S3FileInputStream : public orc::InputStream{
  private:
    Aws::String bucket;
    Aws::String key;
    std::string filename;
    uint64_t fileLength;
    Aws::S3::S3Client s3;
    static const uint64_t min_read_size;
    std::map<uint64_t, S3Extant> fetchedData;
  public:
    S3FileInputStream(Aws::String bucket, Aws::String key) : bucket(bucket), key(key){
      Aws::S3::Model::HeadObjectRequest ho_req;
      ho_req.SetBucket(bucket);
      ho_req.SetKey(key);
      auto ho_res = s3.HeadObject(ho_req);
      if (!ho_res.IsSuccess()){
        std::cerr << ho_res.GetError().GetExceptionName() << ": " 
          << ho_res.GetError().GetMessage() << std::endl;
        exit(1);
      }
      auto ho_rep = ho_res.GetResult(); 
      fileLength=ho_rep.GetContentLength();
      filename = "s3://" + bucket + "/" + key;
    }
    ~S3FileInputStream(){}

    uint64_t getLength() const override {
      return fileLength;
    }
    uint64_t getNaturalReadSize() const override{
      return 4*1024*1024;
    }

    void read(void* buf, uint64_t length, uint64_t offset) override{
      auto lb = fetchedData.lower_bound(offset);

      if (lb != fetchedData.begin()){
        --lb;
        assert(lb->first < offset);
        //check if we overlap we already know that this data starts before us.
        if (lb->second.offset+lb->second.length >= length+offset){
          std::cerr << "fetching from cache from idx " << offset << " to " << offset+length << std::endl;
          std::memcpy(buf, lb->second.data.get()+(offset-lb->second.offset),length);
          return;
        }
      }



      uint64_t fetch_length = length;
      if (fetch_length < min_read_size){
        fetch_length = std::min(min_read_size, fileLength-offset); 
      }
        
      std::cerr << "reading s3 file from idx " << offset << " for " << fetch_length << " bytes" << std::endl;
      Aws::S3::Model::GetObjectRequest go_req;
      go_req.SetBucket(bucket);
      go_req.SetKey(key);
      go_req.SetRange("bytes="+std::to_string(offset)+"-"+std::to_string(offset+fetch_length));
      auto go_res = s3.GetObject(go_req);
      if (!go_res.IsSuccess()){
        std::cerr << go_res.GetError().GetExceptionName() << ": " 
          << go_res.GetError().GetMessage() << std::endl;
        exit(1);
      }
      if (length >= min_read_size){
        go_res.GetResult().GetBody().read(reinterpret_cast<char *> (buf), fetch_length);
      }
      else{
        std::unique_ptr<char> read_data(new char[fetch_length]); 
        go_res.GetResult().GetBody().read(read_data.get(), fetch_length);
        std::memcpy(buf, read_data.get(), length);
        fetchedData.insert(std::make_pair(offset, S3Extant(offset, fetch_length, std::move(read_data))));
        std::cerr << "inserted " << offset << " to " <<offset+fetch_length << std::endl;
      }
    }

    const std::string& getName() const override{
      return filename; 
    }


};
const uint64_t S3FileInputStream::min_read_size = 1*1024*1024;



void printContents(const char* filename, const orc::RowReaderOptions& rowReaderOpts) {
  orc::ReaderOptions readerOpts;
  std::unique_ptr<orc::Reader> reader;
  std::unique_ptr<orc::RowReader> rowReader;
  reader = orc::createReader(std::unique_ptr<orc::InputStream>(new S3FileInputStream("mit-lambda-networking", "data/"+std::string(filename))), readerOpts);
  rowReader = reader->createRowReader(rowReaderOpts);

  std::unique_ptr<orc::ColumnVectorBatch> batch = rowReader->createRowBatch(1000);
  std::string line;
  std::unique_ptr<orc::ColumnPrinter> printer =
    createColumnPrinter(line, &rowReader->getSelectedType());

  while (rowReader->next(*batch)) {
    printer->reset(*batch);
    for(unsigned long i=0; i < batch->numElements; ++i) {
      line.clear();
      printer->printRow(i);
      line += "\n";
      const char* str = line.c_str();
      fwrite(str, 1, strlen(str), stdout);
    }
  }
}

int main(int argc, char* argv[]) {
  if (argc < 2) {
    std::cout << "Usage: orc-contents <filename> [--columns=1,2,...]\n"
              << "Print contents of <filename>.\n"
              << "If columns are specified, only these top-level (logical) columns are printed.\n" ;
    return 1;
  }
  try {
    Aws::SDKOptions options;
    Aws::InitAPI(options);
    const std::string COLUMNS_PREFIX = "--columns=";
    std::list<uint64_t> cols;
    char* filename = ORC_NULLPTR;

    // Read command-line options
    char *param, *value;
    for (int i = 1; i < argc; i++) {
      if ( (param = std::strstr(argv[i], COLUMNS_PREFIX.c_str())) ) {
        value = std::strtok(param+COLUMNS_PREFIX.length(), "," );
        while (value) {
          cols.push_back(static_cast<uint64_t>(std::atoi(value)));
          value = std::strtok(ORC_NULLPTR, "," );
        }
      } else {
        filename = argv[i];
      }
    }
    orc::RowReaderOptions rowReaderOpts;
    if (cols.size() > 0) {
      rowReaderOpts.include(cols);
    }
    if (filename != ORC_NULLPTR) {
      printContents(filename, rowReaderOpts);
    }
    Aws::ShutdownAPI(options);
  } catch (std::exception& ex) {
    std::cerr << "Caught exception: " << ex.what() << "\n";
    return 1;
  }
  return 0;
}
