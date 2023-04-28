#pragma once

#include <orc/OrcFile.hh>
#include <orc/Vector.hh>

#include <thread>
#include <S3Client.h>

namespace starling{

class S3FileInputStream : public orc::InputStream{
  private:
    struct S3Extant{
      uint64_t offset;
      uint64_t length;
      std::unique_ptr<char[]> data;

      S3Extant(){}
      S3Extant(uint64_t offset, uint64_t length, std::unique_ptr<char[]>&& data) :offset(offset), length(length), data(std::move(data)){}

      
      S3Extant(S3Extant && other): offset(other.offset), length(other.length), data(std::move(other.data)){
      }
    };

    std::string bucket;
    std::string key;
    std::string filename;
    uint64_t fileLength;
    S3Client s3;
    static const uint64_t min_read_size;
    std::map<uint64_t, S3Extant> fetchedData;
    std::mutex mux;
  public:
    S3FileInputStream(const std::string &bucket, const std::string &key, int concurrentThreads);
    ~S3FileInputStream();

    uint64_t getLength() const override;

    uint64_t getNaturalReadSize() const override;

    int getNumReads() const;

    void read(void* buf, uint64_t length, uint64_t offset) override;

    const std::string& getName() const override;
};

} //namespace starling
