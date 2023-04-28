#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <iostream>
#include <iomanip>
#include <sstream>
#include <fstream>
#include <chrono>
#include <thread>

#include <sys/queue.h>

#include <arpa/inet.h>

#include <openssl/sha.h>
#include <openssl/hmac.h>

#include <event2/keyvalq_struct.h>
#include <event2/event.h>
#include <event2/http.h>
#include <event2/buffer.h>
#include <event2/bufferevent_ssl.h>

#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentialsProviderChain.h>
#include <aws/core/auth/AWSCredentialsProvider.h>

#include <S3Client.h>

using namespace starling;

std::atomic<long> S3Client::numReads(0);
std::atomic<long> S3Client::numWrites(0);

/**
  * Encodes a binary String as a hex string
  */
const std::string S3Client::HMAC_SHA256(const std::string &signingKey, const std::string &stringToSign){
  unsigned char md[SHA256_DIGEST_LENGTH];
  unsigned int outlen = 0;
  auto test = HMAC(EVP_sha256(), (const unsigned char *)signingKey.c_str(), signingKey.size(), (const unsigned char *)stringToSign.c_str(), stringToSign.size(), md, &outlen); 

  return std::string((const char *)md, outlen);
}

/**
  * Encodes a binary String as a hex string
  */
const std::string S3Client::HexEncode(const std::string &binary){
  std::stringstream ss;
  const unsigned char * binary_str = (const unsigned char *)binary.c_str();
  for (int i = 0; i < binary.size(); ++i){
    ss << std::hex << std::setfill('0') << std::setw(2) << (int) binary_str[i];
  }
  return ss.str();
}


const std::string S3Client::SHA256_MD(const std::string &message){
  SHA256_CTX sha;
  SHA256_Init(&sha);
  SHA256_Update(&sha, message.c_str(), message.size());
  unsigned char md[SHA256_DIGEST_LENGTH];
  SHA256_Final(md, &sha);
  return std::string((char *)md, SHA256_DIGEST_LENGTH);
}



void S3Client::cleanup(){
  payload == nullptr;
  payloadSize = 0;
  returnedPayload = nullptr;
  headers.clear();
  queryParameters.clear();
  canonicalUri = "";
  submittedUri = "";
  duplicateRequestSent = false;
  contentLength = 0;
  returnCode = 0;
  lastBufSize = -1;
}

void S3Client::GetTimeoutFromBytes(struct timeval *tv, unsigned long bytes){
  long useconds_latency = 100*1000;
  //xput is about 30MB/s 
  long useconds_throughput = bytes/30;
  if (read){
    useconds_throughput *= concurrentWorkers;
  }
  long useconds_total = useconds_latency+useconds_throughput;
  //std::cout << "Timeout set at " << useconds_total << "us " << " for " << bytes << " bytes." <<  std::endl;
  //useconds_total = 1000*1000*1000;
  //reading in parallel, so is probably slower
  //useconds_total = 100*1000*1000;
  tv->tv_sec = useconds_total/(1*1000*1000); 
  tv->tv_usec = useconds_total%(1*1000*1000); 
}



const std::string S3Client::GetSigningKey(const std::string &date){
  std::string sak = creds.GetAWSSecretKey();
  return HMAC_SHA256(HMAC_SHA256(HMAC_SHA256(HMAC_SHA256("AWS4"+sak, date), region), service), "aws4_request");
}


void S3Client::on_event_cb(evhtp_connection_t *conn, short event, void * arg){

  S3Client *s3client = (S3Client*)arg;
  //std::cout << "Got events " << event << std::endl;
  if (event & BEV_EVENT_CONNECTED){
    auto now = std::chrono::steady_clock::now();
    //std::cout << "conn time " << (conn == s3client->httpconn[0] ? 0 : 1) << " : " <<  std::chrono::duration_cast<std::chrono::milliseconds>(now-s3client->start_conn_time).count() << std::endl;
  }
  if (event & BEV_EVENT_EOF){
    //std::cout << "Got EOF: " << (int) event << std::endl;
    int conn_num = conn==s3client->httpconn[0] ? 0 : 1;
    s3client->connectHttp(conn_num);
    s3client->startRequest(conn_num);
    return;
  }
  if (event & BEV_EVENT_READING){
    //std::cout << "Got READING: " << (int) event << std::endl;
  }
  if (event & BEV_EVENT_WRITING){
    //std::cout << "Got writing: " << (int) event << std::endl;
  }
  if (event & BEV_EVENT_ERROR){
    //std::cout << "Got error: " << (int) event << std::endl;
    int conn_num = conn==s3client->httpconn[0] ? 0 : 1;
    s3client->connectHttp(conn_num);
    s3client->startRequest(conn_num);
    return;
  }
  if (event & BEV_EVENT_TIMEOUT){
    std::cout << "Got timeout: " << (int) event << std::endl;
  }

  else{
  }
}

evhtp_res S3Client::write_cb(evhtp_connection_t *conn, void * arg){
  S3Client *s3client = (S3Client*)arg;
  if (conn->request->method == htp_method_PUT && conn == s3client->httpconn[0]){
    //start timer
    struct timeval tv{};
    tv.tv_sec = 0;
    tv.tv_usec = 300*1000; //most responses come back within 300 ms
    auto now = std::chrono::steady_clock::now();
    evtimer_del(s3client->write_speed_timer_ev);
    evtimer_add(s3client->timer_ev, &tv);
  }
  return EVHTP_RES_OK;
}

static evhtp_res new_read_cb(evhtp_request_t *req, evbuffer *buf , void * arg){
  std::cout << "read: " <<req<< std::endl;
  return EVHTP_RES_OK;
}

static evhtp_res new_connection_fini_cb(evhtp_connection_t* conn, void*arg){
  std::cout << "connection fini" << std::endl;
  return EVHTP_RES_OK;

}

static evhtp_res print_data(evhtp_request_t * req, evbuf_t * buf, void * arg){
  std::cout << "printing data" << std::endl;
  return EVHTP_RES_OK;
}

static evhtp_res new_header_cb(evhtp_request_t * req, evhtp_header_t *hdr, void * arg){
  if (req->method == htp_method_PUT){
    std::cout << "header " <<hdr->key << " = " << hdr->val<< std::endl;
  }
  return EVHTP_RES_OK;
}

evhtp_res S3Client::new_headers_cb(evhtp_request_t * req, evhtp_headers_t *hdrs, void * arg){
  S3Client *s3client = (S3Client*)arg;
  if (req->method == htp_method_PUT){
    auto now = std::chrono::steady_clock::now();
  }
  return req->status;
}

static evhtp_res new_request_fini_cb(evhtp_request_t * req, void * arg){
  return EVHTP_RES_OK;
}

static evhtp_res new_conn_error_cb(evhtp_connection_t *req, evhtp_error_flags errtype , void * arg){
  std::cout << "conn_error" << std::endl;
  return EVHTP_RES_OK;
}


void S3Client::startRequest(int request_num){
  requests[request_num] = evhtp_request_new(S3Client::on_complete_cb, this);
  evhtp_request *req = requests[request_num];
  evhtp_request_set_keepalive(req, 1);
  for (auto it : headers){
    evhtp_headers_add_header(req->headers_out, evhtp_header_new(it.first.c_str(), it.second.c_str(), 1, 1));
  }
  if (payload != nullptr){
    evbuffer_add(req->buffer_out, payload, payloadSize);
  }
  //evhtp_request_set_hook(req, evhtp_hook_on_error, (evhtp_hook)on_error_cb, this);
  evhtp_request_set_hook(req, evhtp_hook_on_headers, (evhtp_hook)new_headers_cb, this);

  if (evhtp_make_request(httpconn[request_num], req , cmd, submittedUri.c_str())){
    std::cout << "make request failed" << std::endl;
    exit(1);
  }
  switch(cmd){
    case htp_method_GET:
    case htp_method_HEAD:
      S3Client::numReads++;
      break;
    case htp_method_PUT:
      S3Client::numWrites++;
      break;
    default:
      break;
  }
}

void S3Client::startSecondRequest(){
  duplicateRequestSent = true;
  if (httpconn[1] == nullptr){
    connectHttp(1);
  }
  startRequest(1);
}

/*
 * host should be in the headers
 */
const std::string S3Client::CreateCanonicalRequest(const std::string &httpVerb){
  std::stringstream req_ss;
  req_ss << httpVerb << "\n";
  req_ss << canonicalUri << "\n";
  for (auto it = queryParameters.begin(); it != queryParameters.end(); ++it){
    req_ss << it->first << "=" <<it->second;
    if (it != --queryParameters.end()){
      req_ss << "&";
    }
  }
  req_ss << "\n";

  for (auto it = headers.begin(); it != headers.end(); ++it){
    req_ss << it->first << ":" << it->second << "\n";
  }
  req_ss << "\n";

  for (auto it = headers.begin(); it != headers.end(); ++it){
    req_ss << it->first; 
    if (it != --headers.end()){
      req_ss << ";";
    }
  }
  req_ss << "\n";
  req_ss << "UNSIGNED-PAYLOAD";
  return req_ss.str();
}


const std::string S3Client::CreateStringToSign(const std::string &timestr, const std::string &datestr, const std::string canonicalRequest){
  std::stringstream sts_ss;
  sts_ss << "AWS4-HMAC-SHA256\n";
  sts_ss << timestr  << "\n";
  sts_ss << datestr << "/" << region << "/" << service << "/aws4_request" << "\n";
  sts_ss << HexEncode(SHA256_MD(canonicalRequest));
  return sts_ss.str();
}

const std::string S3Client::CreateAuthorizationString(const std::string &datestr, const std::string stringToSign){
  const std::string signingKey = GetSigningKey(datestr);
  std::stringstream auth_ss;
  auth_ss << "AWS4-HMAC-SHA256 ";
  auth_ss << "Credential=" << creds.GetAWSAccessKeyId() << "/" << datestr << "/" << region << "/" << service << "/" << "aws4_request" << ",";
  auth_ss << "SignedHeaders=" ; 
  for (auto it = headers.begin(); it != headers.end(); it++){
    auth_ss << it->first; 
    if (it != --headers.end()){
      auth_ss << ";";
    }
  }
  auth_ss << ",";
  auth_ss << "Signature="<<HexEncode(HMAC_SHA256(signingKey, stringToSign));
  return auth_ss.str();
}

void S3Client::CreateAwsRequest(enum htp_method command){
  numRequests++;
  std::string httpVerb;
  cmd = command;
  switch(command){
    case htp_method_GET:
      httpVerb = "GET";
      break;
    case htp_method_PUT:
      httpVerb = "PUT";
      break;
    case htp_method_HEAD:
      httpVerb = "HEAD";
      break;
    default:
      std::cout << "unsupported command type" << std::endl;
      exit(1);
  }
  if (creds.GetSessionToken().size() > 0){
    headers["x-amz-security-token"] = creds.GetSessionToken();
  }

  time_t now;
  time(&now);
  char timestrbuf[sizeof("YYYYMMDDTHHmmSSZ")+1];
  strftime(timestrbuf, sizeof(timestrbuf), "%Y%m%dT%H%M%SZ", gmtime(&now));
  std::string timestr = timestrbuf;
  char datestrbuf[sizeof("YYYYMMDD")+1];
  strftime(datestrbuf, sizeof(datestrbuf), "%Y%m%d", gmtime(&now));
  std::string datestr = datestrbuf;
  headers["host"] = host;
  headers["x-amz-content-sha256"] = "UNSIGNED-PAYLOAD";
  headers["x-amz-date"] = timestr;
  if (command == htp_method_PUT){
    //headers["x-amz-storage-class"] = "REDUCED_REDUNDANCY";
  }
  const std::string canonicalRequest = CreateCanonicalRequest(httpVerb);
  const std::string stringToSign = CreateStringToSign(timestr, datestr, canonicalRequest); 
  headers["Authorization"] = CreateAuthorizationString(datestr, stringToSign);

  std::stringstream uri_ss;
  uri_ss << "http://" << host << canonicalUri;
  if (queryParameters.size() > 0){
    uri_ss << "?";
    for (auto it = queryParameters.begin(); it != queryParameters.end(); ++it){
      uri_ss << it->first << "=" <<it->second;
      if (it != --queryParameters.end()){
        uri_ss << "&";
      }
    }
  }
  submittedUri = uri_ss.str();

  start_time = std::chrono::steady_clock::now();
  startRequest(0);
}


void S3Client::connectHttp(int conn_num){
  char port_buf[6];
  struct evutil_addrinfo hints{};
  evutil_snprintf(port_buf, sizeof(port_buf), "%d", 80);
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_protocol = IPPROTO_TCP;
  hints.ai_flags = EVUTIL_AI_ADDRCONFIG;
  struct evutil_addrinfo *answer = nullptr;
  auto start_dns = std::chrono::steady_clock::now();
  int err = evutil_getaddrinfo(host.c_str(), port_buf, &hints, &answer);
  auto end_dns = std::chrono::steady_clock::now();
  //std::cout << "DNS time " << conn_num << " : " <<  std::chrono::duration_cast<std::chrono::milliseconds>(end_dns-start_dns).count() << std::endl;
  char * ip_addr = inet_ntoa(((struct in_addr)((struct sockaddr_in*)answer->ai_addr)->sin_addr));
  evutil_freeaddrinfo(answer);
  start_conn_time = std::chrono::steady_clock::now();
  httpconn[conn_num] = evhtp_connection_new(base, ip_addr, 80);

  evhtp_connection_set_hook(httpconn[conn_num], evhtp_hook_on_write, (evhtp_hook)S3Client::write_cb, this);
  evhtp_connection_set_hook(httpconn[conn_num], evhtp_hook_on_event, (evhtp_hook)S3Client::on_event_cb, this);
}

S3Client::S3Client(const std::string &bucket, const std::string &region, int concurrentWorkers) : bucket(bucket), region(region), service("s3"), concurrentWorkers(concurrentWorkers){

  base = event_base_new();
  host = bucket+".s3.amazonaws.com";
  creds = Aws::Auth::DefaultAWSCredentialsProviderChain().GetAWSCredentials();
  payload = nullptr;
  httpconn[1] = nullptr;
  connectHttp(0);
  //establish two connections
  //TODO: measure DNS lookups

  cleanup();
}

S3Client::~S3Client(){
  evhtp_connection_free(httpconn[0]);
  if (httpconn[1] != nullptr){
    evhtp_connection_free(httpconn[1]);
  }

  evhtp_safe_free(base, event_base_free);
}

void S3Client::ResetConnections(){
  evhtp_connection_free(httpconn[0]);
  if (httpconn[1] != nullptr){
    evhtp_connection_free(httpconn[1]);
  }
  connectHttp(0);
  return;
}

long S3Client::GetNumRequests() const{
  return numRequests;
}


/*
 * first and last byte is exclusive
 */
int S3Client::GetObjectWithRange(const std::string &key, unsigned long start, unsigned long length, char * data){
  canonicalUri = "/" + key;
  returnedPayload = data;
  std::stringstream range_ss;
  range_ss <<"bytes="<< start << "-" << start+length-1;
  headers["range"] = range_ss.str();
  CreateAwsRequest(htp_method_GET); 
  struct timeval tv{};
  GetTimeoutFromBytes(&tv, length);
  timer_ev = evtimer_new(base, S3Client::read_timer_cb, this);
  evtimer_add(timer_ev, &tv);
  event_base_dispatch(base);
  event_free(timer_ev);
  int thisReturnCode = returnCode;
  cleanup();
  
  return thisReturnCode;
}

/*
 * first and last byte is exclusive
 */
long S3Client::GetObjectLength(const std::string &key){
  long length = 0;
  canonicalUri = "/" + key;
  std::stringstream range_ss;
  CreateAwsRequest(htp_method_HEAD); 
  struct timeval tv{};
  GetTimeoutFromBytes(&tv, 0);
  timer_ev = evtimer_new(base, S3Client::read_timer_cb, this);
  evtimer_add(timer_ev, &tv);
  event_base_dispatch(base);
  event_free(timer_ev);
  length = contentLength;
  cleanup();
  return length;
}

/*
 * first and last byte is exclusive
 */
void S3Client::PutObject(const std::string &key, const std::string &data){
  PutObject(key, data.c_str(), data.size());
}

/*
 * first and last byte is exclusive
 */
void S3Client::PutObject(const std::string &key, const char * in_payload, int in_payloadSize){
  std::cout << "Putting: " << key << " " << in_payloadSize/(1024*1024) << std::endl;
  payload = in_payload;
  payloadSize = in_payloadSize;
  canonicalUri = "/" + key;
  CreateAwsRequest(htp_method_PUT); 
  struct timeval tv{};
  GetTimeoutFromBytes(&tv, in_payloadSize);
  //std::cout << "Timeout set to : " << (tv.tv_sec*1000+tv.tv_usec/1000) << std::endl;
  timer_ev = evtimer_new(base, S3Client::written_timer_cb, this);
  write_speed_timer_ev = evtimer_new(base, S3Client::write_speed_timer_cb, this);
  evtimer_add(write_speed_timer_ev, &tv);
  event_base_dispatch(base);
  event_free(timer_ev);
  event_free(write_speed_timer_ev);
  cleanup();
}

void S3Client::on_error_cb(evhtp_request_t *req, evhtp_error_flags errtype , void * arg){
  std::cout << "Error: " << (int)errtype << std::endl;
}

void S3Client::read_timer_cb(evutil_socket_t, short event, void * args){
  S3Client *s3client = (S3Client*)args;
  s3client->startSecondRequest();
}

void S3Client::write_speed_timer_cb(evutil_socket_t, short event, void * args){
  S3Client *s3client = (S3Client*)args;
  if (s3client->allow_failure){
    evhtp_request_free(s3client->requests[0]);
    evhtp_connection_free(s3client->httpconn[0]);
    event_base_loopexit(s3client->base, nullptr);
    return;
  }
  s3client->startSecondRequest();
  
}

void S3Client::written_timer_cb(evutil_socket_t, short event, void * args){
  S3Client *s3client = (S3Client*)args;
  evhtp_request_free(s3client->requests[0]);
  evhtp_connection_free(s3client->httpconn[0]);
  s3client->connectHttp(0);
  if (s3client->duplicateRequestSent){
    evhtp_request_free(s3client->requests[1]);
    evhtp_connection_free(s3client->httpconn[1]);
    s3client->httpconn[1] = nullptr;
  }else{
    evtimer_del(s3client->write_speed_timer_ev);
  }
  event_base_loopexit(s3client->base, nullptr);
}

void S3Client::on_complete_cb(struct evhtp_request *req , void * args){
  S3Client *s3client = (S3Client*)args;

  s3client->contentLength = evhtp_request_content_len(req);

  s3client->returnCode = evhtp_request_status(req);
  if (s3client->returnCode >= 300){
    std::cout << "Response line: " << s3client->returnCode << std::endl; 

    char buffer[1024];
    int nread;
    while ((nread = evbuffer_remove(req->buffer_in,
            buffer, sizeof(buffer)))
             > 0) {
      fwrite(buffer, nread, 1, stdout);
    }
        
    fflush(stdout);
    //cleanup remaining requests
    if (s3client->duplicateRequestSent){
      if (req == s3client->requests[0]){
        evhtp_request_free(s3client->requests[1]);
        evhtp_connection_free(s3client->httpconn[1]);
        s3client->httpconn[1] = nullptr;
        std::cout << "cleanup after errror: cancelling second request" << std::endl;
      }else{
        evhtp_request_free(s3client->requests[0]);
        evhtp_connection_free(s3client->httpconn[0]);
        //make the already running connection our first choice next time
        s3client->httpconn[0] = s3client->httpconn[1];
        s3client->httpconn[1] = nullptr;
        std::cout << "cleanup after error: cancelling first request" << std::endl;
      }
    }else{
      evtimer_del(s3client->timer_ev);
    }
    evhtp_request_free(req);
    event_base_loopexit(s3client->base, nullptr);
    //s3client->ResetConnections();
    return;
  }
  int nread = 0;


  if (s3client->returnedPayload != nullptr){ 
    int readSize = 4*1024*1024;
    int totalRead = 0;
    while ((nread = evbuffer_remove(req->buffer_in,
            s3client->returnedPayload+totalRead, readSize))
             > 0) {
      totalRead += nread;
    }
  }
  else{
    char buffer[1024];
    while ((nread = evbuffer_remove(req->buffer_in,
            buffer, sizeof(buffer)))
             > 0) {
      fwrite(buffer, nread, 1, stdout);
    }
  }

  if (s3client->duplicateRequestSent){
    if (req == s3client->requests[0]){
      evhtp_request_free(s3client->requests[1]);
      evhtp_connection_free(s3client->httpconn[1]);
      s3client->httpconn[1] = nullptr;

    }else{
      evhtp_request_free(s3client->requests[0]);
      evhtp_connection_free(s3client->httpconn[0]);
      //make the already running connection our first choice next time
      s3client->httpconn[0] = s3client->httpconn[1];
      s3client->httpconn[1] = nullptr;
    }
  }else{
    evtimer_del(s3client->timer_ev);
  }
  if (req->method == htp_method_PUT){
    evtimer_del(s3client->timer_ev);
    evtimer_del(s3client->write_speed_timer_ev);
  }
  evhtp_request_free(req);

  event_base_loopexit(s3client->base, nullptr);
}


long S3Client::GetNumReads(){
  return S3Client::numReads.load();
}

long S3Client::GetNumWrites(){
  return S3Client::numWrites.load();
}

void S3Client::SetAllowFailure(bool allow_failure){
  this->allow_failure = allow_failure;
}

