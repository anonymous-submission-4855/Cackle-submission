#pragma once

#include <openssl/sha.h>
#include <openssl/hmac.h>

#include <evhtp.h>

#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentialsProviderChain.h>
#include <aws/core/auth/AWSCredentialsProvider.h>

namespace starling{


class S3Client{
  private:
    std::string bucket;
    std::string host;
    std::string region;
    std::string service;
    evbase_t* base;
    evhtp_connection_t *httpconn[2];
    evhtp_request_t *requests[2];
    struct event *timer_ev;
    struct event *write_speed_timer_ev;
    std::string signingString;
    Aws::Auth::AWSCredentials creds;

    std::map<std::string, std::string> headers;
    std::map<std::string, std::string> queryParameters;
    htp_method cmd;
    std::string canonicalUri;
    std::string submittedUri;
    const char * payload;
    unsigned long payloadSize;
    long contentLength;
    long expectedSize = 0;
    int returnCode = 0;
    int lastBufSize = 0;
    std::chrono::time_point<std::chrono::steady_clock> start_time;
    std::chrono::time_point<std::chrono::steady_clock> start_conn_time;
    std::chrono::time_point<std::chrono::steady_clock> last_time;

    bool allow_failure = false;

    static std::atomic<long> numReads;
    static std::atomic<long> numWrites;

    struct cb_args{
      void * client;
      int connectionId;
    };

    char * returnedPayload;

    bool duplicateRequestSent = false;
    int numRequests = 0;

    int concurrentWorkers;

    /*
     * Cleans up the state after completing a request
     */
    void cleanup();

    /*
      * Encodes a binary String as a hex string
      */
    static const std::string HMAC_SHA256(const std::string &signingKey, const std::string &stringToSign);

    /*
      * Encodes a binary String as a hex string
      */
    static const std::string HexEncode(const std::string &binary);

    /*
     * Create a SHA256 Message digest of a given message
     */
    static const std::string SHA256_MD(const std::string &message);

    
    /*
     * Sets a timeout (tv) based on how much we are reading, and the number of concrrent threads
     */
    void GetTimeoutFromBytes(struct timeval *tv, unsigned long bytes);

    /*
     * callback on Error, currently do nothing
     */
    static void on_error_cb(evhtp_request_t *req, evhtp_error_flags errtype , void * arg);

    static void written_timer_cb(evutil_socket_t, short event, void * args);

    static void write_speed_timer_cb(evutil_socket_t, short event, void * args);

    static void read_timer_cb(evutil_socket_t, short event, void * args);

    static void on_complete_cb(struct evhtp_request *req , void * args);

    static evhtp_res write_cb(evhtp_connection_t *conn, void * arg);

    static void on_event_cb(evhtp_connection_t *conn, short events, void * arg);

    static evhtp_res new_headers_cb(evhtp_request_t * req, evhtp_headers_t *hdrs, void * arg);

    void connectHttp(int conn_num);


    const std::string GetSigningKey(const std::string &date);

    void startRequest(int request_num);
    /*
     * Start a second request (usually on timeout)
     */
    void startSecondRequest();

    /*
     * Create the canonical request string
     */
    const std::string CreateCanonicalRequest(const std::string &httpVerb);

    const std::string CreateAuthorizationString(const std::string &datestr, const std::string stringToSign);

    const std::string CreateStringToSign(const std::string &timestr, const std::string &datestr, const std::string canonicalRequest);

    void CreateAwsRequest(enum htp_method command);

  public:
    
    S3Client(const std::string &bucket, const std::string &region, int concurrentWorkers);

    ~S3Client();

    /*
     * first and last byte is exclusive
     */
    int GetObjectWithRange(const std::string &key, unsigned long start, unsigned long length, char * data);

    long GetObjectLength(const std::string &key);

    void PutObject(const std::string &key, const std::string &data);

    void PutObject(const std::string &key, const char * in_payload, int in_payloadSize);

    long GetNumRequests() const;

    static long GetNumReads();

    static long GetNumWrites();

    void ResetConnections();

    void SetAllowFailure(bool allow_failure);
};

} //namespace starling
