// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "rgw_d3n_cacherequest.h" //For porting remote Op stuff
#include "rgw_d3n_datacache.h"
#include "rgw_rest_client.h"
#include "rgw_auth_s3.h"
#include "rgw_op.h"
#include "rgw_common.h"
#include "rgw_auth_s3.h"
#include "rgw_op.h"
#include "rgw_crypt_sanitize.h"

#if __has_include(<filesystem>)
#include <filesystem>
namespace efs = std::filesystem;
#else
#include <experimental/filesystem>
namespace efs = std::experimental::filesystem;
#endif

#define dout_subsys ceph_subsys_rgw
struct RemoteRequest; //For submit_remote_req --Daniel

static const std::string base64_chars =
"ABCDEFGHIJKLMNOPQRSTUVWXYZ"
"abcdefghijklmnopqrstuvwxyz"
"0123456789+/";

using namespace std;

int D3nCacheAioWriteRequest::d3n_prepare_libaio_write_op(bufferlist& bl, unsigned int len, string oid, string cache_location)
{
  std::string location = cache_location + oid;
  int r = 0;

  lsubdout(g_ceph_context, rgw_datacache, 20) << "D3nDataCache: " << __func__ << "(): Write To Cache, location=" << location << dendl;
  cb = new struct aiocb;
  mode_t mode = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;
  memset(cb, 0, sizeof(struct aiocb));
  r = fd = ::open(location.c_str(), O_WRONLY | O_CREAT | O_TRUNC, mode);
  if (fd < 0) {
    ldout(cct, 0) << "ERROR: D3nCacheAioWriteRequest::create_io: open file failed, errno=" << errno << ", location='" << location.c_str() << "'" << dendl;
    goto done;
  }
  if (g_conf()->rgw_d3n_l1_fadvise != POSIX_FADV_NORMAL)
    posix_fadvise(fd, 0, 0, g_conf()->rgw_d3n_l1_fadvise);
  cb->aio_fildes = fd;

  data = malloc(len);
  if (!data) {
    ldout(cct, 0) << "ERROR: D3nCacheAioWriteRequest::create_io: memory allocation failed" << dendl;
    goto close_file;
  }
  cb->aio_buf = data;
  memcpy((void*)data, bl.c_str(), len);
  cb->aio_nbytes = len;
  goto done;

close_file:
  ::close(fd);
done:
  return r;
}

D3nDataCache::D3nDataCache()
  : cct(nullptr), io_type(_io_type::ASYNC_IO), free_data_cache_size(0), outstanding_write_size(0)
{
  lsubdout(g_ceph_context, rgw_datacache, 5) << "D3nDataCache: " << __func__ << "()" << dendl;
}

void D3nDataCache::init(CephContext *_cct) {
  cct = _cct;
  free_data_cache_size = cct->_conf->rgw_d3n_l1_datacache_size;
  head = nullptr;
  tail = nullptr;
  //FOR REMOTE REQUESTS
  datalake_hit = 0;
  remote_hit = 0;
  //PORTING ENDS
  cache_location = cct->_conf->rgw_d3n_l1_datacache_persistent_path;
  if(cache_location.back() != '/') {
      cache_location += "/";
  }
  try {
    if (efs::exists(cache_location)) {
      // d3n: evict the cache storage directory
      if (g_conf()->rgw_d3n_l1_evict_cache_on_start) {
        lsubdout(g_ceph_context, rgw, 5) << "D3nDataCache: init: evicting the persistent storage directory on start" << dendl;
        for (auto& p : efs::directory_iterator(cache_location)) {
          efs::remove_all(p.path());
        }
      }
    } else {
      // create the cache storage directory
      lsubdout(g_ceph_context, rgw, 5) << "D3nDataCache: init: creating the persistent storage directory on start" << dendl;
      efs::create_directories(cache_location);
    }
  } catch (const efs::filesystem_error& e) {
    lderr(g_ceph_context) << "D3nDataCache: init: ERROR initializing the cache storage directory '" << cache_location <<
                              "' : " << e.what() << dendl;
  }

  auto conf_eviction_policy = cct->_conf.get_val<std::string>("rgw_d3n_l1_eviction_policy");
  ceph_assert(conf_eviction_policy == "lru" || conf_eviction_policy == "random");
  if (conf_eviction_policy == "lru")
    eviction_policy = _eviction_policy::LRU;
  if (conf_eviction_policy == "random")
    eviction_policy = _eviction_policy::RANDOM;

#if defined(HAVE_LIBAIO)
  // libaio setup
  struct aioinit ainit{0};
  ainit.aio_threads = cct->_conf.get_val<int64_t>("rgw_d3n_libaio_aio_threads");
  ainit.aio_num = cct->_conf.get_val<int64_t>("rgw_d3n_libaio_aio_num");
  ainit.aio_idle_time = 120;
  aio_init(&ainit);
#endif
}

int D3nDataCache::d3n_io_write(bufferlist& bl, unsigned int len, std::string oid)
{
  D3nChunkDataInfo* chunk_info = new D3nChunkDataInfo;
  std::string location = cache_location + oid;

  lsubdout(g_ceph_context, rgw_datacache, 20) << "D3nDataCache: " << __func__ << "(): location=" << location << dendl;
  FILE *cache_file = nullptr;
  int r = 0;
  size_t nbytes = 0;

  cache_file = fopen(location.c_str(), "w+");
  if (cache_file == nullptr) {
    ldout(cct, 0) << "ERROR: D3nDataCache::fopen file has return error, errno=" << errno << dendl;
    return -errno;
  }

  nbytes = fwrite(bl.c_str(), 1, len, cache_file);
  if (nbytes != len) {
    ldout(cct, 0) << "ERROR: D3nDataCache::io_write: fwrite has returned error: nbytes!=len, nbytes=" << nbytes << ", len=" << len << dendl;
    return -EIO;
  }

  r = fclose(cache_file);
  if (r != 0) {
    ldout(cct, 0) << "ERROR: D3nDataCache::fclsoe file has return error, errno=" << errno << dendl;
    return -errno;
  }

  { // update cahce_map entries for new chunk in cache
    const std::lock_guard l(d3n_cache_lock);
    chunk_info->oid = oid;
    chunk_info->set_ctx(cct);
    chunk_info->size = len;
    d3n_cache_map.insert(pair<string, D3nChunkDataInfo*>(oid, chunk_info));
  }

  return r;
}

void d3n_libaio_write_cb(sigval sigval)
{
  lsubdout(g_ceph_context, rgw_datacache, 30) << "D3nDataCache: " << __func__ << "()" << dendl;
  D3nCacheAioWriteRequest* c = static_cast<D3nCacheAioWriteRequest*>(sigval.sival_ptr);
  c->priv_data->d3n_libaio_write_completion_cb(c);
}


void D3nDataCache::d3n_libaio_write_completion_cb(D3nCacheAioWriteRequest* c)
{
  D3nChunkDataInfo* chunk_info{nullptr};

  ldout(cct, 5) << "D3nDataCache: " << __func__ << "(): oid=" << c->oid << dendl;

  { // update cache_map entries for new chunk in cache
    const std::lock_guard l(d3n_cache_lock);
    d3n_outstanding_write_list.erase(c->oid);
    chunk_info = new D3nChunkDataInfo;
    chunk_info->oid = c->oid;
    chunk_info->set_ctx(cct);
    chunk_info->size = c->cb->aio_nbytes;
    d3n_cache_map.insert(pair<string, D3nChunkDataInfo*>(c->oid, chunk_info));
  }

  { // update free size
    const std::lock_guard l(d3n_eviction_lock);
    free_data_cache_size -= c->cb->aio_nbytes;
    outstanding_write_size -= c->cb->aio_nbytes;
    lru_insert_head(chunk_info);
  }
  delete c;
  c = nullptr;
}

int D3nDataCache::d3n_libaio_create_write_request(bufferlist& bl, unsigned int len, std::string oid)
{
  lsubdout(g_ceph_context, rgw_datacache, 30) << "D3nDataCache: " << __func__ << "(): Write To Cache, oid=" << oid << ", len=" << len << dendl;
  struct D3nCacheAioWriteRequest* wr = new struct D3nCacheAioWriteRequest(cct);
  int r=0;
  if ((r = wr->d3n_prepare_libaio_write_op(bl, len, oid, cache_location)) < 0) {
    ldout(cct, 0) << "ERROR: D3nDataCache: " << __func__ << "() prepare libaio write op r=" << r << dendl;
    goto done;
  }
  wr->cb->aio_sigevent.sigev_notify = SIGEV_THREAD;
  wr->cb->aio_sigevent.sigev_notify_function = d3n_libaio_write_cb;
  wr->cb->aio_sigevent.sigev_notify_attributes = nullptr;
  wr->cb->aio_sigevent.sigev_value.sival_ptr = (void*)wr;
  wr->oid = oid;
  wr->priv_data = this;

  if ((r = ::aio_write(wr->cb)) != 0) {
    ldout(cct, 0) << "ERROR: D3nDataCache: " << __func__ << "() aio_write r=" << r << dendl;
    goto error;
  }
  return 0;

error:
  delete wr;
done:
  return r;
}

void D3nDataCache::put(bufferlist& bl, unsigned int len, std::string& oid)
{
  size_t sr = 0;
  uint64_t freed_size = 0, _free_data_cache_size = 0, _outstanding_write_size = 0;

  ldout(cct, 10) << "D3nDataCache::" << __func__ << "(): oid=" << oid << ", len=" << len << dendl;
  {
    const std::lock_guard l(d3n_cache_lock);
    std::unordered_map<string, D3nChunkDataInfo*>::iterator iter = d3n_cache_map.find(oid);
    if (iter != d3n_cache_map.end()) {
      ldout(cct, 10) << "D3nDataCache::" << __func__ << "(): data already cached, no rewrite" << dendl;
      return;
    }
    auto it = d3n_outstanding_write_list.find(oid);
    if (it != d3n_outstanding_write_list.end()) {
      ldout(cct, 10) << "D3nDataCache: NOTE: data put in cache already issued, no rewrite" << dendl;
      return;
    }
    d3n_outstanding_write_list.insert(oid);
  }
  {
    const std::lock_guard l(d3n_eviction_lock);
    _free_data_cache_size = free_data_cache_size;
    _outstanding_write_size = outstanding_write_size;
  }
  ldout(cct, 20) << "D3nDataCache: Before eviction _free_data_cache_size:" << _free_data_cache_size << ", _outstanding_write_size:" << _outstanding_write_size << ", freed_size:" << freed_size << dendl;
  while (len > (_free_data_cache_size - _outstanding_write_size + freed_size)) {
    ldout(cct, 20) << "D3nDataCache: enter eviction" << dendl;
    if (eviction_policy == _eviction_policy::LRU) {
      sr = lru_eviction();
    } else if (eviction_policy == _eviction_policy::RANDOM) {
      sr = random_eviction();
    } else {
      ldout(cct, 0) << "D3nDataCache: Warning: unknown cache eviction policy, defaulting to lru eviction" << dendl;
      sr = lru_eviction();
    }
    if (sr == 0) {
      ldout(cct, 2) << "D3nDataCache: Warning: eviction was not able to free disk space, not writing to cache" << dendl;
      d3n_outstanding_write_list.erase(oid);
      return;
    }
    ldout(cct, 20) << "D3nDataCache: completed eviction of " << sr << " bytes" << dendl;
    freed_size += sr;
  }
  int r = 0;
  r = d3n_libaio_create_write_request(bl, len, oid);
  if (r < 0) {
    const std::lock_guard l(d3n_cache_lock);
    d3n_outstanding_write_list.erase(oid);
    ldout(cct, 1) << "D3nDataCache: create_aio_write_request fail, r=" << r << dendl;
    return;
  }

  const std::lock_guard l(d3n_eviction_lock);
  free_data_cache_size += freed_size;
  outstanding_write_size += len;
}

//PORTING BEGINS

static size_t _remote_req_cb(void *ptr, size_t size, size_t nmemb, void* param) {
  RemoteRequest *req = static_cast<RemoteRequest *>(param);
//  req->bl->append((char *)ptr, size*nmemb);
   req->s.append((char *)ptr, size*nmemb);
  //lsubdout(g_ceph_context, rgw, 1) << __func__ << " data is written "<< " key " << req->key << " size " << size*nmemb  << dendl;
  return size*nmemb;
}

std::string base64_encode(unsigned char const* bytes_to_encode, unsigned int in_len) {
  std::string ret;
  int i = 0;
  int j = 0;
  unsigned char char_array_3[3];
  unsigned char char_array_4[4];
  while (in_len--) {
    char_array_3[i++] = *(bytes_to_encode++);
    if (i == 3) {
      char_array_4[0] = (char_array_3[0] & 0xfc) >> 2;
      char_array_4[1] = ((char_array_3[0] & 0x03) << 4) + ((char_array_3[1] & 0xf0) >> 4);
      char_array_4[2] = ((char_array_3[1] & 0x0f) << 2) + ((char_array_3[2] & 0xc0) >> 6);
      char_array_4[3] = char_array_3[2] & 0x3f;

      for(i = 0; (i <4) ; i++)
	ret += base64_chars[char_array_4[i]];
      i = 0;
    }
  }

  if (i)
  {
    for(j = i; j < 3; j++)
      char_array_3[j] = '\0';

    char_array_4[0] = (char_array_3[0] & 0xfc) >> 2;
    char_array_4[1] = ((char_array_3[0] & 0x03) << 4) + ((char_array_3[1] & 0xf0) >> 4);
    char_array_4[2] = ((char_array_3[1] & 0x0f) << 2) + ((char_array_3[2] & 0xc0) >> 6);
    char_array_4[3] = char_array_3[2] & 0x3f;

    for (j = 0; (j < i + 1); j++)
      ret += base64_chars[char_array_4[j]];

    while((i++ < 3))
      ret += '=';
  }
  return ret;
}


class CacheThreadPool { //PORT THIS
  public:
    CacheThreadPool(int n) {
      for (int i=0; i<n; ++i) {
	threads.push_back(new PoolWorkerThread(workQueue));
	threads.back()->start();
      }
    }
    ~CacheThreadPool() {
      finish();
    }

    void addTask(Task *nt) {
      workQueue.addTask(nt);
    }

    void finish() {
      for (size_t i=0,e=threads.size(); i<e; ++i)
	workQueue.addTask(NULL);
      for (size_t i=0,e=threads.size(); i<e; ++i) {
	threads[i]->join();
	delete threads[i];
      }
      threads.clear();
    }

  private:
    std::vector<PoolWorkerThread*> threads;
    WorkQueue workQueue;
};

void D3nDataCache::submit_remote_req(RemoteRequest *c){
  std::string endpoint=cct->_conf->backend_url; //what is the backend_url?
  d3n_cache_lock.lock();

  ldout(cct, 1) << "submit_remote_req, dest " << c->dest << " endpoint "<< endpoint<< dendl;
  if ((c->dest).compare(endpoint) == 0) {
        datalake_hit ++;
	    ldout(cct, 1) << "submit_remote_req, datalake_hit " << datalake_hit<< dendl;
  } else {
        remote_hit++;
	    ldout(cct, 1) << "submit_remote_req, remote_hit " <<  remote_hit<< dendl;
  }
  d3n_cache_lock.unlock();

  tp->addTask(new RemoteS3Request(c, cct));
}

string RemoteS3Request::get_date(){
  time_t now = time(0);
  tm *gmtm = gmtime(&now);
  string date;
  char buffer[128];
  std::strftime(buffer,128,"%a, %d %b %Y %X %Z",gmtm);
  date = buffer;
  return date;
}   

int RemoteS3Request::submit_http_get_request_s3(){
  //int begin = req->ofs + req->read_ofs;
  //int end = req->ofs + req->read_ofs + req->read_len - 1;
  auto start = chrono::steady_clock::now();

  off_t begin = req->ofs;
  off_t end = req->ofs + req->read_len - 1;
  std::string range = std::to_string(begin)+ "-"+ std::to_string(end);
  if (req->dest.compare("")==0)
	req->dest = cct->_conf->backend_url;
  //std::string range = std::to_string( (int)req->ofs + (int)(req->read_ofs))+ "-"+ std::to_string( (int)(req->ofs) + (int)(req->read_ofs) + (int)(req->read_len - 1));
  ldout(cct, 10) << __func__  << " key " << req->key << " range " << range  << " dest "<< req->dest <<dendl;
  
  CURLcode res;
  string uri = "/"+ req->path;;
  //string uri = "/"+req->c_block->c_obj.bucket_name + "/" +req->c_block->c_obj.obj_name;
  string date = get_date();
   
  //string AWSAccessKeyId=req->c_block->c_obj.accesskey.id;
  //string YourSecretAccessKeyID=req->c_block->c_obj.accesskey.key;
  string AWSAccessKeyId=req->ak;
  string YourSecretAccessKeyID=req->sk;
  string signature = sign_s3_request("GET", uri, date, YourSecretAccessKeyID, AWSAccessKeyId);
  string Authorization = "AWS "+ AWSAccessKeyId +":" + signature;
  string loc =  req->dest + uri;
  string auth="Authorization: " + Authorization;
  string timestamp="Date: " + date;
  string user_agent="User-Agent: aws-sdk-java/1.7.4 Linux/3.10.0-514.6.1.el7.x86_64 OpenJDK_64-Bit_Server_VM/24.131-b00/1.7.0_131";
  string content_type="Content-Type: application/x-www-form-urlencoded; charset=utf-8";
  curl_handle = curl_easy_init();
  if(curl_handle) {
    struct curl_slist *chunk = NULL;
    chunk = curl_slist_append(chunk, auth.c_str());
    chunk = curl_slist_append(chunk, timestamp.c_str());
    chunk = curl_slist_append(chunk, user_agent.c_str());
    chunk = curl_slist_append(chunk, content_type.c_str());
    chunk = curl_slist_append(chunk, "CACHE_GET_REQ:rgw_datacache");
    curl_easy_setopt(curl_handle, CURLOPT_RANGE, range.c_str());
    res = curl_easy_setopt(curl_handle, CURLOPT_HTTPHEADER, chunk); //set headers
    curl_easy_setopt(curl_handle, CURLOPT_URL, loc.c_str());
    curl_easy_setopt(curl_handle, CURLOPT_FOLLOWLOCATION, 1L); //for redirection of the url
    curl_easy_setopt(curl_handle, CURLOPT_WRITEFUNCTION, _remote_req_cb); //COMMENTED OUT FOR PORTING REASONS
    curl_easy_setopt(curl_handle, CURLOPT_NOSIGNAL, 1L);
//    curl_easy_setopt(curl_handle, CURLOPT_VERBOSE, 1L);
    curl_easy_setopt(curl_handle, CURLOPT_FAILONERROR, 1L);
    curl_easy_setopt(curl_handle, CURLOPT_WRITEDATA, (void*)req);
    res = curl_easy_perform(curl_handle); //run the curl command
    curl_easy_reset(curl_handle);
    curl_slist_free_all(chunk);
    curl_easy_cleanup(curl_handle);
  }
  if(res == CURLE_HTTP_RETURNED_ERROR) {
   ldout(cct,10) << "__func__ " << " CURLE_HTTP_RETURNED_ERROR" <<curl_easy_strerror(res) << " key " << req->key << dendl;
   return -1;
  }
   auto end2 = chrono::steady_clock::now();
   ldout(cct,10) << __func__  << " done dest " <<req->dest << " milisecond " <<
   chrono::duration_cast<chrono::microseconds>(end2 - start).count() 
   << dendl; 
  if (res != CURLE_OK) { return -1;}
  else { return 0; }

}

void RemoteS3Request::run() {

  ldout(cct, 20) << __func__  <<dendl;
  int max_retries = cct->_conf->max_remote_retries;
  int r = 0;
  for (int i=0; i<max_retries; i++ ){
    if(!(r = submit_http_get_request_s3()) && (req->s.size() == (long unsigned int) req->read_len)){
       ldout(cct, 0) <<  __func__  << "remote get success"<<req->key << " r-id "<< req->r->id << dendl;
//       req->func(req);
        req->finish();
      	return;
    }
    if(req->s.size() != (long unsigned int) req->read_len){
//#if(req->bl->length() != r->read_len){
       req->s.clear();
    }
    req->s.clear();
    }

    if (r == ECANCELED) {
    ldout(cct, 0) << "ERROR: " << __func__  << "(): remote s3 request for failed, obj="<<req->key << dendl;
    req->r->result = -1;
    req->aio->put(*(req->r));
    return;
    }
}

string RemoteS3Request::sign_s3_request(string HTTP_Verb, string uri, string date, string YourSecretAccessKeyID, string AWSAccessKeyId){
  std::string Content_Type = "application/x-www-form-urlencoded; charset=utf-8";
  std::string Content_MD5 ="";
  std::string CanonicalizedResource = uri.c_str();
  std::string StringToSign = HTTP_Verb + "\n" + Content_MD5 + "\n" + Content_Type + "\n" + date + "\n" +CanonicalizedResource;
  char key[YourSecretAccessKeyID.length()+1] ;
  strcpy(key, YourSecretAccessKeyID.c_str());
  const char * data = StringToSign.c_str();
  unsigned char* digest;
  digest = HMAC(EVP_sha1(), key, strlen(key), (unsigned char*)data, strlen(data), NULL, NULL);
  std::string signature = base64_encode(digest, 20); //What is this? --Daniel
  return signature;
}
//PORTING ENDS

bool D3nDataCache::get(const string& oid, const off_t len)
{
  const std::lock_guard l(d3n_cache_lock);
  bool exist = false;
  string location = cache_location + oid;

  lsubdout(g_ceph_context, rgw_datacache, 20) << "D3nDataCache: " << __func__ << "(): location=" << location << dendl;
  std::unordered_map<string, D3nChunkDataInfo*>::iterator iter = d3n_cache_map.find(oid);
  if (!(iter == d3n_cache_map.end())) {
    // check inside cache whether file exists or not!!!! then make exist true;
    struct D3nChunkDataInfo* chdo = iter->second;
    struct stat st;
    int r = stat(location.c_str(), &st);
    if ( r != -1 && st.st_size == len) { // file exists and containes required data range length
      exist = true;
      /*LRU*/
      /*get D3nChunkDataInfo*/
      const std::lock_guard l(d3n_eviction_lock);
      lru_remove(chdo);
      lru_insert_head(chdo);
    } else {
      d3n_cache_map.erase(oid);
      const std::lock_guard l(d3n_eviction_lock);
      lru_remove(chdo);
      delete chdo;
      exist = false;
    }
  }
  return exist;
}

size_t D3nDataCache::random_eviction()
{
  lsubdout(g_ceph_context, rgw_datacache, 20) << "D3nDataCache: " << __func__ << "()" << dendl;
  int n_entries = 0;
  int random_index = 0;
  size_t freed_size = 0;
  D3nChunkDataInfo* del_entry;
  string del_oid, location;
  {
    const std::lock_guard l(d3n_cache_lock);
    n_entries = d3n_cache_map.size();
    if (n_entries <= 0) {
      return -1;
    }
    srand (time(NULL));
    random_index = ceph::util::generate_random_number<int>(0, n_entries-1);
    std::unordered_map<string, D3nChunkDataInfo*>::iterator iter = d3n_cache_map.begin();
    std::advance(iter, random_index);
    del_oid = iter->first;
    del_entry =  iter->second;
    ldout(cct, 20) << "D3nDataCache: random_eviction: index:" << random_index << ", free size: " << del_entry->size << dendl;
    freed_size = del_entry->size;
    delete del_entry;
    del_entry = nullptr;
    d3n_cache_map.erase(del_oid); // oid
  }

  location = cache_location + del_oid;
  ::remove(location.c_str());
  return freed_size;
}

size_t D3nDataCache::lru_eviction()
{
  lsubdout(g_ceph_context, rgw_datacache, 20) << "D3nDataCache: " << __func__ << "()" << dendl;
  int n_entries = 0;
  size_t freed_size = 0;
  D3nChunkDataInfo* del_entry;
  string del_oid, location;

  {
    const std::lock_guard l(d3n_eviction_lock);
    del_entry = tail;
    if (del_entry == nullptr) {
      ldout(cct, 2) << "D3nDataCache: lru_eviction: del_entry=null_ptr" << dendl;
      return 0;
    }
    lru_remove(del_entry);
  }

  {
    const std::lock_guard l(d3n_cache_lock);
    n_entries = d3n_cache_map.size();
    if (n_entries <= 0) {
      ldout(cct, 2) << "D3nDataCache: lru_eviction: cache_map.size<=0" << dendl;
      return -1;
    }
    del_oid = del_entry->oid;
    ldout(cct, 20) << "D3nDataCache: lru_eviction: oid to remove: " << del_oid << dendl;
    d3n_cache_map.erase(del_oid); // oid
  }
  freed_size = del_entry->size;
  delete del_entry;
  location = cache_location + del_oid;
  ::remove(location.c_str());
  return freed_size;
}


