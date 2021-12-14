// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#ifndef CEPH_RGWD3NDATACACHE_H
#define CEPH_RGWD3NDATACACHE_H

#include "rgw_rados.h"
#include <curl/curl.h>

#include "rgw_common.h"

#include <unistd.h>
#include <signal.h>
#include "include/Context.h"
#include "include/lru.h"
#include "rgw_d3n_cacherequest.h"
#include "rgw_threadpool.h" //D4N port
#include "rgw_directory.h"


/*D3nDataCache*/
struct D3nDataCache;

/*D4N port*/
struct RemoteRequest; 
class CacheThreadPool;




struct D3nChunkDataInfo : public LRUObject {
	CephContext *cct;
	uint64_t size;
	time_t access_time;
	std::string address;
	std::string oid;
	bool complete;
	struct D3nChunkDataInfo* lru_prev;
	struct D3nChunkDataInfo* lru_next;

	D3nChunkDataInfo(): size(0) {}

	void set_ctx(CephContext *_cct) {
		cct = _cct;
	}

	void dump(Formatter *f) const;
	static void generate_test_instances(std::list<D3nChunkDataInfo*>& o);
};

struct D3nCacheAioWriteRequest {
	cache_block *c_blk;
	std::string oid;
	void *data;
	int fd;
	struct aiocb *cb;
	D3nDataCache *priv_data;
	CephContext *cct;

	D3nCacheAioWriteRequest(CephContext *_cct) : cct(_cct) {}
	int d3n_prepare_libaio_write_op(bufferlist& bl, unsigned int len, std::string oid, std::string cache_location);

  ~D3nCacheAioWriteRequest() {
    ::close(fd);
		cb->aio_buf = nullptr;
		free(data);
		data = nullptr;
		delete(cb);
  }
};

struct D3nDataCache {

private:
  std::unordered_map<std::string, D3nChunkDataInfo*> d3n_cache_map;
  std::set<std::string> d3n_outstanding_write_list;
  std::mutex d3n_cache_lock;
  std::mutex d3n_eviction_lock;

  //FOR REMOTE REQUESTS
  int datalake_hit;
  int remote_hit;
  //may require initialization
  CephContext *cct;
  enum class _io_type {
    SYNC_IO = 1,
    ASYNC_IO = 2,
    SEND_FILE = 3
  } io_type;
  enum class _eviction_policy {
    LRU=0, RANDOM=1
  } eviction_policy;

  struct sigaction action;
  uint64_t free_data_cache_size = 0;
  uint64_t outstanding_write_size = 0;
  struct D3nChunkDataInfo* head;
  struct D3nChunkDataInfo* tail;
  //PORTING THREADPOOL
  CacheThreadPool *tp;
  CacheThreadPool *aging_tp;
  //PORTING ENDS



private:
  void add_io();

public:
  D3nDataCache();
  ~D3nDataCache() {
    while (lru_eviction() > 0);
  }

  std::string cache_location;
  RGWBlockDirectory *blk_dir;


  bool get(const std::string& oid, const off_t len);
  void put(bufferlist& bl, unsigned int len, std::string& obj_key);
  int d3n_io_write(bufferlist& bl, unsigned int len, std::string oid);
  int d3n_libaio_create_write_request(bufferlist& bl, unsigned int len, std::string oid);
  void d3n_libaio_write_completion_cb(D3nCacheAioWriteRequest* c);
  //TODO: add all submit_remote functions
  //PORTING BEGINS
  void submit_remote_req(struct RemoteRequest *c);
  //PORTING ENDS

  size_t random_eviction();
  size_t lru_eviction();

  void init(CephContext *_cct);

  void lru_insert_head(struct D3nChunkDataInfo* o) {
    lsubdout(g_ceph_context, rgw_datacache, 30) << "D3nDataCache: " << __func__ << "()" << dendl;
    o->lru_next = head;
    o->lru_prev = nullptr;
    if (head) {
      head->lru_prev = o;
    } else {
      tail = o;
    }
    head = o;
  }

  void lru_insert_tail(struct D3nChunkDataInfo* o) {
    lsubdout(g_ceph_context, rgw_datacache, 30) << "D3nDataCache: " << __func__ << "()" << dendl;
    o->lru_next = nullptr;
    o->lru_prev = tail;
    if (tail) {
      tail->lru_next = o;
    } else {
      head = o;
    }
    tail = o;
  }

  void lru_remove(struct D3nChunkDataInfo* o) {
    lsubdout(g_ceph_context, rgw_datacache, 30) << "D3nDataCache: " << __func__ << "()" << dendl;
    if (o->lru_next)
      o->lru_next->lru_prev = o->lru_prev;
    else
      tail = o->lru_prev;
    if (o->lru_prev)
      o->lru_prev->lru_next = o->lru_next;
    else
      head = o->lru_next;
    o->lru_next = o->lru_prev = nullptr;
  }
};


template <class T>
class D3nRGWDataCache : public T {

public:
  D3nRGWDataCache() {}

  int init_rados() override {
    int ret;
    ret = T::init_rados();
    if (ret < 0)
      return ret;

    return 0;
  }

  int get_obj_iterate_cb(const DoutPrefixProvider *dpp, const rgw_raw_obj& read_obj, off_t obj_ofs,
                         off_t read_ofs, off_t len, bool is_head_obj,
                         RGWObjState *astate, void *arg) override;
};

template<typename T>
int D3nRGWDataCache<T>::get_obj_iterate_cb(const DoutPrefixProvider *dpp, const rgw_raw_obj& read_obj, off_t obj_ofs,
                                 off_t read_ofs, off_t len, bool is_head_obj,
                                 RGWObjState *astate, void *arg) {
  lsubdout(g_ceph_context, rgw_datacache, 30) << "D3nDataCache::" << __func__ << "(): is head object : " << is_head_obj << dendl;
  librados::ObjectReadOperation op;
  struct get_obj_data* d = static_cast<struct get_obj_data*>(arg);
  std::string oid, key;

  //ldpp_dout(dpp,20) << "PORTING D4N: rgw_cache_list: " << g_conf()->rgw_cache_list << dendl;
  ldpp_dout(dpp,20) << "PORTING D4N: rgw_port: " << g_conf()->rgw_frontends << dendl;


  if (is_head_obj) {
    // only when reading from the head object do we need to do the atomic test
    int r = T::append_atomic_test(dpp, astate, op);
    if (r < 0)
      return r;

    if (astate &&
        obj_ofs < astate->data.length()) {
      unsigned chunk_len = std::min((uint64_t)astate->data.length() - obj_ofs, (uint64_t)len);

      r = d->client_cb->handle_data(astate->data, obj_ofs, chunk_len);
      if (r < 0)
        return r;

      len -= chunk_len;
      d->offset += chunk_len;
      read_ofs += chunk_len;
      obj_ofs += chunk_len;
      if (!len)
        return 0;
    }

    auto obj = d->rgwrados->svc.rados->obj(read_obj);
    r = obj.open(dpp);
    if (r < 0) {
      lsubdout(g_ceph_context, rgw, 4) << "failed to open rados context for " << read_obj << dendl;
      return r;
    }

    ldpp_dout(dpp, 20) << "D3nDataCache::" << __func__ << "(): oid=" << read_obj.oid << " obj-ofs=" << obj_ofs << " read_ofs=" << read_ofs << " len=" << len << dendl;
    op.read(read_ofs, len, nullptr, nullptr);

    const uint64_t cost = len;
    const uint64_t id = obj_ofs; // use logical object offset for sorting replies

    auto completed = d->aio->get(obj, rgw::Aio::librados_op(std::move(op), d->yield), cost, id);
    return d->flush(std::move(completed));
  } else {
    ldpp_dout(dpp, 20) << "D3nDataCache::" << __func__ << "(): oid=" << read_obj.oid << ", is_head_obj=" << is_head_obj << ", obj-ofs=" << obj_ofs << ", read_ofs=" << read_ofs << ", len=" << len << dendl;
    int r;

    op.read(read_ofs, len, nullptr, nullptr);

    const uint64_t cost = len;
    const uint64_t id = obj_ofs; // use logical object offset for sorting replies
    oid = read_obj.oid;

    auto obj = d->rgwrados->svc.rados->obj(read_obj);
    r = obj.open(dpp);
    if (r < 0) {
      lsubdout(g_ceph_context, rgw, 0) << "D3nDataCache: Error: failed to open rados context for " << read_obj << ", r=" << r << dendl;
      return r;
    }

    const bool is_compressed = (astate->attrset.find(RGW_ATTR_COMPRESSION) != astate->attrset.end());
    const bool is_encrypted = (astate->attrset.find(RGW_ATTR_CRYPT_MODE) != astate->attrset.end());
    if (read_ofs != 0 || astate->size != astate->accounted_size || is_compressed || is_encrypted) {
      d->d3n_bypass_cache_write = true;
      lsubdout(g_ceph_context, rgw, 5) << "D3nDataCache: " << __func__ << "(): Note - bypassing datacache: oid=" << read_obj.oid << ", read_ofs!=0 = " << read_ofs << ", size=" << astate->size << " != accounted_size=" << astate->accounted_size << ", is_compressed=" << is_compressed << ", is_encrypted=" << is_encrypted  << dendl;
      auto completed = d->aio->get(obj, rgw::Aio::librados_op(std::move(op), d->yield), cost, id);
      r = d->flush(std::move(completed));
      return r;
    }

    if (d->rgwrados->d3n_data_cache->get(oid, len)) {
      // Read From Cache
      ldpp_dout(dpp, 20) << "D3nDataCache: " << __func__ << "(): READ FROM CACHE: oid=" << read_obj.oid << ", obj-ofs=" << obj_ofs << ", read_ofs=" << read_ofs << ", len=" << len << dendl;
      auto completed = d->aio->get(obj, rgw::Aio::d3n_cache_op(dpp, d->yield, read_ofs, len, d->rgwrados->d3n_data_cache->cache_location), cost, id);
      r = d->flush(std::move(completed));
      if (r < 0) {
        lsubdout(g_ceph_context, rgw, 0) << "D3nDataCache: " << __func__ << "(): Error: failed to drain/flush, r= " << r << dendl;
      }
      return r;
    } else {
  /* Porting D4N */
   //Check the directory if it is in a remote cache
  ldpp_dout(dpp,20) << "PORTING D4N: the object key name is: " << astate -> obj.key.name << dendl;

  std::string port_num = g_conf()->rgw_frontends;
    if (port_num.find("8000") != std::string::npos) { //Hardcode to read to remote

      ldpp_dout(dpp,20) << "PORTING D4N: found the rgw with port 8000: " << port_num << dendl;

      std::string dest = "127.0.0.1:8001";
      std::string path = "bkt/" + astate -> obj.key.name;
      cache_block *c_block = new cache_block(); //Placeholder until function obtains value from arguments
      c_block->c_obj.obj_name = read_obj.oid;
      c_block->c_obj.accesskey.id = "key";
      c_block->c_obj.accesskey.key = "secret key";

      ldpp_dout(dpp,20) << "PORTING D4N: retreived the dest= " << dest << " of the remote rgw instance and the path=" << path << " of the object" << dendl;
      ldpp_dout(dpp,20) << "PORTING D4N: performing a remote get" << dendl;

      RemoteRequest *c =  new RemoteRequest();
      ldpp_dout(dpp,20) << "PORTING D4N: created a remote get, now calling a remote op." << dendl;
      auto completed = d->aio->get(obj, rgw::Aio::remote_op(dpp, std::move(op), d->yield,
                                                                  obj_ofs, read_ofs, len, dest, c,
                                                                  c_block, path, d->rgwrados->d3n_data_cache), cost, id);
      ldpp_dout(dpp,20) << "PORTING D4N: Returned from remote_op and completed=" << dendl;

      auto res =  d->flush(std::move(completed));
      ldpp_dout(dpp,20) << __func__   << "datacache HIT Error: failed to drain/flush" << res << dendl;
      return res;
      }

      //After fetching from remote cache, write it to local cache
      ldpp_dout(dpp,20) << "PORTING D4N: Data Fetched from remote Cache - writing to own Cache" << dendl;
      // Write To Cache
      ldpp_dout(dpp, 20) << "D3nDataCache: " << __func__ << "(): WRITE TO CACHE: oid=" << read_obj.oid << ", obj-ofs=" << obj_ofs << ", read_ofs=" << read_ofs << " len=" << len << dendl;
      auto completed = d->aio->get(obj, rgw::Aio::librados_op(std::move(op), d->yield), cost, id);
      ldpp_dout(dpp,20) << "PORTING D4N: Returned from writing to local read cache and completed=" << dendl;
      return d->flush(std::move(completed));
    }
  }
  lsubdout(g_ceph_context, rgw, 1) << "D3nDataCache: " << __func__ << "(): Warning: Check head object cache handling flow, oid=" << read_obj.oid << dendl;

  return 0;
}

class RemoteS3Request : public Task {
  public:
    RemoteS3Request(RemoteRequest *_req, CephContext *_cct) : Task(), req(_req), cct(_cct) {
      pthread_mutex_init(&qmtx,0);
      pthread_cond_init(&wcond, 0);
    }
    ~RemoteS3Request() {
      pthread_mutex_destroy(&qmtx);
      pthread_cond_destroy(&wcond);
    }
    virtual void run();
    virtual void set_handler(void *handle) {
      curl_handle = (CURL *)handle;
    }
    std::string sign_s3_request(std::string HTTP_Verb, std::string uri, std::string date, std::string YourSecretAccessKeyID, std::string AWSAccessKeyId);
    std::string get_date();
  private:
    int submit_http_get_request_s3();
  private:
    pthread_mutex_t qmtx;
    pthread_cond_t wcond;
    RemoteRequest *req;
    CephContext *cct;
    CURL *curl_handle;

};

#endif
