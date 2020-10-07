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
#include "rgw_threadpool.h"
#include "rgw_cacherequest.h"


/*D3nDataCache*/
struct D3nDataCache;
class D3nL2CacheThreadPool;
class D3nHttpL2Request;

struct D3nChunkDataInfo : public LRUObject {
	CephContext *cct;
	uint64_t size;
	time_t access_time;
	string address;
	string oid;
	bool complete;
	struct D3nChunkDataInfo* lru_prev;
	struct D3nChunkDataInfo* lru_next;

	D3nChunkDataInfo(): size(0) {}

	void set_ctx(CephContext *_cct) {
		cct = _cct;
	}

	void dump(Formatter *f) const;
	static void generate_test_instances(list<D3nChunkDataInfo*>& o);
};

struct D3nCacheAioWriteRequest{
	string oid;
	void *data;
	int fd;
	struct aiocb *cb;
	D3nDataCache *priv_data;
	CephContext *cct;

	D3nCacheAioWriteRequest(CephContext *_cct) : cct(_cct) {}
	int create_io(bufferlist& bl, unsigned int len, string oid);

	void release() {
		::close(fd);
		cb->aio_buf = nullptr;
		free(data);
		data = nullptr;
		free(cb);
		free(this);
	}
};

struct D3nDataCache {

private:
  std::map<string, D3nChunkDataInfo*> cache_map;
  std::list<string> outstanding_write_list;
  int index;
  std::mutex lock;
  std::mutex cache_lock;
  std::mutex req_lock;
  std::mutex eviction_lock;

  CephContext *cct;
  enum _io_type {
    SYNC_IO = 1,
    ASYNC_IO = 2,
    SEND_FILE = 3
  } io_type;

  struct sigaction action;
  uint64_t free_data_cache_size = 0;
  uint64_t outstanding_write_size = 0;
  D3nL2CacheThreadPool *tp;
  struct D3nChunkDataInfo* head;
  struct D3nChunkDataInfo* tail;

private:
  void add_io();

public:
  D3nDataCache();
  ~D3nDataCache() {}

  bool get(const string& oid);
  void put(bufferlist& bl, unsigned int len, string& obj_key);
  int io_write(bufferlist& bl, unsigned int len, std::string oid);
  int create_aio_write_request(bufferlist& bl, unsigned int len, std::string oid);
  void cache_aio_write_completion_cb(D3nCacheAioWriteRequest* c);
  size_t random_eviction();
  size_t lru_eviction();
  std::string hash_uri(std::string dest);
  std::string deterministic_hash(std::string oid);
  void remote_io(struct L2CacheRequest* l2request);
  void init_l2_request_cb(librados::completion_t c, void *arg);
  void push_l2_request(L2CacheRequest* l2request);
  void l2_http_request(off_t ofs , off_t len, std::string oid);
  void init(CephContext *_cct) {
    cct = _cct;
    free_data_cache_size = cct->_conf->rgw_d3n_l1_datacache_size;
    head = nullptr;
    tail = nullptr;
  }

  void lru_insert_head(struct D3nChunkDataInfo* o) {
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

struct d3n_get_obj_data;

template <class T>
class D3nRGWDataCache : public T
{

  D3nDataCache data_cache;

public:
  D3nRGWDataCache() {}

  int init_rados() override {
    int ret;
    data_cache.init(T::cct);
    ret = T::init_rados();
    if (ret < 0)
      return ret;

    return 0;
  }

  int flush_read_list(struct d3n_get_obj_data* d);
  int get_obj_iterate_cb(const rgw_raw_obj& read_obj, off_t obj_ofs,
                         off_t read_ofs, off_t len, bool is_head_obj,
                         RGWObjState *astate, void *arg) override;
};


template<typename T>
int D3nRGWDataCache<T>::flush_read_list(struct d3n_get_obj_data* d) {

  d->data_lock.lock();
  std::list<bufferlist> l;
  l.swap(d->read_list);
  d->get();
  d->read_list.clear();
  d->data_lock.unlock();

  int r = 0;

  std::string oid;
  std::list<bufferlist>::iterator iter;
  for (iter = l.begin(); iter != l.end(); ++iter) {
    bufferlist& bl = *iter;
    oid = d->get_pending_oid();
    if(oid.empty()) {
      lsubdout(g_ceph_context, rgw, 0) << "ERROR: flush_read_list(): get_pending_oid() returned empty oid" << dendl;
      r = -ENOENT;
      break;
    }
    if (bl.length() == 0x400000)
      data_cache.put(bl, bl.length(), oid);
    r = d->client_cb->handle_data(bl, 0, bl.length());
    if (r < 0) {
      lsubdout(g_ceph_context, rgw, 0) << "ERROR: flush_read_list(): d->client_cb->handle_data() returned " << r << dendl;
      break;
    }
  }

  d->data_lock.lock();
  d->put();
  if (r < 0) {
    d->set_cancelled(r);
  }
  d->data_lock.unlock();
  return r;

}

template<typename T>
int D3nRGWDataCache<T>::get_obj_iterate_cb(const rgw_raw_obj& read_obj, off_t obj_ofs,
                                 off_t read_ofs, off_t len, bool is_head_obj,
                                 RGWObjState *astate, void *arg) {

  librados::ObjectReadOperation op;
  struct d3n_get_obj_data* d = static_cast<struct d3n_get_obj_data*>(arg);
  string oid, key;
  bufferlist *pbl;
  librados::AioCompletion *c;

  int r = 0;

  if (is_head_obj) {
    // only when reading from the head object do we need to do the atomic test
    r = T::append_atomic_test(astate, op);
    if (r < 0)
      return r;

    if (astate && obj_ofs < astate->data.length()) {
      unsigned chunk_len = std::min((uint64_t)astate->data.length() - obj_ofs, (uint64_t)len);

      r = d->client_cb->handle_data(astate->data, obj_ofs, chunk_len);
      if (r < 0)
        return r;

      d->lock.lock();
      d->total_read += chunk_len;
      d->lock.unlock();

      len -= chunk_len;
      read_ofs += chunk_len;
      obj_ofs += chunk_len;
      if (!len)
        return 0;
    }
  }

  d->throttle.get(len);
  if (d->is_cancelled()) {
    return d->get_err_code();
  }

  // add io after we check that we're not cancelled, otherwise we're going to have trouble
  // cleaning up
  d->add_io(obj_ofs, len, &pbl, &c);

  lsubdout(g_ceph_context, rgw, 20) << "rados->get_obj_iterate_cb oid=" << read_obj.oid << " obj-ofs=" << obj_ofs << " read_ofs=" << read_ofs << " len=" << len << dendl;
  op.read(read_ofs, len, pbl, NULL);

  librados::IoCtx io_ctx(d->io_ctx);
  io_ctx.locator_set_key(read_obj.loc);
  d->add_pending_oid(read_obj.oid);

  if (data_cache.get(read_obj.oid)) {
    L1CacheRequest* cc;
    d->add_l1_request(&cc, pbl, read_obj.oid, len, obj_ofs, read_ofs, key, c);
    r = io_ctx.cache_aio_notifier(read_obj.oid, static_cast<CacheRequest*>(cc));
    r = d->submit_l1_aio_read(cc);
    if (r != 0 ){
      lsubdout(g_ceph_context, rgw, 0) << "Error cache_aio_read failed err=" << r << dendl;
    }
  } else if (d->deterministic_hash_is_local(read_obj.oid)){
    lsubdout(g_ceph_context, rgw, 20) << "rados->get_obj_iterate_cb oid=" << read_obj.oid << " obj-ofs=" << obj_ofs << " read_ofs=" << read_ofs << " len=" << len << dendl;
    op.read(read_ofs, len, pbl, NULL);
    r = io_ctx.aio_operate(read_obj.oid, c, &op, NULL);
    lsubdout(g_ceph_context, rgw, 20) << "rados->aio_operate r=" << r << " bl.length=" << pbl->length() << dendl;
    if (r < 0) {
      lsubdout(g_ceph_context, rgw, 0) << "rados->aio_operate r=" << r << dendl;
      goto done_err;
    }
  }  else {
    L2CacheRequest* cc;
    d->add_l2_request(&cc, pbl, read_obj.oid, obj_ofs, read_ofs, len, key, c);
    r = io_ctx.cache_aio_notifier(read_obj.oid, static_cast<CacheRequest*>(cc));
    data_cache.push_l2_request(cc);
  }

  // Flush data to client if there is any
  r = flush_read_list(d);
  if (r < 0)
    return r;

  return 0;

done_err:
  lsubdout(g_ceph_context, rgw, 20) << "cancelling io r=" << r << " obj_ofs=" << obj_ofs << dendl;
  d->set_cancelled(r);
  d->cancel_io(obj_ofs);

  return r;
}

class D3nL2CacheThreadPool {
public:
  D3nL2CacheThreadPool(int n) {
    for (int i=0; i<n; ++i) {
      threads.push_back(new PoolWorkerThread(workQueue));
      threads.back()->start();
    }
  }

  ~D3nL2CacheThreadPool() {
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

class D3nHttpL2Request : public Task {
public:
  D3nHttpL2Request(L2CacheRequest* _req, CephContext* _cct) : Task(), req(_req), cct(_cct) {
    pthread_mutex_init(&qmtx, 0);
    pthread_cond_init(&wcond, 0);
  }
  ~D3nHttpL2Request() {
    pthread_mutex_destroy(&qmtx);
    pthread_cond_destroy(&wcond);
  }
  virtual void run();
  virtual void set_handler(void *handle) {
    curl_handle = (CURL *)handle;
  }
private:
  int submit_http_request();
  int sign_request(RGWAccessKey& key, RGWEnv& env, req_info& info);
private:
  pthread_mutex_t qmtx;
  pthread_cond_t wcond;
  L2CacheRequest* req;
  CURL *curl_handle;
  CephContext *cct;
};

#endif
