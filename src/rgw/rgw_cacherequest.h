// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#ifndef RGW_CACHEREQUEST_H
#define RGW_CACHEREQUEST_H

#include <stdlib.h>
#include <aio.h>

#include "include/rados/librados.hpp"
#include "include/Context.h"

class CacheRequest {
  public:
    std::mutex lock;
    int sequence;
    buffer::list* pbl;
    struct d3n_get_obj_data* op_data;
    std::string oid;
    off_t ofs;
    off_t len;
    librados::AioCompletion* lc;
    std::string key;
    off_t read_ofs;
    Context *onack;
    CephContext* cct;
    CacheRequest(CephContext* _cct) : sequence(0), pbl(nullptr), op_data(nullptr), ofs(0), lc(nullptr), read_ofs(0), cct(_cct) {};
    virtual ~CacheRequest(){};
    virtual void release()=0;
    virtual void cancel_io()=0;
    virtual int status()=0;
    virtual void finish()=0;
};

struct L1CacheRequest : public CacheRequest {
  int stat;
  struct aiocb* paiocb;
  L1CacheRequest(CephContext* _cct) :  CacheRequest(_cct), stat(-1), paiocb(NULL) {}
  ~L1CacheRequest(){}
  void release (){
    lock.lock();
    free((void*)paiocb->aio_buf);
    paiocb->aio_buf = nullptr;
    ::close(paiocb->aio_fildes);
    delete(paiocb);
    lock.unlock();
    delete this;
	}

  void cancel_io(){
    lock.lock();
    stat = ECANCELED;
    lock.unlock();
  }

  int status(){
    lock.lock();
    if (stat != EINPROGRESS) {
      lock.unlock();
      if (stat == ECANCELED){
        release();
        return ECANCELED;
      }
    }
    stat = aio_error(paiocb);
    lock.unlock();
    return stat;
  }

  void finish(){
    pbl->append((char*)paiocb->aio_buf, paiocb->aio_nbytes);
    onack->complete(0);
    release();
  }
};

struct L2CacheRequest : public CacheRequest {
  size_t read;
  int stat;
  void *tp;
  string dest;
  L2CacheRequest(CephContext* _cct) : CacheRequest(_cct), read(0), stat(-1) {}
  ~L2CacheRequest(){}
  void release (){
    lock.lock();
    lock.unlock();
  }

  void cancel_io(){
    lock.lock();
    stat = ECANCELED;
    lock.unlock();
  }

  void finish(){
    onack->complete(0);
    release();
  }

  int status(){
    return 0;
  }
};

#endif
