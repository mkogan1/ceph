// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#ifndef RGW_D3NCACHEREQUEST_H
#define RGW_D3NCACHEREQUEST_H

#include <stdlib.h>
#include <aio.h>

#include "include/rados/librados.hpp"
#include "include/Context.h"

class D3nCacheRequest {
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
    D3nCacheRequest(CephContext* _cct) : sequence(0), pbl(nullptr), op_data(nullptr), ofs(0), lc(nullptr), read_ofs(0), cct(_cct) {};
    virtual ~D3nCacheRequest(){};
    virtual void release()=0;
    virtual void cancel_io()=0;
    virtual int status()=0;
    virtual void finish()=0;
};

struct D3nL1CacheRequest : public D3nCacheRequest {
  int stat;
  struct aiocb* paiocb;
  D3nL1CacheRequest(CephContext* _cct) :  D3nCacheRequest(_cct), stat(-1), paiocb(NULL) {}
  ~D3nL1CacheRequest(){}
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

struct D3nL2CacheRequest : public D3nCacheRequest {
  size_t read;
  int stat;
  void *tp;
  string dest;
  D3nL2CacheRequest(CephContext* _cct) : D3nCacheRequest(_cct), read(0), stat(-1) {}
  ~D3nL2CacheRequest(){}
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
