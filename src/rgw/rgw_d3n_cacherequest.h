// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#ifndef RGW_CACHEREQUEST_H
#define RGW_CACHEREQUEST_H

#include <fcntl.h>
#include <stdlib.h>
#include <aio.h>

#include "include/rados/librados.hpp"
#include "include/Context.h"
#include "common/async/completion.h"

#include <errno.h>
#include "common/error_code.h"
#include "common/errno.h"

#include "rgw_aio.h"
#include "rgw_cache.h"

class RGWRESTConn; //For porting



struct D3nGetObjData {
  std::mutex d3n_lock;
};

struct D3nL1CacheRequest {
  ~D3nL1CacheRequest() {
    lsubdout(g_ceph_context, rgw_datacache, 30) << "D3nDataCache: " << __func__ << "(): Read From Cache, complete" << dendl;
  }

  // unique_ptr with custom deleter for struct aiocb
  struct libaio_aiocb_deleter {
    void operator()(struct aiocb* c) {
      if(c->aio_fildes > 0) {
        if( ::close(c->aio_fildes) != 0) {
          lsubdout(g_ceph_context, rgw_datacache, 2) << "D3nDataCache: " << __func__ << "(): Error - can't close file, errno=" << -errno << dendl;
        }
      }
      delete c;
    }
  };

  using unique_aio_cb_ptr = std::unique_ptr<struct aiocb, libaio_aiocb_deleter>;

  struct AsyncFileReadOp {
    bufferlist result;
    unique_aio_cb_ptr aio_cb;
    using Signature = void(boost::system::error_code, bufferlist);
    using Completion = ceph::async::Completion<Signature, AsyncFileReadOp>;

    int init(const DoutPrefixProvider *dpp, const std::string& file_path, off_t read_ofs, off_t read_len, void* arg) {
      ldpp_dout(dpp, 20) << "D3nDataCache: " << __func__ << "(): file_path=" << file_path << dendl;
      aio_cb.reset(new struct aiocb);
      memset(aio_cb.get(), 0, sizeof(struct aiocb));
      aio_cb->aio_fildes = TEMP_FAILURE_RETRY(::open(file_path.c_str(), O_RDONLY|O_CLOEXEC|O_BINARY));
      if(aio_cb->aio_fildes < 0) {
        int err = errno;
        ldpp_dout(dpp, 1) << "ERROR: D3nDataCache: " << __func__ << "(): can't open " << file_path << " : " << cpp_strerror(err) << dendl;
        return -err;
      }
      if (g_conf()->rgw_d3n_l1_fadvise != POSIX_FADV_NORMAL)
        posix_fadvise(aio_cb->aio_fildes, 0, 0, g_conf()->rgw_d3n_l1_fadvise);

      bufferptr bp(read_len);
      aio_cb->aio_buf = bp.c_str();
      result.append(std::move(bp));

      aio_cb->aio_nbytes = read_len;
      aio_cb->aio_offset = read_ofs;
      aio_cb->aio_sigevent.sigev_notify = SIGEV_THREAD;
      aio_cb->aio_sigevent.sigev_notify_function = libaio_cb_aio_dispatch;
      aio_cb->aio_sigevent.sigev_notify_attributes = nullptr;
      aio_cb->aio_sigevent.sigev_value.sival_ptr = arg;

      return 0;
    }

    static void libaio_cb_aio_dispatch(sigval sigval) {
      lsubdout(g_ceph_context, rgw_datacache, 20) << "D3nDataCache: " << __func__ << "()" << dendl;
      auto p = std::unique_ptr<Completion>{static_cast<Completion*>(sigval.sival_ptr)};
      auto op = std::move(p->user_data);
      const int ret = -aio_error(op.aio_cb.get());
      boost::system::error_code ec;
      if (ret < 0) {
          ec.assign(-ret, boost::system::system_category());
      }

      ceph::async::dispatch(std::move(p), ec, std::move(op.result));
    }

    template <typename Executor1, typename CompletionHandler>
    static auto create(const Executor1& ex1, CompletionHandler&& handler) {
      auto p = Completion::create(ex1, std::move(handler));
      return p;
    }
  };

  template <typename ExecutionContext, typename CompletionToken>
  auto async_read(const DoutPrefixProvider *dpp, ExecutionContext& ctx, const std::string& file_path,
                  off_t read_ofs, off_t read_len, CompletionToken&& token) {
    using Op = AsyncFileReadOp;
    using Signature = typename Op::Signature;
    boost::asio::async_completion<CompletionToken, Signature> init(token);
    auto p = Op::create(ctx.get_executor(), init.completion_handler);
    auto& op = p->user_data;

    ldpp_dout(dpp, 20) << "D3nDataCache: " << __func__ << "(): file_path=" << file_path << dendl;
    int ret = op.init(dpp, file_path, read_ofs, read_len, p.get());
    if(0 == ret) {
      ret = ::aio_read(op.aio_cb.get());
    }
    ldpp_dout(dpp, 20) << "D3nDataCache: " << __func__ << "(): ::aio_read(), ret=" << ret << dendl;
    if(ret < 0) {
      auto ec = boost::system::error_code{-ret, boost::system::system_category()};
      ceph::async::post(std::move(p), ec, bufferlist{});
    } else {
      (void)p.release();
    }
    return init.result.get();
  }

  struct d3n_libaio_handler {
    rgw::Aio* throttle = nullptr;
    rgw::AioResult& r;
    // read callback
    void operator()(boost::system::error_code ec, bufferlist bl) const {
      r.result = -ec.value();
      r.data = std::move(bl);
      throttle->put(r);
    }
  };

  void file_aio_read_abstract(const DoutPrefixProvider *dpp, boost::asio::io_context& context, yield_context yield,
                              std::string& file_path, off_t read_ofs, off_t read_len,
                              rgw::Aio* aio, rgw::AioResult& r) {
    using namespace boost::asio;
    async_completion<yield_context, void()> init(yield);
    auto ex = get_associated_executor(init.completion_handler);

    auto& ref = r.obj.get_ref();
    ldpp_dout(dpp, 20) << "D3nDataCache: " << __func__ << "(): oid=" << ref.obj.oid << dendl;
    async_read(dpp, context, file_path+"/"+ref.obj.oid, read_ofs, read_len, bind_executor(ex, d3n_libaio_handler{aio, r}));
  }

};

//PORTING BEGINS -Daniel
class CacheRequest {
  public:

    ceph::mutex lock = ceph::make_mutex("CacheRequest");
    int sequence;
    int stat;
    bufferlist *bl=nullptr;
    off_t ofs;
    off_t read_ofs;
    off_t read_len;
    rgw::AioResult* r = nullptr;
    std::string key;
    rgw::Aio* aio = nullptr;
    librados::AioCompletion *lc;
    Context *onack;
    CacheRequest() :  sequence(0), stat(-1), bl(nullptr), ofs(0),  read_ofs(0), read_len(0), lc(nullptr){};
    virtual ~CacheRequest(){};
    virtual void release()=0;
    virtual void cancel_io()=0;
    virtual int status()=0;
    virtual void finish()=0;

};

typedef   void (*f)( RemoteRequest* func ); //Pointer to a function. Not sure what it does, but it won't work right now
struct RemoteRequest : public CacheRequest{
  std::string dest;
  void *tp;
  RGWRESTConn *conn; //Need to find and implement this class. -Daniel
  std::string path;
  std::string ak;
  std::string sk;
  //bufferlist pbl;// =nullptr;
  std::string s;
  size_t sizeleft;
  const char *readptr;
  f func; //Oh, is the typedef not working on line 149? -Daniel
  cache_block *c_block;
  RemoteRequest() :  CacheRequest(), c_block(nullptr) {}

  ~RemoteRequest(){}
  int prepare_op(std::string key,  bufferlist *bl, off_t read_len, off_t ofs, off_t read_ofs,
                std::string dest, rgw::Aio* aio, rgw::AioResult* r, cache_block *c_block,
                std::string path, void(*f)(RemoteRequest*));
 
  void release (){
//    lock.lock();
//    lock.unlock();
  }

  void cancel_io(){
    lock.lock();
    stat = ECANCELED;
    lock.unlock();
  }
 
  void finish(){
    lock.lock();
    bl->append(s.c_str(), s.size());
    s.clear();
    onack->complete(0);
    lock.unlock();
    delete this;
  }

  int status(){
    return 0;
  }

};
//PORTING ENDS




#endif
