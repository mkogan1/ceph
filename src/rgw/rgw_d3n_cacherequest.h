// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#ifndef RGW_CACHEREQUEST_H
#define RGW_CACHEREQUEST_H

#include <fcntl.h>
#include <stdlib.h>
#include <aio.h>

#include "common/Semaphore.h"
#include "include/rados/librados.hpp"
#include "include/Context.h"
#include "common/async/completion.h"

#include <errno.h>
#include "common/error_code.h"
#include "common/errno.h"

#include "rgw_aio.h"
#include "rgw_cache.h"


struct D3nGetObjData {
  std::mutex d3n_lock;
};

class D3nCacheRequest {
  public:
    virtual ~D3nCacheRequest() {};
    virtual void d3n_libaio_release()=0;
    virtual void d3n_libaio_cancel_io()=0;
    virtual void d3n_libaio_finish()=0;
    virtual int d3n_libaio_status()=0;
};

struct D3nL1CacheRequest : public D3nCacheRequest {
  ~D3nL1CacheRequest() {
    lsubdout(g_ceph_context, rgw_datacache, 30) << "D3nDataCache: " << __func__ << "(): Read From Cache, comlete" << dendl;
  }

  void d3n_libaio_release() {}
  void d3n_libaio_cancel_io() {};
  void d3n_libaio_finish() {};
  int d3n_libaio_status() { return 0; };


  // unique_ptr with custom deleter for struct aiocb
  struct libaio_aiocb_deleter {
    void operator()(struct aiocb* c) {
          if(c->aio_fildes > 0) ::close(c->aio_fildes);
          delete c;
    }
  };

  using unique_aio_cb_ptr = std::unique_ptr<struct aiocb, libaio_aiocb_deleter>;

  template <typename Result>
  struct AsyncFileReadOp {

    bufferlist result;
    unique_aio_cb_ptr aio_cb;
    using Signature = void(boost::system::error_code, Result);
    using Completion = ceph::async::Completion<Signature, AsyncFileReadOp<Result>>;

    int init(std::string file_path, off_t read_ofs, off_t read_len, void* arg) {
      aio_cb.reset(new struct aiocb);
      memset(aio_cb.get(), 0, sizeof(struct aiocb));
      aio_cb->aio_fildes = TEMP_FAILURE_RETRY(::open(file_path.c_str(), O_RDONLY|O_CLOEXEC|O_BINARY));
      if(aio_cb->aio_fildes < 0) {
        int err = errno;
        lsubdout(g_ceph_context, rgw, 2) << "IOC:AsyncFileReadOp can't open " << file_path << ": " << cpp_strerror(err) << dendl;
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
      aio_cb->aio_sigevent.sigev_notify_function = aio_dispatch;
      aio_cb->aio_sigevent.sigev_notify_attributes = nullptr;
      aio_cb->aio_sigevent.sigev_value.sival_ptr = arg;

      return 0;
    }

    static void aio_dispatch(sigval_t sigval) {
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
  auto async_read(ExecutionContext& ctx, const std::string& file_path,
                  off_t read_ofs, off_t read_len, CompletionToken&& token) {
    using Op = AsyncFileReadOp<bufferlist>;
    using Signature = typename Op::Signature;
    boost::asio::async_completion<CompletionToken, Signature> init(token);
    auto p = Op::create(ctx.get_executor(), init.completion_handler);
    auto& op = p->user_data;

    int ret = op.init(file_path, read_ofs, read_len, p.get());

    if(0 == ret) {
      ret = ::aio_read(op.aio_cb.get());
    }

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

  void file_aio_read_abstract(boost::asio::io_context& context, spawn::yield_context yield,
                              std::string& file_path, off_t read_ofs, off_t read_len,
                              rgw::Aio* aio, rgw::AioResult& r) {
    using namespace boost::asio;
    async_completion<spawn::yield_context, void()> init(yield);
    auto ex = get_associated_executor(init.completion_handler);

    auto& ref = r.obj.get_ref();
    async_read(context, file_path+"/"+ref.obj.oid, read_ofs, read_len, bind_executor(ex, d3n_libaio_handler{aio, r}));
  }

};

#endif
