// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2018 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#include <type_traits>
#include "include/rados/librados.hpp"
#include "librados/librados_asio.h"

#include "rgw_aio.h"
#include "rgw_d3n_cacherequest.h"
#include "rgw_d3n_datacache.h" //added for remote_op -Daniel

struct RemoteRequest; //PORTED

namespace rgw {

namespace {

void cb(librados::completion_t, void* arg);

struct state {
  Aio* aio;
  librados::AioCompletion* c;

  state(Aio* aio, AioResult& r)
    : aio(aio),
      c(librados::Rados::aio_create_completion(&r, &cb)) {}
};

void cb(librados::completion_t, void* arg) {
  static_assert(sizeof(AioResult::user_data) >= sizeof(state));
  static_assert(std::is_trivially_destructible_v<state>);
  auto& r = *(static_cast<AioResult*>(arg));
  auto s = reinterpret_cast<state*>(&r.user_data);
  r.result = s->c->get_return_value();
  s->c->release();
  s->aio->put(r);
}

template <typename Op>
Aio::OpFunc aio_abstract(Op&& op) {
  return [op = std::move(op)] (Aio* aio, AioResult& r) mutable {
      constexpr bool read = std::is_same_v<std::decay_t<Op>, librados::ObjectReadOperation>;
      auto s = new (&r.user_data) state(aio, r);
      if constexpr (read) {
        r.result = r.obj.aio_operate(s->c, &op, &r.data);
      } else {
        r.result = r.obj.aio_operate(s->c, &op);
      }
      if (r.result < 0) {
        s->c->release();
        aio->put(r);
      }
    };
}

struct Handler {
  Aio* throttle = nullptr;
  AioResult& r;
  // write callback
  void operator()(boost::system::error_code ec) const {
    r.result = -ec.value();
    throttle->put(r);
  }
  // read callback
  void operator()(boost::system::error_code ec, bufferlist bl) const {
    r.result = -ec.value();
    r.data = std::move(bl);
    throttle->put(r);
  }
};

template <typename Op>
Aio::OpFunc aio_abstract(Op&& op, boost::asio::io_context& context,
                         yield_context yield) {
  return [op = std::move(op), &context, yield] (Aio* aio, AioResult& r) mutable {
      // arrange for the completion Handler to run on the yield_context's strand
      // executor so it can safely call back into Aio without locking
      using namespace boost::asio;
      async_completion<yield_context, void()> init(yield);
      auto ex = get_associated_executor(init.completion_handler);

      auto& ref = r.obj.get_ref();
      librados::async_operate(context, ref.pool.ioctx(), ref.obj.oid, &op, 0,
                              bind_executor(ex, Handler{aio, r}));
    };
}


Aio::OpFunc d3n_cache_aio_abstract(const DoutPrefixProvider *dpp, optional_yield y, off_t read_ofs, off_t read_len, std::string& location) {
  return [dpp, y, read_ofs, read_len, location] (Aio* aio, AioResult& r) mutable {
    // d3n data cache requires yield context (rgw_beast_enable_async=true)
    ceph_assert(y);
    auto& ref = r.obj.get_ref();
    auto c = std::make_unique<D3nL1CacheRequest>();
    lsubdout(g_ceph_context, rgw_datacache, 20) << "D3nDataCache: d3n_cache_aio_abstract(): libaio Read From Cache, oid=" << ref.obj.oid << dendl;
    c->file_aio_read_abstract(dpp, y.get_io_context(), y.get_yield_context(), location, read_ofs, read_len, aio, r);
  };
}

//PORTING -Daniel
void remote_aio_cb(RemoteRequest *c){ //all below commented out by original designers

/*      lsubdout(g_ceph_context, rgw, 1) << "D3nDataCache: " << __func__ << "(): Read From Cache " << c->r->id <<" key " << c->key  << dendl;
        auto& r = *(c->r);
        auto s = reinterpret_cast<remote_state*>(&r.user_data);
  //      c->r->result = 0;
//s->aio->put(*(c->r));
        lsubdout(g_ceph_context, rgw, 1) << "D3nDataCache: " << __func__ << "(): Read From Cache s " << r.id <<" key " << c->key  << dendl;*/
       c->finish();

}
template <typename Op>
Aio::OpFunc remote_aio_abstract(const DoutPrefixProvider *dpp, Op&& op, off_t obj_ofs, off_t read_ofs, off_t read_len, //REMOTE_AIO_ABSTRACT 1
                                std::string dest,  RemoteRequest *c, boost::asio::io_context& context,
                                spawn::yield_context yield, cache_block *c_block, std::string path, D3nDataCache *dc) {
  ldpp_dout(dpp,20) << "PORTING D4N: Beginning remote_aio_abstract TYPE 1 before move(op)" << dendl;
  ldpp_dout(dpp,20) << "PORTING D4N: Beginning remote_aio_abstract TYPE 1 before move(op) path=" << path << dendl;
  ldpp_dout(dpp,20) << "PORTING D4N: Beginning remote_aio_abstract TYPE 1 before move(op) dest=" << dest << dendl;


  return [op = std::move(op), obj_ofs, read_ofs, read_len, dest, c, &context, yield, c_block, path, dc] (Aio* aio, AioResult& r) mutable{

      /*          auto& ref = r.obj.get_ref(); //This stuff was commented out by the original designers -Daniel
    auto cs = new(&r.user_data) remote_state(aio, r);
    cs->c = new RemoteRequest();
    cs->c->prepare_op(ref.obj.oid, &r.data, read_len, obj_ofs, read_ofs, dest, aio, &r, c_block, path, remote_aio_cb);
    dc->submit_remote_req(cs->c);

*/
   //ldpp_dout(dpp,20) << "PORTING D4N: Beginning remote_aio_abstract TYPE 1" << dendl;

  using namespace boost::asio;
  async_completion<spawn::yield_context, void()> init(yield);
  auto ex = get_associated_executor(init.completion_handler);
  auto& ref = r.obj.get_ref();
  RemoteRequest* cc = new RemoteRequest();

  //ldpp_dout(dpp,20) << "PORTING D4N: associated_executor created, as well as a new RemoteRequest" << dendl;

  cc->prepare_op(ref.obj.oid, &r.data, read_len, obj_ofs, read_ofs, dest, aio, &r, c_block, path, remote_aio_cb);

  librados::async_operate(context, ref.pool.ioctx(), ref.obj.oid, &op, cc,
                          bind_executor(ex, Handler{aio, r}));
   //DataCache *ab = static_cast<DataCache *>(dc); 
   dc->submit_remote_req(cc);

   //ldpp_dout(dpp,20) << "PORTING D4N: Submit_remote_req has been called and returned" << dendl;
  };
  ldpp_dout(dpp,20) << "PORTING D4N: Submit_remote_req has been called and returned" << dendl;
}


template <typename Op>
Aio::OpFunc remote_aio_abstract(const DoutPrefixProvider *dpp, Op&& op, optional_yield y, off_t obj_ofs, off_t read_ofs, //REMOTE_AIO_ABSTRACT 2
                                off_t read_len, std::string dest,  RemoteRequest *c, cache_block *c_block,
                                std::string path, D3nDataCache *dc) {
   ldpp_dout(dpp,20) << "PORTING D4N: Beginning remote_aio_abstract TYPE 2" << dendl;
  static_assert(std::is_base_of_v<librados::ObjectOperation, std::decay_t<Op>>);
  static_assert(!std::is_lvalue_reference_v<Op>);
  static_assert(!std::is_const_v<Op>);
 // #ifdef HAVE_BOOST_CONTEXT //Kriegers going to hate me for this but I want to see what happens if we force this
  //ANS: it breaks
   ldpp_dout(dpp,20) << "PORTING D4N: End of remote_aio_abstract TYPE 2 - HAVE_BOOST_CONTEXT" << dendl;
  return remote_aio_abstract(dpp, std::forward<Op>(op),obj_ofs, read_ofs, read_len, dest, c,  y.get_io_context(), y.get_yield_context(),c_block , path, dc);
  //#endif

 //return 0; //temporary
}
//PORTING ENDS




template <typename Op>
Aio::OpFunc aio_abstract(Op&& op, optional_yield y) {
  static_assert(std::is_base_of_v<librados::ObjectOperation, std::decay_t<Op>>);
  static_assert(!std::is_lvalue_reference_v<Op>);
  static_assert(!std::is_const_v<Op>);
  if (y) {
    return aio_abstract(std::forward<Op>(op), y.get_io_context(),
                        y.get_yield_context());
  }
  return aio_abstract(std::forward<Op>(op));
}

} // anonymous namespace

Aio::OpFunc Aio::librados_op(librados::ObjectReadOperation&& op,
                             optional_yield y) {
  return aio_abstract(std::move(op), y);
}
Aio::OpFunc Aio::librados_op(librados::ObjectWriteOperation&& op,
                             optional_yield y) {
  return aio_abstract(std::move(op), y);
}

Aio::OpFunc Aio::d3n_cache_op(const DoutPrefixProvider *dpp, optional_yield y,
                              off_t read_ofs, off_t read_len, std::string& location) {
  return d3n_cache_aio_abstract(dpp, y, read_ofs, read_len, location);
}

//PORTING -Daniel
//There's just the cache op and the abstract op in D4N, I'm going to see if I can focus this on grabbing backend
//disc stuff first.
Aio::OpFunc Aio::remote_op(const DoutPrefixProvider *dpp, librados::ObjectReadOperation&& op, /*boost::asio::io_context& context,*/ optional_yield y,
                              off_t obj_ofs, off_t read_ofs, off_t read_len, std::string dest,
                              RemoteRequest *c, cache_block *c_block, std::string path, D3nDataCache *dc) {
                                ldpp_dout(dpp,20) << "PORTING D4N: Inside a Remote_op, calling an remote_aio_abstract" << dendl;
    return remote_aio_abstract(dpp, std::move(op), y, obj_ofs, read_ofs, read_len, dest, c, c_block, path, dc);
}
//PORTING ENDS
/*
Op&& op, optional_yield y, off_t obj_ofs, off_t read_ofs, off_t read_len, string dest,
RemoteRequest *c, cache_block *c_block, string path, DataCache *dc)
*/


} // namespace rgw
