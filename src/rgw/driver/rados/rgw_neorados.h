// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include <boost/asio/async_result.hpp>
#include <boost/asio/deferred.hpp>

#include <boost/asio/experimental/co_composed.hpp>

#include "include/neorados/RADOS.hpp"

#include "include/stringify.h"

#include "common/dout.h"

#include "osd/osd_types.h"

#include "rgw_pool_types.h"


namespace rgw {
template<boost::asio::completion_token_for<void(boost::system::error_code)>
	 CompletionToken>
auto set_mostly_omap(const DoutPrefixProvider* dpp, neorados::RADOS& rados,
		     std::string name, CompletionToken&& token) {
  namespace asio = boost::asio;
  namespace sys = boost::system;
  using neorados::RADOS;
  return asio::async_initiate<CompletionToken, void(sys::error_code)>
    (asio::experimental::co_composed<void(sys::error_code)>
     ([](auto state, const DoutPrefixProvider* dpp, RADOS& rados,
	 std::string name) -> void {
       try {
	 state.throw_if_cancelled(true);
	 state.reset_cancellation_state(asio::enable_terminal_cancellation());
	 // set pg_autoscale_bias
	 auto bias = rados.cct()->_conf.get_val<double>(
	   "rgw_rados_pool_autoscale_bias");
	 std::vector<std::string> biascommand{
	   {"{\"prefix\": \"osd pool set\", \"pool\": \"" +
	    name + "\", \"var\": \"pg_autoscale_bias\", \"val\": \"" +
	    stringify(bias) + "\"}"}
	 };
	 co_await rados.mon_command(std::move(biascommand), {}, nullptr, nullptr,
				    asio::deferred);
	 // set recovery_priority
	 auto p = rados.cct()->_conf.get_val<uint64_t>(
	   "rgw_rados_pool_recovery_priority");
	 std::vector<std::string> recoverycommand{
	   {"{\"prefix\": \"osd pool set\", \"pool\": \"" +
	    name + "\", \"var\": \"recovery_priority\": \"" +
	    stringify(p) + "\"}"}
	 };
	 co_await rados.mon_command(
	   std::move(recoverycommand),
	   {}, nullptr, nullptr, asio::deferred);
       } catch (const sys::system_error& e) {
	 ldpp_dout(dpp, 10) << "rgw::set_mostly_omap: failed with error "
			    << e.what() << dendl;
	 co_return e.code();
       }
       co_return sys::error_code{};
     }, rados.get_executor()),
     token, dpp, std::ref(rados), std::move(name));
}

template<boost::asio::completion_token_for<void(boost::system::error_code)>
	 CompletionToken>
auto create_pool(const DoutPrefixProvider* dpp,
		 neorados::RADOS& rados,
		 std::string name, bool mostly_omap,
		 CompletionToken&& token) {
  namespace asio = boost::asio;
  namespace sys = boost::system;
  using neorados::RADOS;
  return asio::async_initiate<CompletionToken, void(sys::error_code)>
    (asio::experimental::co_composed<void(sys::error_code)>
     ([](auto state, const DoutPrefixProvider* dpp, RADOS& rados,
	 std::string name, bool mostly_omap) -> void {
       try {
	 state.throw_if_cancelled(true);
	 state.reset_cancellation_state(asio::enable_terminal_cancellation());
	 try {
	   co_await rados.create_pool(name, std::nullopt, asio::deferred);
	 } catch (const sys::system_error& e) {
	   if (e.code() == sys::errc::result_out_of_range) {
	     ldpp_dout(dpp, 0)
	       << "init_iocontext: ERROR: RADOS::create_pool failed with "
	       << e.what()
	       << " (this can be due to a pool or placement group "
	       << "misconfiguration, e.g. pg_num < pgp_num or "
	       << "mon_max_pg_per_osd exceeded)" << dendl;
	   } else if (e.code() == sys::errc::file_exists) {
	     co_return sys::error_code{};
	   } else {
	     throw;
	   }
	 }
	 try {
	   co_await
	     rados.enable_application(name,
				      pg_pool_t::APPLICATION_NAME_RGW,
				      false, asio::deferred);
	 } catch (const sys::system_error& e) {
	   if (e.code() != sys::errc::operation_not_supported) {
	     throw;
	   }
	 }
	 if (mostly_omap) {
	   co_await set_mostly_omap(dpp, rados, std::move(name), asio::deferred);
	 }
       } catch (const sys::system_error& e) {
	 ldpp_dout(dpp, 10) << "rgw::create_pool: failed with error "
			    << e.what() << dendl;
	 co_return e.code();
       }
     }, rados.get_executor()),
     token, dpp, std::ref(rados), std::move(name), mostly_omap);
}

template<boost::asio::completion_token_for<
	   void(boost::system::error_code, neorados::IOContext)>
	 CompletionToken>
auto init_iocontext(const DoutPrefixProvider* dpp,
		    neorados::RADOS& rados,
		    rgw_pool pool,
		    bool create, bool mostly_omap,
		    CompletionToken&& token) {
  namespace asio = boost::asio;
  namespace sys = boost::system;
  using neorados::IOContext;
  using neorados::RADOS;
  return asio::async_initiate<CompletionToken,
			      void(sys::error_code, IOContext)>
    (asio::experimental::co_composed<void(sys::error_code, IOContext)>
       ([](auto state, const DoutPrefixProvider* dpp, RADOS& rados,
	   rgw_pool pool, bool create, bool mostly_omap) -> void {
	 try {
	   state.throw_if_cancelled(true);
	   state.reset_cancellation_state(asio::enable_terminal_cancellation());
	   IOContext loc;
	   // There doesn't seem to be a way to redirect the error
	   // code from asio::deferred?
	   try {
	     loc.pool(co_await rados.lookup_pool(pool.name, asio::deferred));
	   } catch (const sys::system_error& e) {
	     if (e.code() == sys::errc::no_such_file_or_directory && create) {
	       loc.pool(-1);
	     } else {
	       throw;
	     }
	   }

	   if (loc.pool() == -1) {
	     // We have to create it ourselves!
	     co_await create_pool(dpp, rados, pool.name, mostly_omap,
				  asio::deferred);
	     loc.pool(co_await rados.lookup_pool(pool.name, asio::deferred));
	   }

	   if (!pool.ns.empty()) {
	     loc.ns(pool.ns);
	   }
	   co_return {sys::error_code{}, loc};
	 } catch (const sys::system_error& e) {
	   ldpp_dout(dpp, 10) << "init_iocontext: failed with error "
			      << e.what() << dendl;
	   co_return {e.code(), IOContext{}};
	 }
       }, rados.get_executor()),
     token, dpp, std::ref(rados), std::move(pool), create, mostly_omap);
}
}
