// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RGW_DATALOG_H
#define CEPH_RGW_DATALOG_H

#include <atomic>
#include <cstdint>
#include <list>
#include <map>
#include <memory>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

#include <boost/container/flat_map.hpp>
#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>

#undef FMT_HEADER_ONLY
#define FMT_HEADER_ONLY 1
#include <fmt/format.h>

#include "include/rados/librados.hpp"

#include "include/buffer.h"
#include "include/encoding.h"
#include "include/function2.hpp"

#include "common/ceph_context.h"
#include "common/ceph_json.h"
#include "common/ceph_mutex.h"
#include "common/ceph_time.h"
#include "common/Formatter.h"
#include "common/lru_map.h"
#include "common/RefCountedObj.h"

#include "cls/log/cls_log_types.h"

#include "rgw/rgw_basic_types.h"
#include "rgw/rgw_log_backing.h"
#include "rgw/rgw_rados.h"
#include "rgw/rgw_zone.h"

namespace bc = boost::container;

enum DataLogEntityType {
  ENTITY_TYPE_UNKNOWN = 0,
  ENTITY_TYPE_BUCKET = 1,
};

struct rgw_data_change {
  DataLogEntityType entity_type;
  std::string key;
  ceph::real_time timestamp;

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    auto t = std::uint8_t(entity_type);
    encode(t, bl);
    encode(key, bl);
    encode(timestamp, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
     DECODE_START(1, bl);
     std::uint8_t t;
     decode(t, bl);
     entity_type = DataLogEntityType(t);
     decode(key, bl);
     decode(timestamp, bl);
     DECODE_FINISH(bl);
  }

  void dump(ceph::Formatter* f) const;
  void decode_json(JSONObj* obj);
};
WRITE_CLASS_ENCODER(rgw_data_change)

struct rgw_data_change_log_entry {
  std::string log_id;
  ceph::real_time log_timestamp;
  rgw_data_change entry;

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(log_id, bl);
    encode(log_timestamp, bl);
    encode(entry, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
     DECODE_START(1, bl);
     decode(log_id, bl);
     decode(log_timestamp, bl);
     decode(entry, bl);
     DECODE_FINISH(bl);
  }

  void dump(ceph::Formatter* f) const;
  void decode_json(JSONObj* obj);
};
WRITE_CLASS_ENCODER(rgw_data_change_log_entry)

struct RGWDataChangesLogInfo {
  std::string marker;
  ceph::real_time last_update;

  void dump(ceph::Formatter* f) const;
  void decode_json(JSONObj* obj);
};

class RGWDataChangesBE;

class DataLogBackends final
  : public logback_generations,
    private bc::flat_map<uint64_t, boost::intrusive_ptr<RGWDataChangesBE>> {
  friend class logback_generations;
  friend class GenTrim;

  std::mutex m;
  RGWDataChangesLog& datalog;

  DataLogBackends(librados::IoCtx& ioctx,
		  std::string oid,
		  fu2::unique_function<std::string(
		    uint64_t, int) const>&& get_oid,
		  int shards, RGWDataChangesLog& datalog) noexcept
    : logback_generations(ioctx, oid, std::move(get_oid),
			  shards), datalog(datalog) {}
public:

  boost::intrusive_ptr<RGWDataChangesBE> head() {
    std::unique_lock l(m);
    auto i = end();
    --i;
    return i->second;
  }
  int list(int shard, int max_entries,
	   std::vector<rgw_data_change_log_entry>& entries,
	   std::optional<std::string_view> marker,
	   std::string* out_marker, bool* truncated);
  int trim_entries(int shard_id, std::string_view marker);
  void trim_entries(int shard_id, std::string_view marker,
		    librados::AioCompletion* c);
  void set_zero(RGWDataChangesBE* be) {
    emplace(0, be);
  }

  int handle_init(entries_t e) noexcept override;
  int handle_new_gens(entries_t e) noexcept override;
  int handle_empty_to(uint64_t new_tail) noexcept override;
};

class RGWDataChangesLog {
  friend DataLogBackends;
  CephContext* cct;
  RGWRados* store;
  librados::IoCtx ioctx;
  rgw::BucketChangeObserver* observer = nullptr;
  std::unique_ptr<DataLogBackends> bes;

  const int num_shards;
  std::string get_prefix() {
    auto prefix = cct->_conf->rgw_data_log_obj_prefix;
    return prefix.empty() ? prefix : "data_log"s;
  }
  std::string metadata_log_oid() {
    return get_prefix() + "generations_metadata"s;
  }
  std::string prefix;

  ceph::mutex lock = ceph::make_mutex("RGWDataChangesLog::lock");
  ceph::shared_mutex modified_lock =
    ceph::make_shared_mutex("RGWDataChangesLog::modified_lock");
  bc::flat_map<int, bc::flat_set<std::string>> modified_shards;

  std::atomic<bool> down_flag = { false };

  struct ChangeStatus {
    ceph::real_time cur_expiration;
    ceph::real_time cur_sent;
    bool pending = false;
    RefCountedCond* cond = nullptr;
    ceph::mutex lock = ceph::make_mutex("RGWDataChangesLog::ChangeStatus");
  };

  using ChangeStatusPtr = std::shared_ptr<ChangeStatus>;

  lru_map<rgw_bucket_shard, ChangeStatusPtr> changes;

  bc::flat_set<rgw_bucket_shard> cur_cycle;

  void _get_change(const rgw_bucket_shard& bs, ChangeStatusPtr& status);
  void register_renew(const rgw_bucket_shard& bs);
  void update_renewed(const rgw_bucket_shard& bs, ceph::real_time expiration);
  bool going_down() const;
  int choose_oid(const rgw_bucket_shard& bs);

  void renew_run();
  void renew_stop();
  int renew_entries();

  ceph::mutex renew_lock = ceph::make_mutex("ChangesRenewThread::lock");
  ceph::condition_variable renew_cond;
  std::thread renew_thread;

public:

  RGWDataChangesLog(CephContext* cct, RGWRados* store);
  ~RGWDataChangesLog();

  int init();
  int add_entry(const rgw_bucket& bucket_info, int shard_id);
  int get_log_shard_id(rgw_bucket& bucket, int shard_id);
  int list_entries(int shard, int max_entries,
		   std::vector<rgw_data_change_log_entry>& entries,
		   std::optional<std::string_view> marker,
		   std::string* out_marker, bool* truncated);
  int trim_entries(int shard_id, std::string_view marker);
  int trim_entries(int shard_id, std::string_view marker,
		   librados::AioCompletion* c);
  int get_info(int shard_id, RGWDataChangesLogInfo* info);
  int lock_exclusive(int shard_id, timespan duration, string& zone_id, string& owner_id);
  int unlock(int shard_id, string& zone_id, string& owner_id);
  struct LogMarker {
    int shard = 0;
    std::optional<std::string> marker;

    LogMarker() = default;
  };

  int list_entries(int max_entries,
		   std::vector<rgw_data_change_log_entry>& entries,
		   LogMarker& marker, bool* ptruncated);
  void mark_modified(int shard_id, const rgw_bucket_shard& bs);
  auto read_clear_modified() {
    std::unique_lock wl{modified_lock};
    decltype(modified_shards) modified;
    modified.swap(modified_shards);
    modified_shards.clear();
    return modified;
  }

  void set_observer(rgw::BucketChangeObserver* observer) {
    this->observer = observer;
  }
  // a marker that compares greater than any other
  std::string max_marker() const;
  std::string get_oid(uint64_t gen_id, int shard_id) const;
};

class RGWDataChangesBE : public boost::intrusive_ref_counter<RGWDataChangesBE> {
protected:
  librados::IoCtx& ioctx;
  CephContext* const cct;
  RGWDataChangesLog& datalog;

  std::string get_oid(int shard_id) {
    return datalog.get_oid(gen_id, shard_id);
  }
public:
  using entries = std::variant<std::list<cls_log_entry>,
			       std::vector<ceph::buffer::list>>;

  const uint64_t gen_id;

  RGWDataChangesBE(librados::IoCtx& ioctx,
		   RGWDataChangesLog& datalog,
		   uint64_t gen_id)
    : ioctx(ioctx), cct(static_cast<CephContext*>(ioctx.cct())),
      datalog(datalog), gen_id(gen_id) {}
  virtual ~RGWDataChangesBE() = default;

  virtual void prepare(ceph::real_time now,
		       const std::string& key,
		       ceph::buffer::list&& entry,
		       entries& out) = 0;
  virtual int push(int index, entries&& items) = 0;
  virtual int push(int index, ceph::real_time now,
		   const std::string& key,
		   ceph::buffer::list&& bl) = 0;
  virtual int list(int shard, int max_entries,
		   std::vector<rgw_data_change_log_entry>& entries,
		   std::optional<std::string_view> marker,
		   std::string* out_marker, bool* truncated) = 0;
  virtual int get_info(int index, RGWDataChangesLogInfo *info) = 0;
  virtual int trim(int index, std::string_view marker) = 0;
  virtual int trim(int index, std::string_view marker,
		   librados::AioCompletion* c) = 0;
  virtual std::string_view max_marker() const = 0;
};


#endif
