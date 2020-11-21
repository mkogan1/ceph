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

class RGWDataChangesLog;

class RGWDataChangesBE {
protected:
  librados::IoCtx& ioctx;
  CephContext* const cct;
  RGWDataChangesLog& datalog;
private:
public:
  using entries = std::variant<std::list<cls_log_entry>,
			       std::vector<ceph::buffer::list>>;

  RGWDataChangesBE(librados::IoCtx& ioctx,
		   RGWDataChangesLog& datalog)
    : ioctx(ioctx), cct(static_cast<CephContext*>(ioctx.cct())),
      datalog(datalog) {}
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

class RGWDataChangesLog {
  CephContext* cct;
  RGWRados* store;
  librados::IoCtx ioctx;
  rgw::BucketChangeObserver* observer = nullptr;
  std::unique_ptr<RGWDataChangesBE> be;

  const int num_shards;
  std::string get_prefix() {
    auto prefix = cct->_conf->rgw_data_log_obj_prefix;
    return prefix.empty() ? prefix : "data_log"s;
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
  std::string get_oid(int i) const;
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
  std::string_view max_marker() const;
};

#endif
