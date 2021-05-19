// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#undef FMT_HEADER_ONLY
#define FMT_HEADER_ONLY 1
#include "fmt/format.h"

#include "common/errno.h"

#include "librados/AioCompletionImpl.h"

#include "rgw/rgw_datalog.h"
#include "rgw/rgw_log_backing.h"
#include "rgw/rgw_tools.h"
#include "rgw/rgw_zone.h"
#include "rgw/cls_fifo_legacy.h"
#include "cls/fifo/cls_fifo_types.h"
#include "cls/log/cls_log_client.h"

#define dout_context g_ceph_context
static constexpr auto dout_subsys = ceph_subsys_rgw;

namespace lr = librados;

void rgw_data_change::dump(ceph::Formatter* f) const
{
  std::string type;
  switch (entity_type) {
    case ENTITY_TYPE_BUCKET:
      type = "bucket";
      break;
    default:
      type = "unknown";
  }
  encode_json("entity_type", type, f);
  encode_json("key", key, f);
  utime_t ut(timestamp);
  encode_json("timestamp", ut, f);
}

void rgw_data_change::decode_json(JSONObj* obj) {
  std::string s;
  JSONDecoder::decode_json("entity_type", s, obj);
  if (s == "bucket") {
    entity_type = ENTITY_TYPE_BUCKET;
  } else {
    entity_type = ENTITY_TYPE_UNKNOWN;
  }
  JSONDecoder::decode_json("key", key, obj);
  utime_t ut;
  JSONDecoder::decode_json("timestamp", ut, obj);
  timestamp = ut.to_real_time();
}

void rgw_data_change_log_entry::dump(ceph::Formatter* f) const
{
  encode_json("log_id", log_id, f);
  utime_t ut(log_timestamp);
  encode_json("log_timestamp", ut, f);
  encode_json("entry", entry, f);
}

void rgw_data_change_log_entry::decode_json(JSONObj* obj) {
  JSONDecoder::decode_json("log_id", log_id, obj);
  utime_t ut;
  JSONDecoder::decode_json("log_timestamp", ut, obj);
  log_timestamp = ut.to_real_time();
  JSONDecoder::decode_json("entry", entry, obj);
}

class RGWDataChangesOmap final : public RGWDataChangesBE {
  using centries = std::list<cls_log_entry>;
  std::vector<std::string> oids;

public:
  RGWDataChangesOmap(lr::IoCtx& ioctx,
		     RGWDataChangesLog& datalog,
		     uint64_t gen_id,
		     int num_shards)
    : RGWDataChangesBE(ioctx, datalog, gen_id) {
    oids.reserve(num_shards);
    for (auto i = 0; i < num_shards; ++i) {
      oids.push_back(get_oid(i));
    }
  }
  ~RGWDataChangesOmap() override = default;
  void prepare(ceph::real_time ut, const std::string& key,
	       ceph::buffer::list&& entry, entries& out) override {
    if (!std::holds_alternative<centries>(out)) {
      ceph_assert(std::visit([](const auto& v) { return std::empty(v); }, out));
      out = centries();
    }

    cls_log_entry e;
    cls_log_add_prepare_entry(e, utime_t(ut), {}, key, entry);
    std::get<centries>(out).push_back(std::move(e));
  }
  int push(int index, entries&& items) override {
    lr::ObjectWriteOperation op;
    cls_log_add(op, std::get<centries>(items), true);
    auto r = rgw_rados_operate(ioctx, oids[index], &op, null_yield);
    if (r < 0) {
      lderr(cct) << __PRETTY_FUNCTION__
		 << ": failed to push to " << oids[index] << cpp_strerror(-r)
		 << dendl;
    }
    return r;
  }
  int push(int index, ceph::real_time now,
	   const std::string& key,
	   ceph::buffer::list&& bl) override {
    lr::ObjectWriteOperation op;
    cls_log_add(op, utime_t(now), {}, key, bl);
    auto r = rgw_rados_operate(ioctx, oids[index], &op, null_yield);
    if (r < 0) {
      lderr(cct) << __PRETTY_FUNCTION__
		 << ": failed to push to " << oids[index]
		 << cpp_strerror(-r) << dendl;
    }
    return r;
  }
  int list(int index, int max_entries,
	   std::vector<rgw_data_change_log_entry>& entries,
	   std::optional<std::string_view> marker,
	   std::string* out_marker, bool* truncated) override {
    std::list<cls_log_entry> log_entries;
    lr::ObjectReadOperation op;
    cls_log_list(op, {}, {}, std::string(marker.value_or("")),
		 max_entries, log_entries, out_marker, truncated);
    auto r = rgw_rados_operate(ioctx, oids[index], &op, nullptr, null_yield);
    if (r == -ENOENT) {
      *truncated = false;
      return 0;
    }
    if (r < 0) {
      lderr(cct) << __PRETTY_FUNCTION__
		 << ": failed to list " << oids[index]
		 << cpp_strerror(-r) << dendl;
      return r;
    }
    for (auto iter = log_entries.begin(); iter != log_entries.end(); ++iter) {
      rgw_data_change_log_entry log_entry;
      log_entry.log_id = iter->id;
      auto rt = iter->timestamp.to_real_time();
      log_entry.log_timestamp = rt;
      auto liter = iter->data.cbegin();
      try {
	decode(log_entry.entry, liter);
      } catch (ceph::buffer::error& err) {
	lderr(cct) << __PRETTY_FUNCTION__
		   << ": failed to decode data changes log entry: "
		   << err.what() << dendl;
	return -EIO;
      }
      entries.push_back(log_entry);
    }
    return 0;
  }
  int get_info(int index, RGWDataChangesLogInfo *info) override {
    cls_log_header header;
    lr::ObjectReadOperation op;
    cls_log_info(op, &header);
    auto r = rgw_rados_operate(ioctx, oids[index], &op, nullptr, null_yield);
    if (r == -ENOENT) r = 0;
    if (r < 0) {
      lderr(cct) << __PRETTY_FUNCTION__
		 << ": failed to get info from " << oids[index]
		 << cpp_strerror(-r) << dendl;
    } else {
      info->marker = header.max_marker;
      info->last_update = header.max_time.to_real_time();
    }
    return r;
  }
  int trim(int index, std::string_view marker) override {
    lr::ObjectWriteOperation op;
    cls_log_trim(op, {}, {}, {}, std::string(marker));
    auto r = rgw_rados_operate(ioctx, oids[index], &op, null_yield);
    if (r == -ENOENT) r = 0;
    if (r < 0) {
      lderr(cct) << __PRETTY_FUNCTION__
		 << ": failed to get info from " << oids[index]
		 << cpp_strerror(-r) << dendl;
    }
    return r;
  }
  int trim(int index, std::string_view marker,
	   lr::AioCompletion* c) override {
    lr::ObjectWriteOperation op;
    cls_log_trim(op, {}, {}, {}, std::string(marker));
    auto r = ioctx.aio_operate(oids[index], c, &op, 0);
    if (r == -ENOENT) r = 0;
    if (r < 0) {
      lderr(cct) << __PRETTY_FUNCTION__
		 << ": failed to get info from " << oids[index]
		 << cpp_strerror(-r) << dendl;
    }
    return r;
  }
  std::string_view max_marker() const override {
    return "99999999"sv;
  }
};

class RGWDataChangesFIFO final : public RGWDataChangesBE {
  using centries = std::vector<ceph::buffer::list>;
  std::vector<std::unique_ptr<rgw::cls::fifo::FIFO>> fifos;

public:
  RGWDataChangesFIFO(lr::IoCtx& ioctx,
		     RGWDataChangesLog& datalog,
		     uint64_t gen_id, int shards)
    : RGWDataChangesBE(ioctx, datalog, gen_id) {
    fifos.resize(shards);
    for (auto i = 0; i < shards; ++i) {
      auto  r = rgw::cls::fifo::FIFO::create(ioctx, get_oid(i),
					     &fifos[i], null_yield);
      if (r < 0) {
	throw std::system_error(-r, std::system_category());
      }
    }
    ceph_assert(fifos.size() == unsigned(shards));
    ceph_assert(std::none_of(fifos.cbegin(), fifos.cend(),
			     [](const auto& p) {
			       return p == nullptr;
			     }));
  }
  ~RGWDataChangesFIFO() override = default;
  void prepare(ceph::real_time, const std::string&,
	       ceph::buffer::list&& entry, entries& out) override {
    if (!std::holds_alternative<centries>(out)) {
      ceph_assert(std::visit([](auto& v) { return std::empty(v); }, out));
      out = centries();
    }
    std::get<centries>(out).push_back(std::move(entry));
  }
  int push(int index, entries&& items) override {
    auto r = fifos[index]->push(std::get<centries>(items), null_yield);
    if (r < 0) {
      lderr(cct) << __PRETTY_FUNCTION__
		 << ": unable to push to FIFO: " << get_oid(index)
		 << ": " << cpp_strerror(-r) << dendl;
    }
    return r;
  }
  int push(int index, ceph::real_time,
	   const std::string&,
	   ceph::buffer::list&& bl) override {
    auto r = fifos[index]->push(std::move(bl), null_yield);
    if (r < 0) {
      lderr(cct) << __PRETTY_FUNCTION__
		 << ": unable to push to FIFO: " << get_oid(index)
		 << ": " << cpp_strerror(-r) << dendl;
    }
    return r;
  }
  int list(int index, int max_entries,
	   std::vector<rgw_data_change_log_entry>& entries,
	   std::optional<std::string_view> marker,
	   std::string* out_marker, bool* truncated) override {
    std::vector<rgw::cls::fifo::list_entry> log_entries;
    bool more = false;
    auto r = fifos[index]->list(max_entries, marker, &log_entries, &more,
				null_yield);
    if (r < 0) {
      lderr(cct) << __PRETTY_FUNCTION__
		 << ": unable to list FIFO: " << get_oid(index)
		 << ": " << cpp_strerror(-r) << dendl;
      return r;
    }
    for (const auto& entry : log_entries) {
      rgw_data_change_log_entry log_entry;
      log_entry.log_id = entry.marker;
      log_entry.log_timestamp = entry.mtime;
      auto liter = entry.data.cbegin();
      try {
	decode(log_entry.entry, liter);
      } catch (const buffer::error& err) {
	lderr(cct) << __PRETTY_FUNCTION__
		   << ": failed to decode data changes log entry: "
		   << err.what() << dendl;
	return -EIO;
      }
      entries.push_back(std::move(log_entry));
    }
    if (truncated)
      *truncated = more;
    if (out_marker && !log_entries.empty()) {
      *out_marker = log_entries.back().marker;
    }
    return 0;
  }
  int get_info(int index, RGWDataChangesLogInfo *info) override {
    auto& fifo = fifos[index];
    auto r = fifo->read_meta(null_yield);
    if (r < 0) {
      lderr(cct) << __PRETTY_FUNCTION__
		 << ": unable to get FIFO metadata: " << get_oid(index)
		 << ": " << cpp_strerror(-r) << dendl;
      return r;
    }
    auto m = fifo->meta();
    auto p = m.head_part_num;
    if (p < 0) {
      info->marker = rgw::cls::fifo::marker{}.to_string();
      info->last_update = ceph::real_clock::zero();
      return 0;
    }
    rgw::cls::fifo::part_info h;
    r = fifo->get_part_info(p, &h, null_yield);
    if (r < 0) {
      lderr(cct) << __PRETTY_FUNCTION__
		 << ": unable to get part info: " << get_oid(index) << "/" << p
		 << ": " << cpp_strerror(-r) << dendl;
      return r;
    }
    info->marker = rgw::cls::fifo::marker{p, h.last_ofs}.to_string();
    info->last_update = h.max_time;
    return 0;
  }
  int trim(int index, std::string_view marker) override {
    auto r = fifos[index]->trim(marker, false, null_yield);
    if (r < 0) {
      lderr(cct) << __PRETTY_FUNCTION__
		 << ": unable to trim FIFO: " << get_oid(index)
		 << ": " << cpp_strerror(-r) << dendl;
    }
    return r;
  }
  int trim(int index, std::string_view marker,
	   librados::AioCompletion* c) override {
    int r = 0;
    if (marker == rgw::cls::fifo::marker(0, 0).to_string()) {
      auto pc = c->pc;
      pc->get();
      pc->lock.lock();
      pc->rval = 0;
      pc->complete = true;
      pc->lock.unlock();
      auto cb_complete = pc->callback_complete;
      auto cb_complete_arg = pc->callback_complete_arg;
      if (cb_complete)
	cb_complete(pc, cb_complete_arg);

      auto cb_safe = pc->callback_safe;
      auto cb_safe_arg = pc->callback_safe_arg;
      if (cb_safe)
	cb_safe(pc, cb_safe_arg);

      pc->lock.lock();
      pc->callback_complete = NULL;
      pc->callback_safe = NULL;
      pc->cond.Signal();
      pc->put_unlock();
    } else {
      fifos[index]->trim(marker, false, c);
    }
    return r;
  }
  std::string_view max_marker() const override {
    static const std::string mm =
      rgw::cls::fifo::marker::max().to_string();
    return std::string_view(mm);
  }
};

RGWDataChangesLog::RGWDataChangesLog(CephContext* cct, RGWRados* store)
  : cct(cct), store(store),
    num_shards(cct->_conf->rgw_data_log_num_shards),
    prefix(get_prefix()),
    changes(cct->_conf->rgw_data_log_changes_size) {}

int RGWDataChangesLog::init()
{
  auto defbacking = to_log_type(
    cct->_conf.get_val<std::string>("rgw_default_data_log_backing"));
  // Should be guaranteed by `set_enum_allowed`
  auto log_pool = store->svc.zone->get_zone_params().log_pool;
  auto r = rgw_init_ioctx(store->get_rados_handle(), log_pool, ioctx,
			  true, false);
  if (r < 0) {
    lderr(cct) << __PRETTY_FUNCTION__
	       << ": Failed to initialized ioctx, r=" << r
	       << ", pool=" << log_pool << dendl;
    return -r;
  }

  auto found = log_backing_type(ioctx, *defbacking, num_shards,
				[this](int i) { return get_oid(0, i); },
				null_yield);

  if (!found) {
    lderr(cct) << __PRETTY_FUNCTION__
	       << ": Error when checking log type: r="
	       << found.error() << dendl;
  }
  try {
    switch (*found) {
    case log_type::omap:
      be = std::make_unique<RGWDataChangesOmap>(ioctx, *this, 0, num_shards);
      break;
    case log_type::fifo:
      be = std::make_unique<RGWDataChangesFIFO>(ioctx, *this, 0, num_shards);
      break;
    }
  } catch (const std::system_error& e) {
    lderr(cct) << __PRETTY_FUNCTION__
	       << ": Error when starting backend: "
	       << e.what() << dendl;
    return e.code().value();
  }

  ceph_assert(be);
  renew_thread = make_named_thread("rgw_dt_lg_renew",
				   &RGWDataChangesLog::renew_run, this);
  return 0;
}

int RGWDataChangesLog::choose_oid(const rgw_bucket_shard& bs) {
  const auto& name = bs.bucket.name;
  auto shard_shift = (bs.shard_id > 0 ? bs.shard_id : 0);
  auto r = (ceph_str_hash_linux(name.data(), name.size()) +
	    shard_shift) % num_shards;
  return static_cast<int>(r);
}

int RGWDataChangesLog::renew_entries()
{
  if (!store->svc.zone->need_to_log_data())
    return 0;

  /* we can't keep the bucket name as part of the cls_log_entry, and we need
   * it later, so we keep two lists under the map */
  bc::flat_map<int, std::pair<std::vector<rgw_bucket_shard>,
			      RGWDataChangesBE::entries>> m;

  std::unique_lock l(lock);
  decltype(cur_cycle) entries;
  entries.swap(cur_cycle);
  l.unlock();

  std::string section;
  auto ut = real_clock::now();
  for (const auto& bs : entries) {
    auto index = choose_oid(bs);

    rgw_data_change change;
    bufferlist bl;
    change.entity_type = ENTITY_TYPE_BUCKET;
    change.key = bs.get_key();
    change.timestamp = ut;
    encode(change, bl);

    m[index].first.push_back(bs);
    be->prepare(ut, change.key, std::move(bl), m[index].second);
  }

  for (auto& [index, p] : m) {
    auto& [buckets, entries] = p;

    auto now = real_clock::now();

    auto ret = be->push(index, std::move(entries));
    if (ret < 0) {
      /* we don't really need to have a special handling for failed cases here,
       * as this is just an optimization. */
      lderr(cct) << "ERROR: svc.cls->timelog.add() returned " << ret << dendl;
      return ret;
    }

    auto expiration = now;
    expiration += ceph::make_timespan(cct->_conf->rgw_data_log_window);
    for (auto& bs : buckets) {
      update_renewed(bs, expiration);
    }
  }

  return 0;
}

void RGWDataChangesLog::_get_change(const rgw_bucket_shard& bs,
				    ChangeStatusPtr& status)
{
  ceph_assert(ceph_mutex_is_locked(lock));
  if (!changes.find(bs, status)) {
    status = ChangeStatusPtr(new ChangeStatus);
    changes.add(bs, status);
  }
}

void RGWDataChangesLog::register_renew(const rgw_bucket_shard& bs)
{
  std::scoped_lock l{lock};
  cur_cycle.insert(bs);
}

void RGWDataChangesLog::update_renewed(const rgw_bucket_shard& bs,
				       real_time expiration)
{
  std::scoped_lock l{lock};
  ChangeStatusPtr status;
  _get_change(bs, status);

  ldout(cct, 20) << "RGWDataChangesLog::update_renewd() bucket_name="
		 << bs.bucket.name << " shard_id=" << bs.shard_id
		 << " expiration=" << expiration << dendl;
  status->cur_expiration = expiration;
}

int RGWDataChangesLog::get_log_shard_id(rgw_bucket& bucket, int shard_id) {
  rgw_bucket_shard bs(bucket, shard_id);

  return choose_oid(bs);
}

std::string RGWDataChangesLog::get_oid(uint64_t gen_id, int i) const {
  return (gen_id > 0 ?
	  fmt::format("{}@G{}.{}", prefix, gen_id, i) :
	  fmt::format("{}.{}", prefix, i));
}

int RGWDataChangesLog::add_entry(const rgw_bucket& bucket, int shard_id) {
  if (!store->svc.zone->need_to_log_data())
    return 0;

  if (observer) {
    observer->on_bucket_changed(bucket.get_key());
  }

  rgw_bucket_shard bs(bucket, shard_id);

  int index = choose_oid(bs);
  mark_modified(index, bs);

  std::unique_lock l(lock);
  ChangeStatusPtr status;
  _get_change(bs, status);
  l.unlock();

  auto now = real_clock::now();

  std::unique_lock sl(status->lock);

  ldout(cct, 20) << "RGWDataChangesLog::add_entry() bucket.name=" << bucket.name
		 << " shard_id=" << shard_id << " now=" << now
		 << " cur_expiration=" << status->cur_expiration << dendl;

  if (now < status->cur_expiration) {
    /* no need to send, recently completed */
    sl.unlock();
    register_renew(bs);
    return 0;
  }

  RefCountedCond* cond;

  if (status->pending) {
    cond = status->cond;

    ceph_assert(cond);

    status->cond->get();
    sl.unlock();

    int ret = cond->wait();
    cond->put();
    if (!ret) {
      register_renew(bs);
    }
    return ret;
  }

  status->cond = new RefCountedCond;
  status->pending = true;

  ceph::real_time expiration;
  int ret = 0;

  do {
    status->cur_sent = now;

    expiration = now;
    expiration += ceph::make_timespan(cct->_conf->rgw_data_log_window);

    sl.unlock();

    ceph::buffer::list bl;
    rgw_data_change change;
    change.entity_type = ENTITY_TYPE_BUCKET;
    change.key = bs.get_key();
    change.timestamp = now;
    encode(change, bl);
    ldout(cct, 20) << "RGWDataChangesLog::add_entry() sending update with now=" << now << " cur_expiration=" << expiration << dendl;

    ret = be->push(index, now, change.key, std::move(bl));

    now = real_clock::now();

    sl.lock();

  } while (!ret && real_clock::now() > expiration);

  cond = status->cond;

  status->pending = false;
  /* time of when operation started, not completed */
  status->cur_expiration = status->cur_sent;
  status->cur_expiration += make_timespan(cct->_conf->rgw_data_log_window);
  status->cond = nullptr;
  sl.unlock();

  cond->done(ret);
  cond->put();

  return ret;
}

int RGWDataChangesLog::list_entries(int shard, int max_entries,
				    std::vector<rgw_data_change_log_entry>& entries,
				    std::optional<std::string_view> marker,
				    std::string* out_marker, bool* truncated)
{
  assert(shard < num_shards);
  return be->list(shard, max_entries, entries, std::string(marker.value_or("")),
		  out_marker, truncated);
}

int RGWDataChangesLog::list_entries(int max_entries,
				    std::vector<rgw_data_change_log_entry>& entries,
				    LogMarker& marker, bool *ptruncated)
{
  bool truncated;
  entries.clear();
  for (; marker.shard < num_shards && int(entries.size()) < max_entries;
       marker.shard++, marker.marker.reset()) {
    int ret = list_entries(marker.shard, max_entries - entries.size(),
			   entries, marker.marker, NULL, &truncated);
    if (ret == -ENOENT) {
      continue;
    }
    if (ret < 0) {
      return ret;
    }
    if (truncated) {
      *ptruncated = true;
      return 0;
    }
  }
  *ptruncated = (marker.shard < num_shards);
  return 0;
}

int RGWDataChangesLog::get_info(int shard_id, RGWDataChangesLogInfo *info)
{
  assert(shard_id < num_shards);
  return be->get_info(shard_id, info);
}

int RGWDataChangesLog::trim_entries(int shard_id, std::string_view marker)
{
  assert(shard_id < num_shards);
  return be->trim(shard_id, marker);
}

int RGWDataChangesLog::trim_entries(int shard_id, std::string_view marker,
				    librados::AioCompletion* c)
{
  assert(shard_id < num_shards);
  return be->trim(shard_id, marker, c);
}

int RGWDataChangesLog::lock_exclusive(int shard_id, timespan duration, string& zone_id, string& owner_id) {
  return store->lock_exclusive(store->svc.zone->get_zone_params().log_pool,
			       get_oid(0, shard_id), duration, zone_id, owner_id);
}

int RGWDataChangesLog::unlock(int shard_id, string& zone_id, string& owner_id) {
  return store->unlock(store->svc.zone->get_zone_params().log_pool,
		       get_oid(0, shard_id), zone_id, owner_id);
}

bool RGWDataChangesLog::going_down() const
{
  return down_flag;
}

std::string_view RGWDataChangesLog::max_marker() const {
  return "99999999"sv;
}

RGWDataChangesLog::~RGWDataChangesLog() {
  down_flag = true;
  if (renew_thread.joinable()) {
    renew_stop();
    renew_thread.join();
  }
}

void RGWDataChangesLog::renew_run() {
  do {
    ldout(cct, 2) << "RGWDataChangesLog::ChangesRenewThread: start" << dendl;
    int r = renew_entries();
    if (r < 0) {
      ldout(cct, 0) << "ERROR: RGWDataChangesLog::renew_entries returned error r=" << r << dendl;
    }

    if (going_down())
      break;

    int interval = cct->_conf->rgw_data_log_window * 3 / 4;
    std::unique_lock l(renew_lock);
    renew_cond.wait_for(l, interval * 1s);
    l.unlock();
  } while (!going_down());
}

void RGWDataChangesLog::renew_stop()
{
  std::lock_guard l(renew_lock);
  renew_cond.notify_all();
}

void RGWDataChangesLog::mark_modified(int shard_id, const rgw_bucket_shard& bs)
{
  auto key = bs.get_key();
  {
    std::shared_lock rl{modified_lock}; // read lock to check for existence
    auto shard = modified_shards.find(shard_id);
    if (shard != modified_shards.end() && shard->second.count(key)) {
      return;
    }
  }

  std::unique_lock wl{modified_lock}; // write lock for insertion
  modified_shards[shard_id].insert(key);
}
