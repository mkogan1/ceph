// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#undef FMT_HEADER_ONLY
#define FMT_HEADER_ONLY 1
#include "fmt/format.h"

#include "rgw/rgw_datalog.h"
#include "rgw/rgw_zone.h"

static constexpr auto dout_subsys = ceph_subsys_rgw;

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

int RGWDataChangesLog::choose_oid(const rgw_bucket_shard& bs) {
    const string& name = bs.bucket.name;
    int shard_shift = (bs.shard_id > 0 ? bs.shard_id : 0);
    uint32_t r = (ceph_str_hash_linux(name.c_str(), name.size()) + shard_shift) % num_shards;

    return (int)r;
}

std::string RGWDataChangesLog::get_oid(int i) const {
  std::string_view prefix = cct->_conf->rgw_data_log_obj_prefix;
  if (prefix.empty()) {
    prefix = "data_log"sv;
  }
  return fmt::format("{}.{}", prefix, i);
}

int RGWDataChangesLog::renew_entries()
{
  if (!store->svc.zone->need_to_log_data())
    return 0;

  /* we can't keep the bucket name as part of the cls_log_entry, and we need
   * it later, so we keep two lists under the map */
  std::map<int, pair<list<rgw_bucket_shard>, list<cls_log_entry>>> m;

  std::unique_lock l(lock);
  std::map<rgw_bucket_shard, bool> entries;
  entries.swap(cur_cycle);
  l.unlock();

  std::string section;
  auto ut = real_clock::now();
  for (auto iter = entries.begin(); iter != entries.end(); ++iter) {
    const auto& bs = iter->first;

    int index = choose_oid(bs);

    cls_log_entry entry;

    rgw_data_change change;
    bufferlist bl;
    change.entity_type = ENTITY_TYPE_BUCKET;
    change.key = bs.get_key();
    change.timestamp = ut;
    encode(change, bl);

    store->time_log_prepare_entry(entry, ut, section, change.key, bl);

    m[index].first.push_back(bs);
    m[index].second.emplace_back(std::move(entry));
  }

  for (auto miter = m.begin(); miter != m.end(); ++miter) {
    auto& entries = miter->second.second;

    auto now = real_clock::now();

    int ret = store->time_log_add(oids[miter->first], entries, NULL);
    if (ret < 0) {
      /* we don't really need to have a special handling for failed cases here,
       * as this is just an optimization. */
      lderr(cct) << "ERROR: store->time_log_add() returned " << ret << dendl;
      return ret;
    }

    real_time expiration = now;
    expiration += make_timespan(cct->_conf->rgw_data_log_window);

    auto& buckets = miter->second.first;
    for (auto liter = buckets.begin(); liter != buckets.end(); ++liter) {
      update_renewed(*liter, expiration);
    }
  }

  return 0;
}

void RGWDataChangesLog::_get_change(const rgw_bucket_shard& bs, ChangeStatusPtr& status)
{
  ceph_assert(lock.is_locked());
  if (!changes.find(bs, status)) {
    status = ChangeStatusPtr(new ChangeStatus);
    changes.add(bs, status);
  }
}

void RGWDataChangesLog::register_renew(rgw_bucket_shard& bs)
{
  std::lock_guard l(lock);
  cur_cycle[bs] = true;
}

void RGWDataChangesLog::update_renewed(rgw_bucket_shard& bs,
				       real_time expiration)
{
  std::lock_guard l(lock);
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

int RGWDataChangesLog::add_entry(rgw_bucket& bucket, int shard_id) {
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

  real_time now = real_clock::now();

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

  string& oid = oids[index];
  real_time expiration;

  int ret;

  do {
    status->cur_sent = now;

    expiration = now;
    expiration += ceph::make_timespan(cct->_conf->rgw_data_log_window);

    sl.unlock();

    bufferlist bl;
    rgw_data_change change;
    change.entity_type = ENTITY_TYPE_BUCKET;
    change.key = bs.get_key();
    change.timestamp = now;
    encode(change, bl);
    string section;

    ldout(cct, 20) << "RGWDataChangesLog::add_entry() sending update with now=" << now << " cur_expiration=" << expiration << dendl;

    ret = store->time_log_add(oid, now, section, change.key, bl);

    now = real_clock::now();

    sl.lock();

  } while (!ret && real_clock::now() > expiration);

  cond = status->cond;

  status->pending = false;
  status->cur_expiration = status->cur_sent; /* time of when operation started, not completed */
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
  list<cls_log_entry> log_entries;

  int ret = store->time_log_list(oids[shard], {}, {}, max_entries, log_entries,
				 std::string(marker.value_or("")),
				 out_marker, truncated);
  if (ret < 0)
    return ret;

  for (auto iter = log_entries.begin(); iter != log_entries.end(); ++iter) {
    rgw_data_change_log_entry log_entry;
    log_entry.log_id = iter->id;
    real_time rt = iter->timestamp.to_real_time();
    log_entry.log_timestamp = rt;
    auto liter = iter->data.cbegin();
    try {
      decode(log_entry.entry, liter);
    } catch (buffer::error& err) {
      lderr(cct) << "ERROR: failed to decode data changes log entry" << dendl;
      return -EIO;
    }
    entries.push_back(log_entry);
  }

  return 0;
}

int RGWDataChangesLog::list_entries(int max_entries, std::vector<rgw_data_change_log_entry>& entries,
				    LogMarker& marker, bool* ptruncated)
{
  bool truncated;
  entries.clear();

  for (; marker.shard < num_shards && (int)entries.size() < max_entries;
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

int RGWDataChangesLog::get_info(int shard_id, RGWDataChangesLogInfo* info)
{
  assert(shard_id < num_shards);

  string oid = oids[shard_id];

  cls_log_header header;

  int ret = store->time_log_info(oid, &header);
  if ((ret < 0) && (ret != -ENOENT))
    return ret;

  info->marker = header.max_marker;
  info->last_update = header.max_time.to_real_time();

  return 0;
}

int RGWDataChangesLog::trim_entries(int shard_id, std::string_view marker)
{
  assert(shard_id < num_shards);
  return store->time_log_trim(oids[shard_id], {}, {}, {}, std::string(marker),
			      nullptr);
}

int RGWDataChangesLog::trim_entries(int shard_id, std::string_view marker,
				    librados::AioCompletion* c)
{
  assert(shard_id < num_shards);
  return store->time_log_trim(oids[shard_id], {}, {}, {}, std::string(marker),
			      c);
}

int RGWDataChangesLog::lock_exclusive(int shard_id, timespan duration, string& zone_id, string& owner_id) {
  return store->lock_exclusive(store->svc.zone->get_zone_params().log_pool, oids[shard_id], duration, zone_id, owner_id);
}

int RGWDataChangesLog::unlock(int shard_id, string& zone_id, string& owner_id) {
  return store->unlock(store->svc.zone->get_zone_params().log_pool, oids[shard_id], zone_id, owner_id);
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
  delete[] oids;
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

  std::shared_lock rl(modified_lock);
  auto iter = modified_shards.find(shard_id);
  if (iter != modified_shards.end()) {
    auto& keys = iter->second;
    if (keys.find(key) != keys.end()) {
      rl.unlock();
      return;
    }
  }
  rl.unlock();

  std::unique_lock wl(modified_lock);
  modified_shards[shard_id].insert(key);
}

void RGWDataChangesLog::read_clear_modified(map<int, set<string> > &modified)
{
  std::unique_lock wl(modified_lock);
  modified.swap(modified_shards);
  modified_shards.clear();
}
