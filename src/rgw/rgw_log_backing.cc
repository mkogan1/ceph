// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "cls/log/cls_log_client.h"
#include "cls/version/cls_version_client.h"

#include "rgw_log_backing.h"
#include "rgw_tools.h"
#include "cls_fifo_legacy.h"

namespace cb = ceph::buffer;

static constexpr auto dout_subsys = ceph_subsys_rgw;

enum class shard_check { dne, omap, fifo, corrupt };
inline std::ostream& operator <<(std::ostream& m, const shard_check& t) {
  switch (t) {
  case shard_check::dne:
    return m << "shard_check::dne";
  case shard_check::omap:
    return m << "shard_check::omap";
  case shard_check::fifo:
    return m << "shard_check::fifo";
  case shard_check::corrupt:
    return m << "shard_check::corrupt";
  }

  return m << "shard_check::UNKNOWN=" << static_cast<uint32_t>(t);
}

namespace {
/// Return the shard type, and a bool to see whether it has entries.
shard_check
probe_shard(librados::IoCtx& ioctx, const std::string& oid,
	    bool& fifo_unsupported, optional_yield y)
{
  auto cct = static_cast<CephContext*>(ioctx.cct());
  ldout(cct, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
		 << " probing oid=" << oid
		 << dendl;
  if (!fifo_unsupported) {
    std::unique_ptr<rgw::cls::fifo::FIFO> fifo;
    auto r = rgw::cls::fifo::FIFO::open(ioctx, oid,
					&fifo, y,
					std::nullopt, true);
    switch (r) {
    case 0:
      ldout(cct, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
		     << ": oid=" << oid << " is FIFO"
		     << dendl;
      return shard_check::fifo;
      break;

    case -ENODATA:
      ldout(cct, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
		     << ": oid=" << oid << " is empty and therefore OMAP"
		     << dendl;
      return shard_check::omap;
      break;

    case -ENOENT:
      ldout(cct, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
		     << ": oid=" << oid << " does not exist"
		     << dendl;
      return shard_check::dne;
      break;

    case -EPERM:
      ldout(cct, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
		     << ": FIFO is unsupported, marking."
		     << dendl;
      fifo_unsupported = true;
      return shard_check::omap;
      break;

    default:
      lderr(cct) << __PRETTY_FUNCTION__ << ":" << __LINE__
		 << ": error probing: r=" << r
		 << ", oid=" << oid << dendl;
      return shard_check::corrupt;
    }
  } else {
    // Since FIFO is unsupported, OMAP is the only alternative
    return shard_check::omap;
  }
}

tl::expected<log_type, int>
handle_dne(librados::IoCtx& ioctx,
	   log_type def,
	   std::string oid,
	   bool fifo_unsupported,
	   optional_yield y)
{
  auto cct = static_cast<CephContext*>(ioctx.cct());
  if (def == log_type::fifo) {
    if (fifo_unsupported) {
      lderr(cct) << __PRETTY_FUNCTION__ << ":" << __LINE__
		 << " WARNING: FIFO set as default but not supported by OSD. "
		 << "Falling back to OMAP." << dendl;
      return log_type::omap;
    }
    std::unique_ptr<rgw::cls::fifo::FIFO> fifo;
    auto r = rgw::cls::fifo::FIFO::create(ioctx, oid,
					  &fifo, y,
					  std::nullopt);
    if (r < 0) {
      lderr(cct) << __PRETTY_FUNCTION__ << ":" << __LINE__
		 << " error creating FIFO: r=" << r
		 << ", oid=" << oid << dendl;
      return tl::unexpected(r);
    }
  }
  return def;
}
}

tl::expected<log_type, int>
log_backing_type(librados::IoCtx& ioctx,
		 log_type def,
		 int shards,
		 const fu2::unique_function<std::string(int) const>& get_oid,
		 optional_yield y)
{
  auto cct = static_cast<CephContext*>(ioctx.cct());
  auto check = shard_check::dne;
  bool fifo_unsupported = false;
  for (int i = 0; i < shards; ++i) {
    auto c = probe_shard(ioctx, get_oid(i), fifo_unsupported, y);
    if (c == shard_check::corrupt)
      return tl::unexpected(-EIO);
    if (c == shard_check::dne) continue;
    if (check == shard_check::dne) {
      check = c;
      continue;
    }

    if (check != c) {
      lderr(cct) << __PRETTY_FUNCTION__ << ":" << __LINE__
		 << " clashing types: check=" << check
		 << ", c=" << c << dendl;
      return tl::unexpected(-EIO);
    }
  }
  if (check == shard_check::corrupt) {
    lderr(cct) << __PRETTY_FUNCTION__ << ":" << __LINE__
	       << " should be unreachable!" << dendl;
    return tl::unexpected(-EIO);
  }

  if (check == shard_check::dne)
    return handle_dne(ioctx,
		      def,
		      get_oid(0),
		      fifo_unsupported,
		      y);

  return (check == shard_check::fifo ? log_type::fifo : log_type::omap);
}

int log_remove(librados::IoCtx& ioctx,
	       int shards,
	       const fu2::unique_function<std::string(int) const>& get_oid,
	       bool leave_zero,
	       optional_yield y)
{
  int r1 = 0;
  auto cct = static_cast<CephContext*>(ioctx.cct());
  for (int i = 0; i < shards; ++i) {
    auto oid = get_oid(i);
    rados::cls::fifo::info info;
    uint32_t part_header_size = 0, part_entry_overhead = 0;

    auto r = rgw::cls::fifo::get_meta(ioctx, oid, nullopt, &info,
				      &part_header_size, &part_entry_overhead,
				      0, y, true);
    if (r == -ENOENT) continue;
    if (r == 0 && info.head_part_num > -1) {
      for (auto j = info.tail_part_num; j <= info.head_part_num; ++j) {
	librados::ObjectWriteOperation op;
	op.remove();
	auto part_oid = info.part_oid(j);
	auto subr = rgw_rados_operate(ioctx, part_oid, &op, null_yield);
	if (subr < 0 && subr != -ENOENT) {
	  if (r1 >= 0)
	    r1 = subr;
	  lderr(cct) << __PRETTY_FUNCTION__ << ":" << __LINE__
		     << ": failed removing FIFO part: part_oid=" << part_oid
		     << ", subr=" << subr << dendl;
	}
      }
    }
    if (r < 0 && r != -ENODATA) {
      if (r1 >= 0)
	r1 = r;
      lderr(cct) << __PRETTY_FUNCTION__ << ":" << __LINE__
		 << ": failed checking FIFO part: oid=" << oid
		 << ", r=" << r << dendl;
    }
    librados::ObjectWriteOperation op;
    if (i == 0 && leave_zero) {
      // Leave shard 0 in existence, but remove contents and
      // omap. cls_lock stores things in the xattrs. And sync needs to
      // rendezvous with locks on generation 0 shard 0.
      op.omap_set_header({});
      op.omap_clear();
      op.truncate(0);
    } else {
      op.remove();
    }
    r = rgw_rados_operate(ioctx, oid, &op, null_yield);
    if (r < 0 && r != -ENOENT) {
      if (r1 >= 0)
	r1 = r;
      lderr(cct) << __PRETTY_FUNCTION__ << ":" << __LINE__
		 << ": failed removing shard: oid=" << oid
		 << ", r=" << r << dendl;
    }
  }
  return r1;
}

logback_generations::~logback_generations() {
  if (watchcookie > 0) {
    auto cct = static_cast<CephContext*>(ioctx.cct());
    auto r = ioctx.unwatch2(watchcookie);
    if (r < 0) {
      lderr(cct) << __PRETTY_FUNCTION__ << ":" << __LINE__
		 << ": failed unwatching oid=" << oid
		 << ", r=" << r << dendl;
    }
  }
}

int logback_generations::setup(log_type def, optional_yield y) noexcept
{
  try {
    auto cct = static_cast<CephContext*>(ioctx.cct());
    // First, read.
    auto res = read(y);
    if (!res && res.error() != -ENOENT) {
      return res.error();
    }
    if (res) {
      std::unique_lock lock(m);
      std::tie(entries_, version) = std::move(*res);
    } else {
      // Are we the first? Then create generation 0 and the generations
      // metadata.
      librados::ObjectWriteOperation op;
      auto type = log_backing_type(ioctx, def, shards,
				   [this](int shard) {
				     return this->get_oid(0, shard);
				   }, y);
      if (!type)
	return type.error();

      logback_generation l;
      l.type = *type;

      std::unique_lock lock(m);
      version.ver = 1;
      static constexpr auto TAG_LEN = 24;
      version.tag.clear();
      append_rand_alpha(cct, version.tag, version.tag, TAG_LEN);
      op.create(true);
      cls_version_set(op, version);
      cb::list bl;
      entries_.emplace(0, std::move(l));
      encode(entries_, bl);
      lock.unlock();

      op.write_full(bl);
      auto r = rgw_rados_operate(ioctx, oid, &op, y);
      if (r < 0 && r != -EEXIST) {
	lderr(cct) << __PRETTY_FUNCTION__ << ":" << __LINE__
		   << ": failed writing oid=" << oid
		   << ", r=" << r << dendl;
	bs::system_error(-r, bs::system_category());
      }
      // Did someone race us? Then re-read.
      if (r != 0) {
	res = read(y);
	if (!res)
	  return res.error();
	if (res->first.empty())
	  return -EIO;
	auto l = res->first.begin()->second;
	// In the unlikely event that someone raced us, created
	// generation zero, incremented, then erased generation zero,
	// don't leave generation zero lying around.
	if (l.gen_id != 0) {
	  r = log_remove(ioctx, shards,
			 [this](int shard) {
			   return this->get_oid(0, shard);
			 }, true, y);
	  if (r < 0)
	    return r;
	}
	std::unique_lock lock(m);
	std::tie(entries_, version) = std::move(*res);
      }
    }
    // Pass all non-empty generations to the handler
    std::unique_lock lock(m);
    auto i = lowest_nomempty(entries_);
    entries_t e;
    std::copy(i, entries_.cend(),
	      std::inserter(e, e.end()));
    m.unlock();
    int r = watch();
    if (r < 0) {
      lderr(cct) << __PRETTY_FUNCTION__ << ":" << __LINE__
		 << ": failed to re-establish watch, unsafe to continue: oid="
		 << oid << ", r=" << r << dendl;
    }
    return handle_init(std::move(e));
  } catch (const std::bad_alloc&) {
    return -ENOMEM;
  }
}

int logback_generations::update(optional_yield y) noexcept
{
  try {
    auto cct = static_cast<CephContext*>(ioctx.cct());
    auto res = read(y);
    if (!res) {
      return res.error();
    }

    std::unique_lock l(m);
    auto& [es, v] = *res;
    if (v == version) {
      // Nothing to do!
      return 0;
    }

    // Check consistency and prepare update
    if (es.empty()) {
      lderr(cct) << __PRETTY_FUNCTION__ << ":" << __LINE__
		 << ": INCONSISTENCY! Read empty update." << dendl;
      return -EFAULT;
    }
    auto cur_lowest = lowest_nomempty(entries_);
    // Straight up can't happen
    assert(cur_lowest != entries_.cend());
    auto new_lowest = lowest_nomempty(es);
    if (new_lowest == es.cend()) {
      lderr(cct) << __PRETTY_FUNCTION__ << ":" << __LINE__
		 << ": INCONSISTENCY! Read update with no active head." << dendl;
      return -EFAULT;
    }
    if (new_lowest->first < cur_lowest->first) {
      lderr(cct) << __PRETTY_FUNCTION__ << ":" << __LINE__
		 << ": INCONSISTENCY! Tail moved wrong way." << dendl;
      return -EFAULT;
    }

    std::optional<uint64_t> highest_empty;
    if (new_lowest->first > cur_lowest->first && new_lowest != es.begin()) {
      --new_lowest;
      highest_empty = new_lowest->first;
    }

    entries_t new_entries;

    if ((es.end() - 1)->first < (entries_.end() - 1)->first) {
      lderr(cct) << __PRETTY_FUNCTION__ << ":" << __LINE__
		 << ": INCONSISTENCY! Head moved wrong way." << dendl;
      return -EFAULT;
    }

    if ((es.end() - 1)->first > (entries_.end() - 1)->first) {
      auto ei = es.lower_bound((entries_.end() - 1)->first + 1);
      std::copy(ei, es.end(), std::inserter(new_entries, new_entries.end()));
    }

    // Everything checks out!

    version = v;
    entries_ = es;
    l.unlock();

    if (highest_empty) {
      int r = handle_empty_to(*highest_empty);
      if (r < 0)
	return r;
    }

    if (!new_entries.empty()) {
      int r = handle_new_gens(std::move(new_entries));
      if (r < 0)
	return r;
    }
  } catch (const std::bad_alloc&) {
    return -ENOMEM;
  }
  return 0; 
}

auto logback_generations::read(optional_yield y) noexcept ->
  tl::expected<std::pair<entries_t, obj_version>, int>
{
  try {
    auto cct = static_cast<CephContext*>(ioctx.cct());
    librados::ObjectReadOperation op;
    std::unique_lock l(m);
    cls_version_check(op, version, VER_COND_GE);
    l.unlock();
    obj_version v2;
    cls_version_read(op, &v2);
    cb::list bl;
    op.read(0, 0, &bl, nullptr);
    auto r = rgw_rados_operate(ioctx, oid, &op, nullptr, y);
    if (r < 0) {
      if (r == -ENOENT) {
	ldout(cct, 5) << __PRETTY_FUNCTION__ << ":" << __LINE__
		      << ": oid=" << oid
		      << " not found" << dendl;
      } else {
	lderr(cct) << __PRETTY_FUNCTION__ << ":" << __LINE__
		   << ": failed reading oid=" << oid
		   << ", r=" << r << dendl;
      }
      return tl::unexpected(r);
    }
    auto bi = bl.cbegin();
    entries_t e;
    try {
      decode(e, bi);
    } catch (const cb::error& err) {
      return tl::unexpected(-EIO);
    }
    return std::pair{ std::move(e), std::move(v2) };
  } catch (const std::bad_alloc&) {
    return tl::unexpected(-ENOMEM);
  }
}

int logback_generations::write(entries_t&& e,
			       std::unique_lock<std::mutex>&& l_,
			       optional_yield y) noexcept
{
  auto l = std::move(l_);
  ceph_assert(l.mutex() == &m &&
	      l.owns_lock());
  try {
    auto cct = static_cast<CephContext*>(ioctx.cct());
    librados::ObjectWriteOperation op;
    cls_version_check(op, version, VER_COND_GE);
    cb::list bl;
    encode(e, bl);
    op.write_full(bl);
    cls_version_inc(op);
    auto r = rgw_rados_operate(ioctx, oid, &op, y);
    if (r == 0) {
      entries_ = std::move(e);
      version.inc();
      return 0;
    }
    l.unlock();
    if (r < 0 && r != -ECANCELED) {
      lderr(cct) << __PRETTY_FUNCTION__ << ":" << __LINE__
		 << ": failed reading oid=" << oid
		 << ", r=" << r << dendl;
      return r;
    }
    if (r == -ECANCELED) {
      r = update(y);
      if (r < 0) {
	return r;
      } else {
	return -ECANCELED;
      }
    }
  } catch (const std::bad_alloc&) {
    return -ENOMEM;
  }
  return 0;
}


int logback_generations::watch() noexcept {
  try {
    auto cct = static_cast<CephContext*>(ioctx.cct());
    auto r = ioctx.watch2(oid, &watchcookie, this);
    if (r < 0) {
      lderr(cct) << __PRETTY_FUNCTION__ << ":" << __LINE__
		 << ": failed to set watch oid=" << oid
		 << ", r=" << r << dendl;
      return r;
    }
  } catch (const std::bad_alloc&) {
    return -ENOMEM;
  }
  return 0;
}

int logback_generations::new_backing(log_type type,
				     optional_yield y) noexcept {
  auto cct = static_cast<CephContext*>(ioctx.cct());
  static constexpr auto max_tries = 10;
  int r = 0;
  try {
    r = update(y);
    if (r < 0)
      return r;
    auto tries = 0;
    entries_t new_entries;
    do {
      std::unique_lock l(m);
      auto last = entries_.end() - 1;
      if (last->second.type == type) {
	// Nothing to be done
	return 0;
      }
      auto newgenid = last->first + 1;
      logback_generation newgen;
      newgen.gen_id = newgenid;
      newgen.type = type;
      new_entries.emplace(newgenid, newgen);
      auto es = entries_;
      es.emplace(newgenid, std::move(newgen));
      r = write(std::move(es), std::move(l), y);
      ++tries;
    } while (r == -ECANCELED && tries < max_tries);
    if (tries >= max_tries) {
      lderr(cct) << __PRETTY_FUNCTION__ << ":" << __LINE__
		 << ": exhausted retry attempts." << dendl;
      return r;
    }

    if (r < 0) {
      lderr(cct) << __PRETTY_FUNCTION__ << ":" << __LINE__
		 << ": write failed with r=" << r << dendl;
      return r;
    }

    cb::list bl, rbl;

    r = rgw_rados_notify(ioctx, oid, bl, 10'000, &rbl, y);
    if (r < 0) {
      lderr(cct) << __PRETTY_FUNCTION__ << ":" << __LINE__
		 << ": notify failed with r=" << r << dendl;
      return r;
    }
    r = handle_new_gens(new_entries);
  } catch (const std::bad_alloc&) {
    return -ENOMEM;
  }
  return 0;
}

int logback_generations::empty_to(uint64_t gen_id, optional_yield y) noexcept {
  auto cct = static_cast<CephContext*>(ioctx.cct());
  static constexpr auto max_tries = 10;
  int r = 0;
  try {
    r = update(y);
    if (r < 0) return r;
    auto tries = 0;
    uint64_t newtail = 0;
    do {
      std::unique_lock l(m);
      {
	auto last = entries_.end() - 1;
	if (gen_id >= last->first) {
	  lderr(cct) << __PRETTY_FUNCTION__ << ":" << __LINE__
		     << ": Attempt to trim beyond the possible." << dendl;
	  return -EINVAL;
	}
      }
      auto es = entries_;
      auto ei = es.upper_bound(gen_id);
      if (ei == es.begin()) {
	// Nothing to be done.
	return 0;
      }
      for (auto i = es.begin(); i < ei; ++i) {
	newtail = i->first;
	i->second.pruned = ceph::real_clock::now();
      }
      r = write(std::move(es), std::move(l), y);
      ++tries;
    } while (r == -ECANCELED && tries < max_tries);
    if (tries >= max_tries) {
      lderr(cct) << __PRETTY_FUNCTION__ << ":" << __LINE__
		 << ": exhausted retry attempts." << dendl;
      return r;
    }

    if (r < 0) {
      lderr(cct) << __PRETTY_FUNCTION__ << ":" << __LINE__
		 << ": write failed with r=" << r << dendl;
      return r;
    }

    cb::list bl, rbl;

    r = rgw_rados_notify(ioctx, oid, bl, 10'000, &rbl, y);
    if (r < 0) {
      lderr(cct) << __PRETTY_FUNCTION__ << ":" << __LINE__
		 << ": notify failed with r=" << r << dendl;
      return r;
    }
    r = handle_empty_to(newtail);
  } catch (const std::bad_alloc&) {
    return -ENOMEM;
  }
  return r;
}

int logback_generations::remove_empty(optional_yield y) noexcept {
  auto cct = static_cast<CephContext*>(ioctx.cct());
  static constexpr auto max_tries = 10;
  int r = 0;
  try {
    r = update(y);
    if (r < 0)
      return r;
    auto tries = 0;
    entries_t new_entries;
    std::unique_lock l(m);
    ceph_assert(!entries_.empty());
    {
      auto i = lowest_nomempty(entries_);
      if (i == entries_.begin()) {
	return 0;
      }
    }
    entries_t es;
    auto now = ceph::real_clock::now();
    l.unlock();
    do {
      std::copy_if(entries_.cbegin(), entries_.cend(),
		   std::inserter(es, es.end()),
		   [now](const auto& e) {
		     if (!e.second.pruned)
		       return false;

		     auto pruned = *e.second.pruned;
		     return (now - pruned) >= 1h;
		   });
      auto es2 = entries_;
      for (const auto& [gen_id, e] : es) {
	ceph_assert(e.pruned);
	r = log_remove(ioctx, shards,
		       [this, gen_id = gen_id](int shard) {
			 return this->get_oid(gen_id, shard);
		       }, (gen_id == 0), y);
	if (r) {
	  lderr(cct) << __PRETTY_FUNCTION__ << ":" << __LINE__
		     << ": Error pruning: gen_id=" << gen_id
		     << " r=" << r << dendl;
	}
	if (auto i = es2.find(gen_id); i != es2.end()) {
	  es2.erase(i);
	}
      }
      l.lock();
      es.clear();
      r = write(std::move(es2), std::move(l), y);
      ++tries;
    } while (r == -ECANCELED && tries < max_tries);
    if (tries >= max_tries) {
      lderr(cct) << __PRETTY_FUNCTION__ << ":" << __LINE__
		 << ": exhausted retry attempts." << dendl;
      return r;
    }

    if (r < 0) {
      lderr(cct) << __PRETTY_FUNCTION__ << ":" << __LINE__
		 << ": write failed with r=" << r << dendl;
      return r;
    }
  } catch (const std::bad_alloc&) {
    return -ENOMEM;
  }
  return 0;
}

void logback_generations::handle_notify(uint64_t notify_id,
					uint64_t cookie,
					uint64_t notifier_id,
					bufferlist& bl)
{
  auto cct = static_cast<CephContext*>(ioctx.cct());
  int r = 0;
  if (notifier_id != my_id) {
    r = update(null_yield);
    if (r < 0) {
      lderr(cct)
	<< __PRETTY_FUNCTION__ << ":" << __LINE__
	<< ": update failed, no one to report to and no safe way to continue."
	<< dendl;
      abort();
    }
  }
  cb::list rbl;
  ioctx.notify_ack(oid, notify_id, watchcookie, rbl);
}

void logback_generations::handle_error(uint64_t cookie, int err) {
  auto cct = static_cast<CephContext*>(ioctx.cct());
  auto r = ioctx.unwatch2(watchcookie);
  if (r < 0) {
    lderr(cct) << __PRETTY_FUNCTION__ << ":" << __LINE__
	       << ": failed to set unwatch oid=" << oid
	       << ", r=" << r << dendl;
  }

  r = watch();
  if (r < 0) {
    lderr(cct) << __PRETTY_FUNCTION__ << ":" << __LINE__
	       << ": failed to re-establish watch, unsafe to continue: oid="
	       << oid << ", r=" << r << dendl;
  }
}
