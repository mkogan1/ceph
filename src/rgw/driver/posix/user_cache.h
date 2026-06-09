// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright contributors to the Ceph project
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#pragma once

#include <shared_mutex>
#include <string>
#include <unordered_map>

#include "common/dout.h"
#include "rgw_common.h"
#include "rgw_sal_fwd.h"

#define dout_subsys ceph_subsys_rgw

namespace rgw::sal {

struct UserCacheEntry {
  RGWUserInfo info;
  Attrs attrs;
  RGWObjVersionTracker objv_tracker;
};

class UserCache {
  mutable std::shared_mutex mtx;
  std::unordered_map<std::string, UserCacheEntry> by_id;
  std::unordered_map<std::string, std::string> ak_to_id;

public:
  void insert_user(const DoutPrefixProvider* dpp, const UserCacheEntry& entry) {
    ldpp_dout(dpp, 30) << "UserCache: caching user uid=" << entry.info.user_id.id << dendl;
    std::unique_lock wl{mtx};
    const auto& uid = entry.info.user_id.id;
    by_id[uid] = entry;
    for (const auto& [ak, _] : entry.info.access_keys) {
      ak_to_id[ak] = uid;
    }
  }

  bool lookup_user_by_uid(const DoutPrefixProvider* dpp, const std::string& user_id,
              UserCacheEntry& out) const {
    std::shared_lock rl{mtx};
    auto it = by_id.find(user_id);
    if (it == by_id.end()) {
      ldpp_dout(dpp, 30) << "UserCache: lookup cached user by uid=" << user_id << " : not found" << dendl;
      return false;
    }
    out = it->second;
    ldpp_dout(dpp, 30) << "UserCache: lookup cached user by uid=" << user_id << " : found" << dendl;
    return true;
  }

  bool lookup_user_by_access_key(const DoutPrefixProvider* dpp,
                            const std::string& key,
                            UserCacheEntry& out) const {
    std::shared_lock rl{mtx};
    auto it = ak_to_id.find(key);
    if (it == ak_to_id.end()) {
      ldpp_dout(dpp, 30) << "UserCache: lookup cached user by key=" << key << " : not found" << dendl;
      return false;
    }
    auto uid_it = by_id.find(it->second);
    if (uid_it == by_id.end()) {
      ldpp_dout(dpp, 30) << "UserCache: lookup cached user by key=" << key << ": uid=" << it->second << " : not found" << dendl;
      return false;
    }
    out = uid_it->second;
    ldpp_dout(dpp, 30) << "UserCache: lookup cached user by key=" << key << ": uid=" << it->second << " : found" << dendl;
    return true;
  }

  void invalidate_user(const DoutPrefixProvider* dpp, const std::string& user_id) {
    ldpp_dout(dpp, 30) << "UserCache: invalidating cached user uid=" << user_id << dendl;
    std::unique_lock wl{mtx};
    auto it = by_id.find(user_id);
    if (it != by_id.end()) {
      for (const auto& [ak, _] : it->second.info.access_keys) {
        ak_to_id.erase(ak);
      }
      by_id.erase(it);
    }
  }
};

} // namespace rgw::sal

#undef dout_subsys
