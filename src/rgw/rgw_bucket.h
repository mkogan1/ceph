// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RGW_BUCKET_H
#define CEPH_RGW_BUCKET_H

#include <string>
#include <memory>

#include "include/types.h"
#include "rgw_common.h"
#include "rgw_tools.h"

#include "rgw_rados.h"

#include "rgw_string.h"

#include "common/Formatter.h"
#include "common/lru_map.h"
#include "common/ceph_time.h"
#include "rgw_formats.h"


// define as static when RGWBucket implementation completes
extern void rgw_get_buckets_obj(const rgw_user& user_id, string& buckets_obj_id);

extern int rgw_bucket_store_info(RGWRados *store, const string& bucket_name, bufferlist& bl, bool exclusive,
                                 map<string, bufferlist> *pattrs, RGWObjVersionTracker *objv_tracker,
                                 real_time mtime);
extern int rgw_bucket_instance_store_info(RGWRados *store, string& oid, bufferlist& bl, bool exclusive,
                                 map<string, bufferlist> *pattrs, RGWObjVersionTracker *objv_tracker,
                                 real_time mtime);

extern int rgw_bucket_parse_bucket_instance(const string& bucket_instance, string *target_bucket_instance, int *shard_id);
extern int rgw_bucket_parse_bucket_key(CephContext *cct, const string& key,
                                       rgw_bucket* bucket, int *shard_id);

extern int rgw_bucket_instance_remove_entry(RGWRados *store, const string& entry,
					    RGWObjVersionTracker *objv_tracker);
extern void rgw_bucket_instance_key_to_oid(string& key);
extern void rgw_bucket_instance_oid_to_key(string& oid);

extern int rgw_bucket_delete_bucket_obj(RGWRados *store,
                                        const string& tenant_name,
                                        const string& bucket_name,
                                        RGWObjVersionTracker& objv_tracker);

extern int rgw_bucket_sync_user_stats(RGWRados *store, const rgw_user& user_id,
                                      const RGWBucketInfo& bucket_info,
                                      RGWBucketEnt* pent);
extern int rgw_bucket_sync_user_stats(RGWRados *store, const string& tenant_name, const string& bucket_name);

extern std::string rgw_make_bucket_entry_name(const std::string& tenant_name,
                                              const std::string& bucket_name);
static inline void rgw_make_bucket_entry_name(const string& tenant_name,
                                              const string& bucket_name,
                                              std::string& bucket_entry) {
  bucket_entry = rgw_make_bucket_entry_name(tenant_name, bucket_name);
}

extern void rgw_parse_url_bucket(const string& bucket,
                                 const string& auth_tenant,
                                 string &tenant_name, string &bucket_name);

struct RGWBucketCompleteInfo {
  RGWBucketInfo info;
  map<string, bufferlist> attrs;

  void dump(Formatter *f) const;
  void decode_json(JSONObj *obj);
};

class RGWBucketEntryMetadataObject : public RGWMetadataObject {
  RGWBucketEntryPoint ep;
public:
  RGWBucketEntryMetadataObject(RGWBucketEntryPoint& _ep, obj_version& v, real_time m) : ep(_ep) {
    objv = v;
    mtime = m;
  }

  void dump(Formatter *f) const override {
    ep.dump(f);
  }
};

class RGWBucketInstanceMetadataObject : public RGWMetadataObject {
  RGWBucketCompleteInfo info;
public:
  RGWBucketInstanceMetadataObject() {}
  RGWBucketInstanceMetadataObject(RGWBucketCompleteInfo& i, obj_version& v, real_time m) : info(i) {
    objv = v;
    mtime = m;
  }

  void dump(Formatter *f) const override {
    info.dump(f);
  }

  void decode_json(JSONObj *obj) {
    info.decode_json(obj);
  }

  RGWBucketInfo& get_bucket_info() { return info.info; }
};

/**
 * Store a list of the user's buckets, with associated functinos.
 */
class RGWUserBuckets
{
  std::map<std::string, RGWBucketEnt> buckets;

public:
  RGWUserBuckets() = default;
  RGWUserBuckets(RGWUserBuckets&&) = default;

  RGWUserBuckets& operator=(const RGWUserBuckets&) = default;

  void encode(bufferlist& bl) const {
    using ceph::encode;
    encode(buckets, bl);
  }
  void decode(bufferlist::const_iterator& bl) {
    using ceph::decode;
    decode(buckets, bl);
  }
  /**
   * Check if the user owns a bucket by the given name.
   */
  bool owns(string& name) {
    map<string, RGWBucketEnt>::iterator iter;
    iter = buckets.find(name);
    return (iter != buckets.end());
  }

  /**
   * Add a (created) bucket to the user's bucket list.
   */
  void add(const RGWBucketEnt& bucket) {
    buckets[bucket.bucket.name] = bucket;
  }

  /**
   * Remove a bucket from the user's list by name.
   */
  void remove(const string& name) {
    map<string, RGWBucketEnt>::iterator iter;
    iter = buckets.find(name);
    if (iter != buckets.end()) {
      buckets.erase(iter);
    }
  }

  /**
   * Get the user's buckets as a map.
   */
  map<string, RGWBucketEnt>& get_buckets() { return buckets; }

  /**
   * Cleanup data structure
   */
  void clear() { buckets.clear(); }

  size_t count() { return buckets.size(); }
};
WRITE_CLASS_ENCODER(RGWUserBuckets)

class RGWMetadataManager;
class RGWMetadataHandler;

class RGWBucketMetaHandlerAllocator {
public:
  static RGWMetadataHandler *alloc();
};

class RGWBucketInstanceMetaHandlerAllocator {
public:
  static RGWMetadataHandler *alloc();
};

class RGWArchiveBucketMetaHandlerAllocator {
public:
  static RGWMetadataHandler *alloc();
};

class RGWArchiveBucketInstanceMetaHandlerAllocator {
public:
  static RGWMetadataHandler *alloc();
};

extern void rgw_bucket_init(RGWMetadataManager *mm);
/**
 * Get all the buckets owned by a user and fill up an RGWUserBuckets with them.
 * Returns: 0 on success, -ERR# on failure.
 */
extern int rgw_read_user_buckets(RGWRados *store,
                                 const rgw_user& user_id,
                                 RGWUserBuckets& buckets,
                                 const string& marker,
                                 const string& end_marker,
                                 uint64_t max,
                                 bool need_stats,
				 bool* is_truncated,
                                 uint64_t default_amount = 1000);

struct rgw_ep_info {
  RGWBucketEntryPoint &ep;
  map<string, bufferlist>& attrs;
  RGWObjVersionTracker ep_objv;
  rgw_ep_info(RGWBucketEntryPoint &ep, map<string, bufferlist>& attrs)
    : ep(ep), attrs(attrs) { }
};

extern int rgw_link_bucket(RGWRados* store,
                           const rgw_user& user_id,
                           rgw_bucket& bucket,
                           ceph::real_time creation_time,
                           bool update_entrypoint = true,
                           rgw_ep_info *pinfo = nullptr);
extern int rgw_unlink_bucket(RGWRados *store, const rgw_user& user_id,
                             const string& tenant_name, const string& bucket_name, bool update_entrypoint = true);
extern int rgw_bucket_chown(RGWRados* const store, RGWBucketInfo& bucket_info,
                            const rgw_user& uid, const std::string& display_name,
                            const string& marker);
extern int rgw_set_bucket_acl(RGWRados* store, ACLOwner& owner, rgw_bucket& bucket,
                              RGWBucketInfo& bucket_info, bufferlist& bl);
extern int rgw_remove_object(RGWRados *store, const RGWBucketInfo& bucket_info, const rgw_bucket& bucket, rgw_obj_key& key);
extern int rgw_remove_bucket(RGWRados *store, rgw_bucket& bucket, bool delete_children);
extern int rgw_remove_bucket_bypass_gc(RGWRados *store, rgw_bucket& bucket, int concurrent_max);

extern int rgw_bucket_set_attrs(RGWRados *store, RGWBucketInfo& bucket_info,
                                map<string, bufferlist>& attrs,
                                RGWObjVersionTracker *objv_tracker);
extern int rgw_object_get_attr(RGWRados* store, const RGWBucketInfo& bucket_info,
                               const rgw_obj& obj, const char* attr_name,
                               bufferlist& out_bl);

extern void check_bad_user_bucket_mapping(RGWRados *store, const rgw_user& user_id, bool fix);

struct RGWBucketAdminOpState {
  rgw_user uid;
  std::string display_name;
  std::string bucket_name;
  std::string bucket_id;
  std::string object_name;
  std::string new_bucket_name;

  bool list_buckets;
  bool stat_buckets;
  bool check_objects;
  bool fix_index;
  bool delete_child_objects;
  bool bucket_stored;
  int max_aio = 0;

  rgw_bucket bucket;

  RGWQuotaInfo quota;

  void set_fetch_stats(bool value) { stat_buckets = value; }
  void set_check_objects(bool value) { check_objects = value; }
  void set_fix_index(bool value) { fix_index = value; }
  void set_delete_children(bool value) { delete_child_objects = value; }

  void set_max_aio(int value) { max_aio = value; }

  void set_user_id(const rgw_user& user_id) {
    if (!user_id.empty())
      uid = user_id;
  }
  void set_tenant(const std::string& tenant_str) {
    uid.tenant = tenant_str;
  }
  void set_bucket_name(const std::string& bucket_str) {
    bucket_name = bucket_str; 
  }
  void set_object(std::string& object_str) {
    object_name = object_str;
  }
  void set_new_bucket_name(std::string& new_bucket_str) {
    new_bucket_name = new_bucket_str;
  }
  void set_quota(RGWQuotaInfo& value) {
    quota = value;
  }


  rgw_user& get_user_id() { return uid; }
  std::string& get_user_display_name() { return display_name; }
  std::string& get_bucket_name() { return bucket_name; }
  std::string& get_object_name() { return object_name; }
  std::string& get_tenant() { return uid.tenant; }

  rgw_bucket& get_bucket() { return bucket; }
  void set_bucket(rgw_bucket& _bucket) {
    bucket = _bucket; 
    bucket_stored = true;
  }

  void set_bucket_id(const string& bi) {
    bucket_id = bi;
  }
  const string& get_bucket_id() { return bucket_id; }

  bool will_fetch_stats() { return stat_buckets; }
  bool will_fix_index() { return fix_index; }
  bool will_delete_children() { return delete_child_objects; }
  bool will_check_objects() { return check_objects; }
  bool is_user_op() { return !uid.empty(); }
  bool is_system_op() { return uid.empty(); }
  bool has_bucket_stored() { return bucket_stored; }
  int get_max_aio() { return max_aio; }

  RGWBucketAdminOpState() : list_buckets(false), stat_buckets(false), check_objects(false), 
                            fix_index(false), delete_child_objects(false),
                            bucket_stored(false)  {}
};

/*
 * A simple wrapper class for administrative bucket operations
 */

class RGWBucket
{
  RGWUserBuckets buckets;
  RGWRados *store;
  RGWAccessHandle handle;

  RGWUserInfo user_info;
  std::string tenant;
  std::string bucket_name;

  bool failure;

  RGWBucketInfo bucket_info;

public:
  RGWBucket() : store(NULL), handle(NULL), failure(false) {}
  int init(RGWRados *storage, RGWBucketAdminOpState& op_state,
              std::string *err_msg = NULL, map<string, bufferlist> *pattrs = NULL);

  int check_bad_index_multipart(RGWBucketAdminOpState& op_state,
              RGWFormatterFlusher& flusher, std::string *err_msg = NULL);

  int check_object_index(RGWBucketAdminOpState& op_state,
                         RGWFormatterFlusher& flusher,
                         std::string *err_msg = NULL);

  int check_index(RGWBucketAdminOpState& op_state,
          map<RGWObjCategory, RGWStorageStats>& existing_stats,
          map<RGWObjCategory, RGWStorageStats>& calculated_stats,
          std::string *err_msg = NULL);

  int remove(RGWBucketAdminOpState& op_state, bool bypass_gc = false, bool keep_index_consistent = true, std::string *err_msg = NULL);
  int link(RGWBucketAdminOpState& op_state, map<string, bufferlist>& attrs,
	std::string *err_msg = NULL);
  int chown(RGWBucketAdminOpState& op_state, const string& marker, std::string *err_msg = NULL);
  int unlink(RGWBucketAdminOpState& op_state, std::string *err_msg = NULL);
  int set_quota(RGWBucketAdminOpState& op_state, std::string *err_msg = NULL);

  int remove_object(RGWBucketAdminOpState& op_state, std::string *err_msg = NULL);
  int policy_bl_to_stream(bufferlist& bl, ostream& o);
  int get_policy(RGWBucketAdminOpState& op_state, RGWAccessControlPolicy& policy);

  void clear_failure() { failure = false; }

  const RGWBucketInfo& get_bucket_info() const { return bucket_info; }
};

class RGWBucketAdminOp
{
public:
  static int get_policy(RGWRados *store, RGWBucketAdminOpState& op_state,
                  RGWFormatterFlusher& flusher);
  static int get_policy(RGWRados *store, RGWBucketAdminOpState& op_state,
                  RGWAccessControlPolicy& policy);
  static int dump_s3_policy(RGWRados *store, RGWBucketAdminOpState& op_state,
                  ostream& os);

  static int unlink(RGWRados *store, RGWBucketAdminOpState& op_state);
  static int link(RGWRados *store, RGWBucketAdminOpState& op_state, string *err_msg = NULL);
  static int chown(RGWRados *store, RGWBucketAdminOpState& op_state, const string& marker, string *err_msg = NULL);

  static int check_index(RGWRados *store, RGWBucketAdminOpState& op_state,
                  RGWFormatterFlusher& flusher);

  static int remove_bucket(RGWRados *store, RGWBucketAdminOpState& op_state, bool bypass_gc = false, bool keep_index_consistent = true);
  static int remove_object(RGWRados *store, RGWBucketAdminOpState& op_state);
  static int info(RGWRados *store, RGWBucketAdminOpState& op_state, RGWFormatterFlusher& flusher);
  static int limit_check(RGWRados *store, RGWBucketAdminOpState& op_state,
			 const std::list<std::string>& user_ids,
			 RGWFormatterFlusher& flusher,
			 bool warnings_only = false);
  static int set_quota(RGWRados *store, RGWBucketAdminOpState& op_state);

  static int list_stale_instances(RGWRados *store, RGWBucketAdminOpState& op_state,
				  RGWFormatterFlusher& flusher);

  static int clear_stale_instances(RGWRados *store, RGWBucketAdminOpState& op_state,
				   RGWFormatterFlusher& flusher);
  static int fix_lc_shards(RGWRados *store, RGWBucketAdminOpState& op_state,
                           RGWFormatterFlusher& flusher);
  static int fix_obj_expiry(RGWRados *store, RGWBucketAdminOpState& op_state,
			    RGWFormatterFlusher& flusher, bool dry_run = false);
};

bool rgw_find_bucket_by_id(CephContext *cct, RGWMetadataManager *mgr, const string& marker,
                           const string& bucket_id, rgw_bucket* bucket_out);

#endif
