// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "rgw_d3n_datacache.h"
#include "rgw_rest_client.h"
#include "rgw_auth_s3.h"
#include "rgw_op.h"
#include "rgw_common.h"
#include "rgw_auth_s3.h"
#include "rgw_op.h"
#include "rgw_crypt_sanitize.h"
#include "rgw_directory.h"

#if __has_include(<filesystem>)
#include <filesystem>
namespace efs = std::filesystem;
#else
#include <experimental/filesystem>
namespace efs = std::experimental::filesystem;
#endif

#define dout_subsys ceph_subsys_rgw

using namespace std;

int D3nCacheAioWriteRequest::d3n_prepare_libaio_write_op(bufferlist& bl, unsigned int len, string oid, string cache_location)
{
  std::string location = cache_location + oid;
  int r = 0;

  lsubdout(g_ceph_context, rgw_datacache, 20) << "D3nDataCache: " << __func__ << "(): Write To Cache, location=" << location << dendl;
  cb = new struct aiocb;
  mode_t mode = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;
  memset(cb, 0, sizeof(struct aiocb));
  r = fd = ::open(location.c_str(), O_WRONLY | O_CREAT | O_TRUNC, mode);
  if (fd < 0) {
    ldout(cct, 0) << "ERROR: D3nCacheAioWriteRequest::create_io: open file failed, errno=" << errno << ", location='" << location.c_str() << "'" << dendl;
    goto done;
  }
  if (g_conf()->rgw_d3n_l1_fadvise != POSIX_FADV_NORMAL)
    posix_fadvise(fd, 0, 0, g_conf()->rgw_d3n_l1_fadvise);
  cb->aio_fildes = fd;

  data = malloc(len);
  if (!data) {
    ldout(cct, 0) << "ERROR: D3nCacheAioWriteRequest::create_io: memory allocation failed" << dendl;
    goto close_file;
  }
  cb->aio_buf = data;
  memcpy((void*)data, bl.c_str(), len);
  cb->aio_nbytes = len;
  goto done;

close_file:
  ::close(fd);
done:
  return r;
}

D3nDataCache::D3nDataCache()
  : cct(nullptr), io_type(_io_type::ASYNC_IO), free_data_cache_size(0), outstanding_write_size(0)
{
  lsubdout(g_ceph_context, rgw_datacache, 5) << "D3nDataCache: " << __func__ << "()" << dendl;
}

void D3nDataCache::init(CephContext *_cct) {
  cct = _cct;
  free_data_cache_size = cct->_conf->rgw_d3n_l1_datacache_size;
  head = nullptr;
  tail = nullptr;
  cache_location = cct->_conf->rgw_d3n_l1_datacache_persistent_path;
  if(cache_location.back() != '/') {
      cache_location += "/";
  }
  try {
    if (efs::exists(cache_location)) {
      // d3n: evict the cache storage directory
      if (g_conf()->rgw_d3n_l1_evict_cache_on_start) {
        lsubdout(g_ceph_context, rgw, 5) << "D3nDataCache: init: evicting the persistent storage directory on start" << dendl;
        for (auto& p : efs::directory_iterator(cache_location)) {
          efs::remove_all(p.path());
        }
      }
    } else {
      // create the cache storage directory
      lsubdout(g_ceph_context, rgw, 5) << "D3nDataCache: init: creating the persistent storage directory on start" << dendl;
      efs::create_directories(cache_location);
    }
  } catch (const efs::filesystem_error& e) {
    lderr(g_ceph_context) << "D3nDataCache: init: ERROR initializing the cache storage directory '" << cache_location <<
                              "' : " << e.what() << dendl;
  }

  auto conf_eviction_policy = cct->_conf.get_val<std::string>("rgw_d3n_l1_eviction_policy");
  ceph_assert(conf_eviction_policy == "lru" || conf_eviction_policy == "random");
  if (conf_eviction_policy == "lru")
    eviction_policy = _eviction_policy::LRU;
  if (conf_eviction_policy == "random")
    eviction_policy = _eviction_policy::RANDOM;

#if defined(HAVE_LIBAIO)
  // libaio setup
  struct aioinit ainit{0};
  ainit.aio_threads = cct->_conf.get_val<int64_t>("rgw_d3n_libaio_aio_threads");
  ainit.aio_num = cct->_conf.get_val<int64_t>("rgw_d3n_libaio_aio_num");
  ainit.aio_idle_time = 120;
  aio_init(&ainit);
#endif
}

int D3nDataCache::d3n_io_write(bufferlist& bl, unsigned int len, std::string oid)
{
  D3nChunkDataInfo* chunk_info = new D3nChunkDataInfo;
  std::string location = cache_location + oid;

  lsubdout(g_ceph_context, rgw_datacache, 20) << "D3nDataCache: " << __func__ << "(): location=" << location << dendl;
  FILE *cache_file = nullptr;
  int r = 0;
  size_t nbytes = 0;

  cache_file = fopen(location.c_str(), "w+");
  if (cache_file == nullptr) {
    ldout(cct, 0) << "ERROR: D3nDataCache::fopen file has return error, errno=" << errno << dendl;
    return -errno;
  }

  nbytes = fwrite(bl.c_str(), 1, len, cache_file);
  if (nbytes != len) {
    ldout(cct, 0) << "ERROR: D3nDataCache::io_write: fwrite has returned error: nbytes!=len, nbytes=" << nbytes << ", len=" << len << dendl;
    return -EIO;
  }

  r = fclose(cache_file);
  if (r != 0) {
    ldout(cct, 0) << "ERROR: D3nDataCache::fclsoe file has return error, errno=" << errno << dendl;
    return -errno;
  }

  { // update cahce_map entries for new chunk in cache
    const std::lock_guard l(d3n_cache_lock);
    chunk_info->oid = oid;
    chunk_info->set_ctx(cct);
    chunk_info->size = len;
    d3n_cache_map.insert(pair<string, D3nChunkDataInfo*>(oid, chunk_info));
  }

  return r;
}

void d3n_libaio_write_cb(sigval sigval)
{
  lsubdout(g_ceph_context, rgw_datacache, 30) << "D3nDataCache: " << __func__ << "()" << dendl;
  D3nCacheAioWriteRequest* c = static_cast<D3nCacheAioWriteRequest*>(sigval.sival_ptr);
  c->priv_data->d3n_libaio_write_completion_cb(c);
}


void D3nDataCache::d3n_libaio_write_completion_cb(D3nCacheAioWriteRequest* c)
{
  D3nChunkDataInfo* chunk_info{nullptr};

  ldout(cct, 5) << "D3nDataCache: " << __func__ << "(): oid=" << c->oid << dendl;

  { // update cache_map entries for new chunk in cache
    const std::lock_guard l(d3n_cache_lock);
    d3n_outstanding_write_list.erase(c->oid);
    chunk_info = new D3nChunkDataInfo;
    chunk_info->oid = c->oid;
    chunk_info->set_ctx(cct);
    chunk_info->size = c->cb->aio_nbytes;
    d3n_cache_map.insert(pair<string, D3nChunkDataInfo*>(c->oid, chunk_info));
  }

  { // update free size
    const std::lock_guard l(d3n_eviction_lock);
    free_data_cache_size -= c->cb->aio_nbytes;
    outstanding_write_size -= c->cb->aio_nbytes;
    lru_insert_head(chunk_info);
  }

  // EC528 Fall 2021 Mandy Edit:
  // cache_block *cachePtr = new cache_block; // free memory later
  // // "Mandy", "myBucket", "obj", NULL, 10, time(0), "eTag", NULL, NULL, NULL, NULL, NULL, NULL, false, false, NULL, 0, false, false

  // cache_obj objMandy;
  // uint64_t offset_temp = 0;
  // uint64_t block_id_temp = 1;
  // uint64_t size_in_bytes_temp = 20;
  // uint64_t access_count_temp = 5;
  // time_t lastAccessTime_temp = 7;
  // bool cachedOnRemote_temp = false;

  // cachePtr->c_obj = objMandy;
  // //ldout(cct, 5) << "Mandy - cachePtr c_obj: " << cachePtr->c_obj << dendl;
  // cachePtr->offset = offset_temp;
  // // ldout(cct, 5) << "Mandy - cachePtr offset: " << cachePtr->offset << dendl;
  // cachePtr->block_id = block_id_temp;
  // // ldout(cct, 5) << "Mandy - cachePtr block_id: " << cachePtr->block_id << dendl;
  // cachePtr->size_in_bytes = size_in_bytes_temp;
  // // ldout(cct, 5) << "Mandy - cachePtr size_in_bytes: " << cachePtr->size_in_bytes << dendl;
  // cachePtr->etag = "Mandy_etag1";
  // // ldout(cct, 5) << "Mano u dy - cachePtr etag: " << cachePtr->etag << dendl;
  // cachePtr->hosts_list = vector<string>();
  // // ldout(cct, 5) << "Mandy - cachePtr hosts_list: " << cachePtr->hosts_list << dendl;
  // cachePtr->access_count = access_count_temp;
  // // ldout(cct, 5) << "Mandy - cachePtr access_count: " << cachePtr->access_count << dendl;
  // cachePtr->lastAccessTime = lastAccessTime_temp;
  // // ldout(cct, 5) << "Mandy - cachePtr lastAccessTime: " << cachePtr->lastAccessTime << dendl;
  // cachePtr->cachedOnRemote = cachedOnRemote_temp;
  // // ldout(cct, 5) << "Mandy - cachePtr cachedOnRemote: " << cachePtr->cachedOnRemote << dendl;

  // RGWBlockDirectory rgwVar;
  // int rgwSetValue = rgwVar.setValue(cachePtr);
  // ldout(cct, 5) << "Mandy: setValue: " << rgwSetValue << dendl;
  // int rgwOutput = rgwVar.getValue(cachePtr);
  // ldout(cct, 5) << "Mandy: getValue: " << rgwOutput << dendl;

  // mandy comment out 
  // ldout(cct, 5) << "Mandy start" <<dendl;
  // uint64_t size_in_bytes_obj_temp = 20;
  // time_t lastAccessTime_obj_temp = time(NULL);
  // BackendProtocol backendProtocol_obj_temp = S3;
  // HomeLocation home_location_obj_temp = CACHE;
  // time_t aclTimeStamp_obj_temp = time(NULL);
  // time_t creationTime_obj_temp = time(NULL);
  // bool dirty_obj_temp = false;
  // bool intermediate_obj_temp = false;
  // uint64_t offset_obj_temp = 0;
  // bool is_remote_req_obj_temp = false;
  // bool is_remote_req_put_obj_temp = false; 

  // cache_obj* mandyCacheObj;
  // mandyCacheObj = new cache_obj();
  // //mandyCacheObj->init(cct);
  // mandyCacheObj->owner = "Mandy";
  // ldout(cct, 5) << "mandy owner: " <<  mandyCacheObj->owner << dendl;
  // mandyCacheObj->bucket_name = "Mandy_owner";
  // mandyCacheObj->obj_name = "Mandy_obj_name";
  // mandyCacheObj->size_in_bytes = size_in_bytes_obj_temp;
  // ldout(cct, 5) << "mandy owner: " <<  mandyCacheObj->size_in_bytes << dendl;
  // mandyCacheObj->lastAccessTime = mktime(gmtime(&lastAccessTime_obj_temp));
  // mandyCacheObj->etag = "Mandy_etag";
  // mandyCacheObj->backendProtocol = backendProtocol_obj_temp;
  // mandyCacheObj->home_location = home_location_obj_temp;
  // mandyCacheObj->hosts_list = vector<string>("mandy", "sam"); // double check syntax 
  // mandyCacheObj->acl = "Mandy_acl";
  // mandyCacheObj->aclTimeStamp = mktime(gmtime(&aclTimeStamp_obj_temp));
  // mandyCacheObj->creationTime = mktime(gmtime(&creationTime_obj_temp));
  // mandyCacheObj->dirty = dirty_obj_temp;
  // mandyCacheObj->intermediate = intermediate_obj_temp;
  // mandyCacheObj->mapping_id = "Mandy_mapping_id";
  // mandyCacheObj->offset = offset_obj_temp;
  // mandyCacheObj->is_remote_req = is_remote_req_obj_temp;
  // mandyCacheObj->is_remote_req_put = is_remote_req_put_obj_temp;

  // uint64_t offset_temp = 0;
  // uint64_t block_id_temp = 1;
  // uint64_t size_in_bytes_temp = 4;
  // uint64_t access_count_temp = 5;
  // time_t lastAccessTime_temp = time(NULL);
  // bool cachedOnRemote_temp = false;

  // cache_block* mandyCacheBlock;
  // mandyCacheBlock = new cache_block();
  // //mandyCacheBlock->init(cct);
  // mandyCacheBlock->c_obj = *mandyCacheObj;
  // mandyCacheBlock->offset = offset_temp;
  // mandyCacheBlock->block_id = block_id_temp;
  // mandyCacheBlock->size_in_bytes = size_in_bytes_temp;
  // mandyCacheBlock->etag = "Mandy_etag1";
  // mandyCacheBlock->hosts_list = vector<string>("mandy", "sam");
  // mandyCacheBlock->access_count = access_count_temp;
  // mandyCacheBlock->lastAccessTime = mktime(gmtime(&lastAccessTime_temp));
  // mandyCacheBlock->cachedOnRemote = cachedOnRemote_temp;

  ldout(cct, 5) << "Mandy start" <<dendl;
  uint64_t size_in_bytes_obj_temp = 20;
  time_t lastAccessTime_obj_temp = time(NULL);
  BackendProtocol backendProtocol_obj_temp = S3;
  HomeLocation home_location_obj_temp = CACHE;
  time_t aclTimeStamp_obj_temp = time(NULL);
  time_t creationTime_obj_temp = time(NULL);
  bool dirty_obj_temp = false;
  bool intermediate_obj_temp = false;
  uint64_t offset_obj_temp = 0;
  bool is_remote_req_obj_temp = false;
  bool is_remote_req_put_obj_temp = false; 

  ldout(cct, 5) << "Mandy before mandyCacheObj" <<dendl;
  cache_obj mandyCacheObj;
  //mandyCacheObj->init(cct);
  mandyCacheObj.owner = "Mandy";
  ldout(cct, 5) << "mandy owner: " <<  mandyCacheObj.owner << dendl;
  mandyCacheObj.bucket_name = "Mandy_owner";
  ldout(cct, 5) << "mandy bucket_name: " <<  mandyCacheObj.bucket_name << dendl;
  mandyCacheObj.obj_name = "Mandy_obj_name";
  ldout(cct, 5) << "mandy obj_name: " <<  mandyCacheObj.obj_name << dendl;
  mandyCacheObj.size_in_bytes = size_in_bytes_obj_temp;
  ldout(cct, 5) << "mandy size_in_bytes: " <<  mandyCacheObj.size_in_bytes << dendl;
  mandyCacheObj.lastAccessTime = mktime(gmtime(&lastAccessTime_obj_temp));
  ldout(cct, 5) << "mandy lastAccessTime: " <<  mandyCacheObj.lastAccessTime << dendl;
  mandyCacheObj.etag = "Mandy_etag";
  ldout(cct, 5) << "mandy etag: " <<  mandyCacheObj.etag << dendl;
  mandyCacheObj.backendProtocol = backendProtocol_obj_temp;
  ldout(cct, 5) << "mandy backendProtocol: " <<  mandyCacheObj.backendProtocol << dendl;
  mandyCacheObj.home_location = home_location_obj_temp;
  ldout(cct, 5) << "mandy home_location: " <<  mandyCacheObj.home_location << dendl;
  vector<string> mandyHostList{"mandy", "sam"}; 
  mandyCacheObj.hosts_list = mandyHostList; // double check syntax 
  ldout(cct, 5) << "mandy hosts_list: " <<  mandyCacheObj.hosts_list << dendl;
  mandyCacheObj.acl = "Mandy_acl";
  ldout(cct, 5) << "mandy acl: " <<  mandyCacheObj.acl << dendl;
  mandyCacheObj.aclTimeStamp = mktime(gmtime(&aclTimeStamp_obj_temp));
  ldout(cct, 5) << "mandy aclTimeStamp: " <<  mandyCacheObj.aclTimeStamp << dendl;
  mandyCacheObj.creationTime = mktime(gmtime(&creationTime_obj_temp));
  ldout(cct, 5) << "mandy creationTime: " <<  mandyCacheObj.creationTime << dendl;
  mandyCacheObj.dirty = dirty_obj_temp;
  ldout(cct, 5) << "mandy dirty: " <<  mandyCacheObj.dirty << dendl;
  mandyCacheObj.intermediate = intermediate_obj_temp;
  ldout(cct, 5) << "mandy intermediate: " <<  mandyCacheObj.intermediate << dendl;
  mandyCacheObj.mapping_id = "Mandy_mapping_id";
  ldout(cct, 5) << "mandy mapping_id: " <<  mandyCacheObj.mapping_id << dendl;
  mandyCacheObj.offset = offset_obj_temp;
  ldout(cct, 5) << "mandy offset: " <<  mandyCacheObj.offset << dendl;
  mandyCacheObj.is_remote_req = is_remote_req_obj_temp;
  ldout(cct, 5) << "mandy is_remote_req: " <<  mandyCacheObj.is_remote_req << dendl;
  mandyCacheObj.is_remote_req_put = is_remote_req_put_obj_temp;
  ldout(cct, 5) << "mandy is_remote_req_put: " <<  mandyCacheObj.is_remote_req_put << dendl;

  uint64_t offset_temp = 0;
  uint64_t block_id_temp = 1;
  uint64_t size_in_bytes_temp = 4;
  uint64_t access_count_temp = 5;
  time_t lastAccessTime_temp = time(NULL);
  bool cachedOnRemote_temp = false;

  ldout(cct, 5) << "Mandy before mandyCacheBlock" <<dendl;
  cache_block mandyCacheBlock;
  //mandyCacheBlock->init(cct);
  mandyCacheBlock.c_obj = mandyCacheObj;
 // ldout(cct, 5) << "mandy c_obj: " <<  mandyCacheBlock.c_obj << dendl;
  mandyCacheBlock.offset = offset_temp;
  ldout(cct, 5) << "mandy offset: " <<  mandyCacheBlock.offset << dendl;
  mandyCacheBlock.block_id = block_id_temp;
  ldout(cct, 5) << "mandy block_id: " <<  mandyCacheBlock.block_id << dendl;
  mandyCacheBlock.size_in_bytes = size_in_bytes_temp;
  ldout(cct, 5) << "mandy size_in_bytes: " <<  mandyCacheBlock.size_in_bytes << dendl;
  mandyCacheBlock.etag = "Mandy_etag1";
  ldout(cct, 5) << "mandy etag: " <<  mandyCacheBlock.etag << dendl;
  mandyCacheBlock.hosts_list = mandyHostList;
  ldout(cct, 5) << "mandy hosts_list: " <<  mandyCacheBlock.hosts_list << dendl;
  mandyCacheBlock.access_count = access_count_temp;
  ldout(cct, 5) << "mandy access_count: " <<  mandyCacheBlock.access_count << dendl;
  mandyCacheBlock.lastAccessTime = mktime(gmtime(&lastAccessTime_temp));
  ldout(cct, 5) << "mandy lastAccessTime: " <<  mandyCacheBlock.lastAccessTime << dendl;
  mandyCacheBlock.cachedOnRemote = cachedOnRemote_temp;
  ldout(cct, 5) << "mandy cachedOnRemote: " <<  mandyCacheBlock.cachedOnRemote << dendl;


  ldout(cct, 5) << "Mandy before mandyBlkDirectory" <<dendl;
  RGWBlockDirectory* mandyBlkDirectory = new RGWBlockDirectory();
  mandyBlkDirectory->init(cct);
  ldout(cct, 5) << "Mandy before calling set value for blk" << dendl;
  int rgwSetValue = mandyBlkDirectory->setValue(&mandyCacheBlock);
  ldout(cct, 5) << "Mandy: setValue: " << rgwSetValue << dendl;
  int rgwGetValue = mandyBlkDirectory->getValue(&mandyCacheBlock);
  ldout(cct, 5) << "Mandy: getValue: " << rgwGetValue << dendl;

  ldout(cct, 5) << "Mandy before mandyObjDirectory" << dendl;
  RGWObjectDirectory* mandyObjDirectory;
  mandyObjDirectory = new RGWObjectDirectory();
  mandyObjDirectory->init(cct);
  ldout(cct, 5) << "Mandy before calling set value for obj" << dendl;
  int rgwObjSetValue = mandyObjDirectory->setValue(&mandyCacheObj);
  ldout(cct, 5) << "Mandy: setObjValue: " << rgwObjSetValue << dendl;
  int rgwObjGetValue = mandyObjDirectory->getValue(&mandyCacheObj);
  ldout(cct, 5) << "Mandy: getObjValue: " << rgwObjGetValue << dendl;

  /*
  ldout(cct, 5) << "D3nDataCache: " << __func__ << "(): oid=" << c->oid << dendl;

  // call RGWBlockDirectory::setValue
  // https://github.com/ekaynar/ceph-master/blob/datacache/src/rgw/rgw_cache.cc
  // https://github.com/ekaynar/ceph-master/blob/datacache/src/rgw/rgw_cache.h 
  // create dummy c block, pass set value and pass c block 
  // log file in build/out/radosgw.8000.log --> put name with capital letter to search for error  
  Edit vstart file
  MON=1 OSD=1 RGW=1 MGR=0 MDS=0 ../src/vstart.sh -n -d -o debug_ms=0 \
  -o rgw_d3n_l1_local_datacache_enabled=true \
  -o rgw_d3n_l1_datacache_persistent_path=/tmp \
  -o rgw_d3n_l1_datacache_size=10737418240 \


if this doesn't work, ask for all config varaibles to start d3n and send him these 3
reverify when i do search for directory, confirm put in cache is the right place 
  

  */
  delete c;
  c = nullptr;
}

int D3nDataCache::d3n_libaio_create_write_request(bufferlist& bl, unsigned int len, std::string oid)
{
  lsubdout(g_ceph_context, rgw_datacache, 30) << "D3nDataCache: " << __func__ << "(): Write To Cache, oid=" << oid << ", len=" << len << dendl;
  struct D3nCacheAioWriteRequest* wr = new struct D3nCacheAioWriteRequest(cct);
  int r=0;
  if ((r = wr->d3n_prepare_libaio_write_op(bl, len, oid, cache_location)) < 0) {
    ldout(cct, 0) << "ERROR: D3nDataCache: " << __func__ << "() prepare libaio write op r=" << r << dendl;
    goto done;
  }
  wr->cb->aio_sigevent.sigev_notify = SIGEV_THREAD;
  wr->cb->aio_sigevent.sigev_notify_function = d3n_libaio_write_cb;
  wr->cb->aio_sigevent.sigev_notify_attributes = nullptr;
  wr->cb->aio_sigevent.sigev_value.sival_ptr = (void*)wr;
  wr->oid = oid;
  wr->priv_data = this;

  if ((r = ::aio_write(wr->cb)) != 0) {
    ldout(cct, 0) << "ERROR: D3nDataCache: " << __func__ << "() aio_write r=" << r << dendl;
    goto error;
  }
  return 0;

error:
  delete wr;
done:
  return r;
}

void D3nDataCache::put(bufferlist& bl, unsigned int len, std::string& oid)
{
  size_t sr = 0;
  uint64_t freed_size = 0, _free_data_cache_size = 0, _outstanding_write_size = 0;

  ldout(cct, 10) << "D3nDataCache::" << __func__ << "(): oid=" << oid << ", len=" << len << dendl;
  {
    const std::lock_guard l(d3n_cache_lock);
    std::unordered_map<string, D3nChunkDataInfo*>::iterator iter = d3n_cache_map.find(oid);
    if (iter != d3n_cache_map.end()) {
      ldout(cct, 10) << "D3nDataCache::" << __func__ << "(): data already cached, no rewrite" << dendl;
      return;
    }
    auto it = d3n_outstanding_write_list.find(oid);
    if (it != d3n_outstanding_write_list.end()) {
      ldout(cct, 10) << "D3nDataCache: NOTE: data put in cache already issued, no rewrite" << dendl;
      return;
    }
    d3n_outstanding_write_list.insert(oid);
  }
  {
    const std::lock_guard l(d3n_eviction_lock);
    _free_data_cache_size = free_data_cache_size;
    _outstanding_write_size = outstanding_write_size;
  }
  ldout(cct, 20) << "D3nDataCache: Before eviction _free_data_cache_size:" << _free_data_cache_size << ", _outstanding_write_size:" << _outstanding_write_size << ", freed_size:" << freed_size << dendl;
  while (len > (_free_data_cache_size - _outstanding_write_size + freed_size)) {
    ldout(cct, 20) << "D3nDataCache: enter eviction" << dendl;
    if (eviction_policy == _eviction_policy::LRU) {
      sr = lru_eviction();
    } else if (eviction_policy == _eviction_policy::RANDOM) {
      sr = random_eviction();
    } else {
      ldout(cct, 0) << "D3nDataCache: Warning: unknown cache eviction policy, defaulting to lru eviction" << dendl;
      sr = lru_eviction();
    }
    if (sr == 0) {
      ldout(cct, 2) << "D3nDataCache: Warning: eviction was not able to free disk space, not writing to cache" << dendl;
      d3n_outstanding_write_list.erase(oid);
      return;
    }
    ldout(cct, 20) << "D3nDataCache: completed eviction of " << sr << " bytes" << dendl;
    freed_size += sr;
  }
  int r = 0;
  r = d3n_libaio_create_write_request(bl, len, oid);
  if (r < 0) {
    const std::lock_guard l(d3n_cache_lock);
    d3n_outstanding_write_list.erase(oid);
    ldout(cct, 1) << "D3nDataCache: create_aio_write_request fail, r=" << r << dendl;
    return;
  }

  const std::lock_guard l(d3n_eviction_lock);
  free_data_cache_size += freed_size;
  outstanding_write_size += len;
}

bool D3nDataCache::get(const string& oid, const off_t len)
{
  const std::lock_guard l(d3n_cache_lock);
  bool exist = false;
  string location = cache_location + oid;

  lsubdout(g_ceph_context, rgw_datacache, 20) << "D3nDataCache: " << __func__ << "(): location=" << location << dendl;
  std::unordered_map<string, D3nChunkDataInfo*>::iterator iter = d3n_cache_map.find(oid);
  if (!(iter == d3n_cache_map.end())) {
    // check inside cache whether file exists or not!!!! then make exist true;
    struct D3nChunkDataInfo* chdo = iter->second;
    struct stat st;
    int r = stat(location.c_str(), &st);
    if ( r != -1 && st.st_size == len) { // file exists and containes required data range length
      exist = true;
      /*LRU*/
      /*get D3nChunkDataInfo*/
      const std::lock_guard l(d3n_eviction_lock);
      lru_remove(chdo);
      lru_insert_head(chdo);
    } else {
      d3n_cache_map.erase(oid);
      const std::lock_guard l(d3n_eviction_lock);
      lru_remove(chdo);
      delete chdo;
      exist = false;
    }
  }
  return exist;
}

size_t D3nDataCache::random_eviction()
{
  lsubdout(g_ceph_context, rgw_datacache, 20) << "D3nDataCache: " << __func__ << "()" << dendl;
  int n_entries = 0;
  int random_index = 0;
  size_t freed_size = 0;
  D3nChunkDataInfo* del_entry;
  string del_oid, location;
  {
    const std::lock_guard l(d3n_cache_lock);
    n_entries = d3n_cache_map.size();
    if (n_entries <= 0) {
      return -1;
    }
    srand (time(NULL));
    random_index = ceph::util::generate_random_number<int>(0, n_entries-1);
    std::unordered_map<string, D3nChunkDataInfo*>::iterator iter = d3n_cache_map.begin();
    std::advance(iter, random_index);
    del_oid = iter->first;
    del_entry =  iter->second;
    ldout(cct, 20) << "D3nDataCache: random_eviction: index:" << random_index << ", free size: " << del_entry->size << dendl;
    freed_size = del_entry->size;
    delete del_entry;
    del_entry = nullptr;
    d3n_cache_map.erase(del_oid); // oid
  }

  location = cache_location + del_oid;
  ::remove(location.c_str());
  return freed_size;
}

size_t D3nDataCache::lru_eviction()
{
  lsubdout(g_ceph_context, rgw_datacache, 20) << "D3nDataCache: " << __func__ << "()" << dendl;
  int n_entries = 0;
  size_t freed_size = 0;
  D3nChunkDataInfo* del_entry;
  string del_oid, location;

  {
    const std::lock_guard l(d3n_eviction_lock);
    del_entry = tail;
    if (del_entry == nullptr) {
      ldout(cct, 2) << "D3nDataCache: lru_eviction: del_entry=null_ptr" << dendl;
      return 0;
    }
    lru_remove(del_entry);
  }

  {
    const std::lock_guard l(d3n_cache_lock);
    n_entries = d3n_cache_map.size();
    if (n_entries <= 0) {
      ldout(cct, 2) << "D3nDataCache: lru_eviction: cache_map.size<=0" << dendl;
      return -1;
    }
    del_oid = del_entry->oid;
    ldout(cct, 20) << "D3nDataCache: lru_eviction: oid to remove: " << del_oid << dendl;
    d3n_cache_map.erase(del_oid); // oid
  }
  freed_size = del_entry->size;
  delete del_entry;
  location = cache_location + del_oid;
  ::remove(location.c_str());
  return freed_size;
}
