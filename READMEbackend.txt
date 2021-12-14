This document contains general information on each file modified by the backend team for the 528 Cloud Computing Project
12/14/2021
------------------------------------------------------------------------------------------------------------------------
/src/rgw
    rgw_d3n_datacache.h
        D3NDataCache: added new function submit_remote_req()
        get_obj_iterate_cb(): modified to attempt remote request to other RGW's caches. Currently nonfunctional
        class RemoteS3Request: added class
    rgw_d3n_datacache.cc
        RemoteS3Request: Ported functions
        class CacheThreadPool: added class
        D3NDataCache::submit_remote_req(): added function
    rgw_aio.h
        remote_op(): new function
    rgw_aio.cc
        remote_op(): Implemented function
        remote_aio_abstract(): Implemented two forms of function - isolated as part of fault in get operation
        remote_aio_cb(): Implemented function
    rgw_d3n_cacherequest.h
        class CacheRequest: added class - could be incorporated into D3nL1CacheRequest?
        struct LocalRequest: added class
    rgw_d3n_cacherequest.cc: Added file
        RemoteRequest::prepare_op(): Implemented function
    rgw_threadpool.h: Added file(?)
    rgw_cache.h: added new headers
/src/common/options
    rgw.yaml.in: new values for config added to assist with remote op
/src/librados/
    IoCtxImpl.cc:
        librados::IoCtxImpl::cache_aio_operate_read(): Implemented function - should remove this from librados due to refactoring
    librados_asio.h:
        async_operate(): Implemented function
    librados_cxx.cc:
        librados::IoCtx::aio_operate(): Implemented function
      

