#pragma once

#include <aio.h>
#include "acconfig.h"
#include "rgw_common.h"
#include "rgw_cache_driver.h"

#if defined(HAVE_LIBURING)
#include <liburing.h>
#include <limits.h>
#include <memory>
#include <vector>
#include <mutex>
#include <unordered_map>

namespace rgw { namespace cache {

// Alignment for O_DIRECT I/O (typically 512 or 4096 bytes for modern devices)
constexpr size_t IO_BUFFER_ALIGNMENT = 4096;

// Round up size to alignment boundary
inline size_t align_size(size_t size, size_t alignment = IO_BUFFER_ALIGNMENT) {
  return (size + alignment - 1) & ~(alignment - 1);
}

// Thread-safe buffer pool for io_uring operations with aligned memory
class BufferPool {
public:
  BufferPool(size_t max_buffers_per_size = 64)
    : max_buffers_per_size_(max_buffers_per_size) {}

  ~BufferPool() {
    // Clean up all buffers
    for (auto& [size, pool] : pools_) {
      for (void* buf : pool) {
        ::free(buf);
      }
    }
  }

  // Allocate aligned buffer from pool or create new one
  void* allocate(size_t size) {
    // Round up to alignment boundary for O_DIRECT compatibility
    size_t aligned_size = align_size(size);

    std::lock_guard<std::mutex> lock(mutex_);
    auto& pool = pools_[aligned_size];

    if (!pool.empty()) {
      void* buf = pool.back();
      pool.pop_back();
      stats_.hits++;
      return buf;
    }

    // No buffer available, allocate aligned memory
    stats_.misses++;
    stats_.total_allocated++;
    void* buf = nullptr;
    if (posix_memalign(&buf, IO_BUFFER_ALIGNMENT, aligned_size) != 0) {
      return nullptr;
    }
    return buf;
  }

  // Return buffer to pool
  void deallocate(void* buf, size_t size) {
    if (!buf) return;

    size_t aligned_size = align_size(size);
    std::lock_guard<std::mutex> lock(mutex_);
    auto& pool = pools_[aligned_size];

    // Only keep up to max_buffers_per_size_ buffers per size
    if (pool.size() < max_buffers_per_size_) {
      pool.push_back(buf);
      stats_.returns++;
    } else {
      ::free(buf);
      stats_.freed++;
    }
  }

  // Get statistics
  struct Stats {
    uint64_t hits = 0;
    uint64_t misses = 0;
    uint64_t returns = 0;
    uint64_t freed = 0;
    uint64_t total_allocated = 0;
  };

  Stats get_stats() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return stats_;
  }

  // Clear all cached buffers (for testing/debugging)
  void clear() {
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto& [size, pool] : pools_) {
      for (void* buf : pool) {
        ::free(buf);
      }
      pool.clear();
    }
    pools_.clear();
  }

private:
  mutable std::mutex mutex_;
  std::unordered_map<size_t, std::vector<void*>> pools_;
  size_t max_buffers_per_size_;
  Stats stats_;
};

} } // namespace rgw::cache
#endif // HAVE_LIBURING

namespace rgw { namespace cache {

class SSDDriver : public CacheDriver {
public:
  SSDDriver(Partition& partition_info, bool admin) : partition_info(partition_info), admin(admin) {}
  virtual ~SSDDriver() {}

  virtual int initialize(const DoutPrefixProvider* dpp) override;
  virtual int put(const DoutPrefixProvider* dpp, const std::string& key, const bufferlist& bl, uint64_t len, const rgw::sal::Attrs& attrs, optional_yield y) override;
  virtual int get(const DoutPrefixProvider* dpp, const std::string& key, off_t offset, uint64_t len, bufferlist& bl, rgw::sal::Attrs& attrs, optional_yield y) override;
  virtual rgw::AioResultList get_async (const DoutPrefixProvider* dpp, optional_yield y, rgw::Aio* aio, const std::string& key, off_t ofs, uint64_t len, uint64_t cost, uint64_t id) override;
  virtual rgw::AioResultList put_async(const DoutPrefixProvider* dpp, optional_yield y, rgw::Aio* aio, const std::string& key, const bufferlist& bl, uint64_t len, const rgw::sal::Attrs& attrs, uint64_t cost, uint64_t id) override;
  virtual int append_data(const DoutPrefixProvider* dpp, const::std::string& key, const bufferlist& bl_data, optional_yield y) override;
  virtual int delete_data(const DoutPrefixProvider* dpp, const::std::string& key, optional_yield y) override;
  virtual int rename(const DoutPrefixProvider* dpp, const::std::string& oldKey, const::std::string& newKey, optional_yield y) override;
  virtual int get_attrs(const DoutPrefixProvider* dpp, const std::string& key, rgw::sal::Attrs& attrs, optional_yield y) override;
  virtual int set_attrs(const DoutPrefixProvider* dpp, const std::string& key, const rgw::sal::Attrs& attrs, optional_yield y) override;
  virtual int update_attrs(const DoutPrefixProvider* dpp, const std::string& key, const rgw::sal::Attrs& attrs, optional_yield y) override;
  virtual int delete_attrs(const DoutPrefixProvider* dpp, const std::string& key, rgw::sal::Attrs& del_attrs, optional_yield y) override;
  virtual int get_attr(const DoutPrefixProvider* dpp, const std::string& key, const std::string& attr_name, std::string& attr_val, optional_yield y) override;
  virtual int set_attr(const DoutPrefixProvider* dpp, const std::string& key, const std::string& attr_name, const std::string& attr_val, optional_yield y) override;
  int delete_attr(const DoutPrefixProvider* dpp, const std::string& key, const std::string& attr_name);

  /* Partition */
  virtual Partition get_current_partition_info(const DoutPrefixProvider* dpp) override { return partition_info; }
  virtual uint64_t get_free_space(const DoutPrefixProvider* dpp, optional_yield y) override;
  void set_free_space(const DoutPrefixProvider* dpp, uint64_t free_space);

  virtual int restore_blocks_objects(const DoutPrefixProvider* dpp, ObjectDataCallback obj_func, BlockDataCallback block_func) override;

#if defined(HAVE_LIBURING)
  BufferPool::Stats get_buffer_pool_stats() const { return buffer_pool_.get_stats(); }
#endif

private:
  Partition partition_info;
  uint64_t free_space;
  CephContext* cct;
  std::mutex cache_lock;
  bool admin;

  // Runtime backend selection - true = io_uring, false = libaio
  bool use_io_uring_ = false;

#if defined(HAVE_LIBURING)
  BufferPool buffer_pool_;  // Buffer pool for io_uring operations
  int64_t IoUringQueueDepth = 256;  // Will be set from config in initialize()

  int ensure_thread_uring(const DoutPrefixProvider* dpp, struct io_uring** ring_out) const;
#endif

  struct libaio_read_handler {
    rgw::Aio* throttle = nullptr;
    rgw::AioResult& r;
    // read callback
    void operator()(boost::system::error_code ec, bufferlist bl) const {
      r.result = -ec.value();
      r.data = std::move(bl);
      throttle->put(r);
    }
  };

  struct libaio_write_handler {
    rgw::Aio* throttle = nullptr;
    rgw::AioResult& r;
    // write callback
    void operator()(boost::system::error_code ec) const {
      r.result = -ec.value();
      throttle->put(r);
    }
  };

#if defined(HAVE_LIBURING)
  // Async read operation using io_uring
  struct IoUringAsyncReadOp {
    bufferlist result;
    int fd = -1;
    off_t offset = 0;
    size_t length = 0;
    void* buffer = nullptr;
    BufferPool* buffer_pool = nullptr;
    using Signature = void(boost::system::error_code, bufferlist);
    using Completion = ceph::async::Completion<Signature, IoUringAsyncReadOp>;

    int prepare_io_uring_read_op(const DoutPrefixProvider *dpp, const std::string& file_path, off_t read_ofs, size_t read_len, void* arg, struct io_uring* ring);
    static void io_uring_read_completion(struct io_uring_cqe* cqe, IoUringAsyncReadOp* op);

    template <typename Executor1, typename CompletionHandler>
    static auto create(const Executor1& ex1, CompletionHandler&& handler);
  };

  // Async write operation using io_uring
  class IoUringAsyncWriteRequest {
  public:
    const DoutPrefixProvider* dpp;
    std::string file_path;
    std::string temp_file_path;
    void* data;
    int fd;
    size_t length;
    SSDDriver *priv_data;
    rgw::sal::Attrs attrs;
    BufferPool* buffer_pool;

    using Signature = void(boost::system::error_code);
    using Completion = ceph::async::Completion<Signature, IoUringAsyncWriteRequest>;

    int prepare_io_uring_write_op(const DoutPrefixProvider *dpp, bufferlist& bl, unsigned int len, std::string file_path, struct io_uring* ring);
    static void io_uring_write_completion(struct io_uring_cqe* cqe, IoUringAsyncWriteRequest* op);

    template <typename Executor1, typename CompletionHandler>
    static auto create(const Executor1& ex1, CompletionHandler&& handler);

    IoUringAsyncWriteRequest() : dpp(nullptr), data(nullptr), fd(-1), length(0), priv_data(nullptr), buffer_pool(nullptr) {}
    ~IoUringAsyncWriteRequest() = default;
  };
#endif // HAVE_LIBURING

  // libaio implementation (always available as fallback)
  // unique_ptr with custom deleter for struct aiocb
  struct libaio_aiocb_deleter {
    void operator()(struct aiocb* c) {
      if(c->aio_fildes > 0) {
        TEMP_FAILURE_RETRY(::close(c->aio_fildes));
      }
      c->aio_buf = nullptr;
      delete c;
    }
  };

  using unique_aio_cb_ptr = std::unique_ptr<struct aiocb, libaio_aiocb_deleter>;

  struct LibaioAsyncReadOp {
    bufferlist result;
    unique_aio_cb_ptr aio_cb;
    using Signature = void(boost::system::error_code, bufferlist);
    using Completion = ceph::async::Completion<Signature, LibaioAsyncReadOp>;

    int prepare_libaio_read_op(const DoutPrefixProvider *dpp, const std::string& file_path, off_t read_ofs, off_t read_len, void* arg);
    static void libaio_cb_aio_dispatch(sigval sigval);

    template <typename Executor1, typename CompletionHandler>
    static auto create(const Executor1& ex1, CompletionHandler&& handler);
  };

  struct LibaioAsyncWriteRequest {
    const DoutPrefixProvider* dpp;
    std::string file_path;
    std::string temp_file_path;
    void *data;
    int fd;
    unique_aio_cb_ptr cb;
    SSDDriver *priv_data;
    rgw::sal::Attrs attrs;

    using Signature = void(boost::system::error_code);
    using Completion = ceph::async::Completion<Signature, LibaioAsyncWriteRequest>;

    int prepare_libaio_write_op(const DoutPrefixProvider *dpp, bufferlist& bl, unsigned int len, std::string file_path);
    static void libaio_write_cb(sigval sigval);

    template <typename Executor1, typename CompletionHandler>
    static auto create(const Executor1& ex1, CompletionHandler&& handler);
  };

#if defined(HAVE_LIBURING)
  // io_uring async operations
  template <typename Executor, typename CompletionToken>
  auto get_async_uring(const DoutPrefixProvider *dpp, const Executor& ex, const std::string& key,
                 off_t read_ofs, off_t read_len, CompletionToken&& token);

  template <typename Executor, typename CompletionToken>
  void put_async_uring(const DoutPrefixProvider *dpp, const Executor& ex, const std::string& key,
                 const bufferlist& bl, uint64_t len, const rgw::sal::Attrs& attrs, CompletionToken&& token);
#endif

  // libaio async operations
  template <typename Executor, typename CompletionToken>
  auto get_async_libaio(const DoutPrefixProvider *dpp, const Executor& ex, const std::string& key,
                 off_t read_ofs, off_t read_len, CompletionToken&& token);

  template <typename Executor, typename CompletionToken>
  void put_async_libaio(const DoutPrefixProvider *dpp, const Executor& ex, const std::string& key,
                 const bufferlist& bl, uint64_t len, const rgw::sal::Attrs& attrs, CompletionToken&& token);

  // Dispatch to appropriate backend based on use_io_uring_ flag
  template <typename Executor, typename CompletionToken>
  auto get_async(const DoutPrefixProvider *dpp, const Executor& ex, const std::string& key,
                 off_t read_ofs, off_t read_len, CompletionToken&& token);

  template <typename Executor, typename CompletionToken>
  void put_async(const DoutPrefixProvider *dpp, const Executor& ex, const std::string& key,
                 const bufferlist& bl, uint64_t len, const rgw::sal::Attrs& attrs, CompletionToken&& token);

  rgw::Aio::OpFunc ssd_cache_read_op(const DoutPrefixProvider *dpp, optional_yield y, rgw::cache::CacheDriver* cache_driver,
                                     off_t read_ofs, off_t read_len, const std::string& key);

  rgw::Aio::OpFunc ssd_cache_write_op(const DoutPrefixProvider *dpp, optional_yield y, rgw::cache::CacheDriver* cache_driver,
                                      const bufferlist& bl, uint64_t len, const rgw::sal::Attrs& attrs, const std::string& key);
};

} } // namespace rgw::cache
