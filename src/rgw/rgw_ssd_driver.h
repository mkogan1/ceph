#pragma once

#include <liburing.h>
#include <limits.h>
#include "rgw_common.h"
#include "rgw_cache_driver.h"

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
  virtual uint64_t get_free_space(const DoutPrefixProvider* dpp) override;
  void set_free_space(const DoutPrefixProvider* dpp, uint64_t free_space);

  virtual int restore_blocks_objects(const DoutPrefixProvider* dpp, ObjectDataCallback obj_func, BlockDataCallback block_func) override;

private:
  Partition partition_info;
  uint64_t free_space;
  CephContext* cct;
  std::mutex cache_lock;
  bool admin;

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

  // Async read operation using io_uring
  struct AsyncReadOp {
    bufferlist result;
    int fd;
    off_t offset;
    size_t length;
    void* buffer;
    using Signature = void(boost::system::error_code, bufferlist);
    using Completion = ceph::async::Completion<Signature, AsyncReadOp>;

    int prepare_io_uring_read_op(const DoutPrefixProvider *dpp, const std::string& file_path, off_t read_ofs, size_t read_len, void* arg, struct io_uring* ring);
    static void io_uring_read_completion(struct io_uring_cqe* cqe, AsyncReadOp* op);

    template <typename Executor1, typename CompletionHandler>
    static auto create(const Executor1& ex1, CompletionHandler&& handler);
  };

  // Async write operation using io_uring
  class AsyncWriteRequest {
  public:
    const DoutPrefixProvider* dpp;
    std::string file_path;
    std::string temp_file_path;
    void* data;
    int fd;
    size_t length;
    SSDDriver *priv_data;
    rgw::sal::Attrs attrs;

    using Signature = void(boost::system::error_code);
    using Completion = ceph::async::Completion<Signature, AsyncWriteRequest>;

    int prepare_io_uring_write_op(const DoutPrefixProvider *dpp, bufferlist& bl, unsigned int len, std::string file_path, struct io_uring* ring);
    static void io_uring_write_completion(struct io_uring_cqe* cqe, AsyncWriteRequest* op);

    template <typename Executor1, typename CompletionHandler>
    static auto create(const Executor1& ex1, CompletionHandler&& handler);

    AsyncWriteRequest() {
      file_path = "";
      temp_file_path = "";
      std::clog << "MK| OK >>" << __FILE__ << " :" << __LINE__ << " | " << __func__ << "(): >>" << std::endl;
    }
    ~AsyncWriteRequest() {
      std::clog << "MK| OK <<" << __FILE__ << " :" << __LINE__ << " | " << __func__ << "(): <<" << std::endl;
    }
  };

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

  int ensure_thread_uring(const DoutPrefixProvider* dpp, struct io_uring** ring_out) const;

  int64_t IoUringQueueDepth = g_conf().get_val<int64_t>("rgw_d4n_io_uring_queue_depth");

};

} } // namespace rgw::cache

