#include <boost/asio/system_executor.hpp>
#include "common/async/completion.h"
#include "common/errno.h"
#include "common/async/blocked_completion.h"
#include "rgw_ssd_driver.h"

#if defined(HAVE_LIBURING)
#include <liburing.h>
#include <boost/asio/posix/stream_descriptor.hpp>
#endif

#if defined(__linux__)
#include <features.h>
#include <sys/eventfd.h>
#include <sys/xattr.h>
#endif

#include <filesystem>
#include <cstdio>
#include <errno.h>
#include <unistd.h>
namespace efs = std::filesystem;

namespace rgw { namespace cache {

static std::atomic<uint64_t> index{0};
static std::atomic<uint64_t> dir_index{0};

#if defined(HAVE_LIBURING)
namespace {
struct IoUringCqeHandler {
    virtual ~IoUringCqeHandler() = default;
    virtual void complete_from_cqe(const io_uring_cqe& cqe) = 0;
    virtual void complete_from_error(int err) = 0;
};

struct ThreadIoUringState {
    io_uring ring{};
    int event_fd = -1;
    bool initialized = false;
    bool eventfd_registered = false;
    std::mutex ring_mutex;
    std::unique_ptr<boost::asio::posix::stream_descriptor> event_stream;
    bool reaper_started = false;
    bool wait_armed = false;

    int ensure(unsigned queue_depth, unsigned flags) {
        if (initialized) {
            return 0;
        }
        int ret = io_uring_queue_init(queue_depth, &ring, flags);
        if (ret < 0) {
            // Retry without optional flags when kernel support is limited.
            if (ret == -EINVAL) {
                unsigned basic_flags = flags & (IORING_SETUP_IOPOLL | IORING_SETUP_SQPOLL);
                ret = io_uring_queue_init(queue_depth, &ring, basic_flags);
            }
            if (ret < 0) {
                return ret;
            }
        }

        event_fd = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
        if (event_fd < 0) {
            int err = -errno;
            io_uring_queue_exit(&ring);
            return err;
        }

        ret = io_uring_register_eventfd(&ring, event_fd);
        if (ret < 0) {
            ::close(event_fd);
            event_fd = -1;
            io_uring_queue_exit(&ring);
            return ret;
        }
        eventfd_registered = true;
        initialized = true;
        return 0;
    }

    ~ThreadIoUringState() {
        if (initialized) {
            io_uring_queue_exit(&ring);
        }
        if (!event_stream && event_fd >= 0) {
            ::close(event_fd);
            event_fd = -1;
        }
    }
};

thread_local std::shared_ptr<ThreadIoUringState> thread_uring_state;

std::shared_ptr<ThreadIoUringState> get_thread_uring_state()
{
    if (!thread_uring_state) {
        thread_uring_state = std::make_shared<ThreadIoUringState>();
    }
    return thread_uring_state;
}

void arm_thread_uring_reaper(const std::shared_ptr<ThreadIoUringState>& state);

void process_thread_uring_cqes(const std::shared_ptr<ThreadIoUringState>& state)
{
    std::vector<std::pair<IoUringCqeHandler*, io_uring_cqe>> completions;
    {
        std::lock_guard<std::mutex> lock(state->ring_mutex);
        io_uring_cqe* cqe = nullptr;
        while (io_uring_peek_cqe(&state->ring, &cqe) == 0) {
            auto* handler = static_cast<IoUringCqeHandler*>(io_uring_cqe_get_data(cqe));
            completions.emplace_back(handler, *cqe);
            io_uring_cqe_seen(&state->ring, cqe);
        }
    }

    for (auto& [handler, cqe] : completions) {
        if (!handler) {
            continue;
        }
        handler->complete_from_cqe(cqe);
        delete handler;
    }
}

void on_thread_uring_event(const std::shared_ptr<ThreadIoUringState>& state,
                           const boost::system::error_code& ec)
{
    {
        std::lock_guard<std::mutex> lock(state->ring_mutex);
        state->wait_armed = false;
    }

    if (ec == boost::asio::error::operation_aborted) {
        return;
    }

    if (!ec) {
        uint64_t wakeups = 0;
        while (::read(state->event_fd, &wakeups, sizeof(wakeups)) < 0 && errno == EINTR) {
        }
    }

    // Drain all CQEs that are currently available and dispatch callbacks.
    process_thread_uring_cqes(state);
    arm_thread_uring_reaper(state);
}

void arm_thread_uring_reaper(const std::shared_ptr<ThreadIoUringState>& state)
{
    boost::asio::posix::stream_descriptor* stream = nullptr;
    {
        std::lock_guard<std::mutex> lock(state->ring_mutex);
        if (!state->event_stream || state->wait_armed) {
            return;
        }
        state->wait_armed = true;
        stream = state->event_stream.get();
    }

    stream->async_wait(boost::asio::posix::stream_descriptor::wait_read,
                       [weak_state = std::weak_ptr<ThreadIoUringState>(state)](const boost::system::error_code& ec) {
                           if (auto state_locked = weak_state.lock()) {
                               on_thread_uring_event(state_locked, ec);
                           }
                       });
}

template <typename Executor>
int ensure_thread_uring_reaper(const std::shared_ptr<ThreadIoUringState>& state,
                               const Executor& ex)
{
    {
        std::lock_guard<std::mutex> lock(state->ring_mutex);
        if (!state->event_stream) {
            state->event_stream = std::make_unique<boost::asio::posix::stream_descriptor>(ex, state->event_fd);
        }
        if (state->reaper_started) {
            return 0;
        }
        state->reaper_started = true;
    }
    arm_thread_uring_reaper(state);
    return 0;
}
} // anonymous namespace
#endif // HAVE_LIBURING

static std::vector<std::string> tokenize_key(std::string_view key)
{
    std::vector<std::string> tokens;
    size_t start = 0, end = 0;
    while ((end = key.find(CACHE_DELIM, start)) != std::string_view::npos) {
        tokens.emplace_back(key.substr(start, end - start));
        start = end + 1;
    }
    // Add the last token
    if (start < key.length()) {
        tokens.emplace_back(key.substr(start));
    }
    return tokens;
}

/*
* Parses key to return directory path and file name
*/
static void parse_key(const DoutPrefixProvider* dpp, const std::string& location, const std::string& key, std::string& dir_path, std::string& file_name, bool temp = false) {
    ldpp_dout(dpp, 10) << __func__ << "() key is: " << key << dendl;
    std::string bucket_id, object, version;
    std::vector<std::string> parts = tokenize_key(key);

    ldpp_dout(dpp, 10) << __func__ << "() parts.size() is " << parts.size() << dendl;

    if (parts.size() == 3 || parts.size() == 5) {
        bucket_id = parts[0];
        ldpp_dout(dpp, 10) <<  __func__ << "() bucket_id is " << bucket_id << dendl;
        object = parts[2];
        ldpp_dout(dpp, 10) <<  __func__ << "() object is " << object << dendl;
        version = parts[1];
        if (parts.size() == 5) { //has offset and length
            version += CACHE_DELIM + parts[3] + CACHE_DELIM + parts[4];
        }
        if (temp) {
            version += "_" + std::to_string(index++);
        }
        ldpp_dout(dpp, 10) <<  __func__ << "() version is " << version << dendl;
        dir_path = location + "/" + bucket_id + "/" + object;
        file_name = version;
        ldpp_dout(dpp, 10) <<  __func__ << "() dir_path is " << dir_path << dendl;
    }
    return;
}

static void create_directories(const DoutPrefixProvider* dpp, const std::string& dir_path)
{
    std::error_code ec;
    std::string temp_dir_path = dir_path + "_" + std::to_string(dir_index++);
    if (!efs::exists(dir_path, ec)) {
        if (!efs::create_directories(temp_dir_path, ec)) {
            ldpp_dout(dpp, 0) << "create_directories::: ERROR creating directory: '" << temp_dir_path <<
                            "' : " << ec.value() << dendl;
        } else {
            efs::rename(temp_dir_path, dir_path, ec);
            if (ec) {
                ldpp_dout(dpp, 0) << "create_directories::: ERROR renaming directory: '" << temp_dir_path <<
                            "' : " << ec.value() << dendl;
                efs::remove(temp_dir_path, ec);
            } else {
                uid_t uid = dpp->get_cct()->get_set_uid();
                gid_t gid = dpp->get_cct()->get_set_gid();

                ldpp_dout(dpp, 5) << "create_directories:: uid is " << uid << " and gid is " << gid << dendl;
                ldpp_dout(dpp, 5) << "create_directories:: changing permissions for directory: " << dendl;

                if (uid) {
                    if (chown(dir_path.c_str(), uid, gid) == -1) {
                        ldpp_dout(dpp, 5) << "create_directories: chown return error: " << strerror(errno) << dendl;
                    }

                    if (chmod(dir_path.c_str(), S_IRWXU|S_IRWXG|S_IRWXO) == -1) {
                        ldpp_dout(dpp, 5) << "create_directories: chmod return error: " << strerror(errno) << dendl;
                    }
                }
            }
        }
    }
}

static inline std::string get_file_path(const DoutPrefixProvider* dpp, const std::string& dir_path, const std::string& file_name)
{
    return dir_path + "/" + file_name;
}

static std::string create_dirs_get_filepath_from_key(const DoutPrefixProvider* dpp, const std::string& location, const std::string& key, bool temp=false)
{
    std::string dir_path, file_name;
    parse_key(dpp, location, key, dir_path, file_name, temp);
    create_directories(dpp, dir_path);
    return get_file_path(dpp, dir_path, file_name);
}

#if defined(HAVE_LIBURING)
int SSDDriver::ensure_thread_uring(const DoutPrefixProvider* dpp, struct io_uring** ring_out) const
{
    unsigned flags = 0;
    std::string enabled_features;

    // IOPOLL: Use polling for I/O completion instead of interrupts
    // This provides lower latency for NVMe devices that support polling
    // Note: IOPOLL requires O_DIRECT and is checked/warned in initialize()
    if (dpp->get_cct()->_conf->rgw_d4n_io_uring_iopoll) {
        flags |= IORING_SETUP_IOPOLL;
        enabled_features += "IOPOLL ";
    }

    // SQPOLL: Kernel-side submission queue polling
    // Note: SQPOLL and IOPOLL can be used together for maximum performance
    if (dpp->get_cct()->_conf->rgw_d4n_io_uring_sqpoll) {
        flags |= IORING_SETUP_SQPOLL;
        enabled_features += "SQPOLL ";
    }
    if (dpp->get_cct()->_conf->rgw_d4n_io_uring_direct_io) {
        enabled_features += "O_DIRECT ";
    }
    enabled_features += "EVENTFD ";

    auto state = get_thread_uring_state();
    int ret = state->ensure(IoUringQueueDepth, flags);
    if (ret < 0) {
        ldpp_dout(dpp, 0) << "ERROR: Failed to initialize io_uring with flags=" << flags
                          << ", queue_depth=" << IoUringQueueDepth << ": " << cpp_strerror(-ret) << dendl;
        return ret;
    }

    if (!enabled_features.empty()) {
        ldpp_dout(dpp, 10) << "SSDDriver: io_uring initialized with async CQ reaper: " << enabled_features
                          << "(queue_depth=" << IoUringQueueDepth << ")" << dendl;
    }

    if (ring_out) {
        *ring_out = &state->ring;
    }

    return 0;
}
#endif // HAVE_LIBURING

int SSDDriver::initialize(const DoutPrefixProvider* dpp)
{
    if(partition_info.location.back() != '/') {
      partition_info.location += "/";
    }

    if (!admin) { // Only initialize or evict cache if radosgw-admin is not responsible for call
      try {
          if (efs::exists(partition_info.location)) {
              if (dpp->get_cct()->_conf->rgw_d4n_l1_evict_cache_on_start) {
                  ldpp_dout(dpp, 5) << "initialize: evicting the persistent storage directory on start" << dendl;

                  uid_t uid = dpp->get_cct()->get_set_uid();
                  gid_t gid = dpp->get_cct()->get_set_gid();

                  ldpp_dout(dpp, 5) << "initialize:: uid is " << uid << " and gid is " << gid << dendl;
                  ldpp_dout(dpp, 5) << "initialize:: changing permissions for datacache directory." << dendl;

                  if (uid) {
                    if (chown(partition_info.location.c_str(), uid, gid) == -1) {
                      ldpp_dout(dpp, 5) << "initialize: chown return error: " << strerror(errno) << dendl;
                    }

                    if (chmod(partition_info.location.c_str(), S_IRWXU|S_IRWXG|S_IRWXO) == -1) {
                      ldpp_dout(dpp, 5) << "initialize: chmod return error: " << strerror(errno) << dendl;
                    }
                  }

                  for (auto& p : efs::directory_iterator(partition_info.location)) {
                      efs::remove_all(p.path());
                  }
              }
          } else {
              ldpp_dout(dpp, 5) << "initialize:: creating the persistent storage directory on start: " << partition_info.location << dendl;
              std::error_code ec;
              if (!efs::create_directories(partition_info.location, ec)) {
                  ldpp_dout(dpp, 0) << "initialize::: ERROR initializing the cache storage directory: '" << partition_info.location <<
                                  "' : " << ec.value() << dendl;
              } else {
                  uid_t uid = dpp->get_cct()->get_set_uid();
                  gid_t gid = dpp->get_cct()->get_set_gid();

                  ldpp_dout(dpp, 5) << "initialize:: uid is " << uid << " and gid is " << gid << dendl;
                  ldpp_dout(dpp, 5) << "initialize:: changing permissions for datacache directory." << dendl;

                  if (uid) {
                    if (chown(partition_info.location.c_str(), uid, gid) == -1) {
                      ldpp_dout(dpp, 5) << "initialize: chown return error: " << strerror(errno) << dendl;
                    }

                    if (chmod(partition_info.location.c_str(), S_IRWXU|S_IRWXG|S_IRWXO) == -1) {
                      ldpp_dout(dpp, 5) << "initialize: chmod return error: " << strerror(errno) << dendl;
                    }
                  }
              }
          }
      } catch (const efs::filesystem_error& e) {
          ldpp_dout(dpp, 0) << "initialize::: ERROR initializing the cache storage directory '" << partition_info.location <<
                                  "' : " << e.what() << dendl;
      }
    }

    // Determine which I/O backend to use based on config
    std::string backend_type = dpp->get_cct()->_conf.get_val<std::string>("rgw_d4n_io_backend_type");
    ldpp_dout(dpp, 5) << "SSDDriver: requested I/O backend type: " << backend_type << dendl;

#if defined(HAVE_LIBURING)
    if (backend_type == "liburing") {
        // Try to initialize io_uring
        IoUringQueueDepth = dpp->get_cct()->_conf.get_val<int64_t>("rgw_d4n_io_uring_queue_depth");

        // Check IOPOLL requirements
        if (dpp->get_cct()->_conf->rgw_d4n_io_uring_iopoll &&
            !dpp->get_cct()->_conf->rgw_d4n_io_uring_direct_io) {
            ldpp_dout(dpp, 0) << "WARNING: IOPOLL requires O_DIRECT. Enabling rgw_d4n_io_uring_direct_io "
                              << "is recommended for IOPOLL mode to work correctly." << dendl;
        }

        int uring_ret = ensure_thread_uring(dpp, nullptr);
        if (uring_ret < 0) {
            ldpp_dout(dpp, 0) << "WARNING: io_uring initialization failed: " << cpp_strerror(-uring_ret)
                              << ", falling back to libaio" << dendl;
            use_io_uring_ = false;
        } else {
            ldpp_dout(dpp, 5) << "SSDDriver: io_uring backend initialized with buffer pool" << dendl;
            use_io_uring_ = true;
        }
    } else {
        ldpp_dout(dpp, 5) << "SSDDriver: using libaio backend (io_uring available but not selected)" << dendl;
        use_io_uring_ = false;
    }
#else
    if (backend_type == "liburing") {
        ldpp_dout(dpp, 0) << "WARNING: io_uring backend requested but liburing not compiled in, using libaio" << dendl;
    }
    use_io_uring_ = false;
#endif

    // Initialize libaio if that's what we're using
    if (!use_io_uring_) {
#if defined(HAVE_LIBAIO) && defined(__GLIBC__)
        struct aioinit ainit{0};
        ainit.aio_threads = dpp->get_cct()->_conf.get_val<int64_t>("rgw_d4n_libaio_aio_threads");
        ainit.aio_num = dpp->get_cct()->_conf.get_val<int64_t>("rgw_d4n_libaio_aio_num");
        ainit.aio_idle_time = 120;
        aio_init(&ainit);
        ldpp_dout(dpp, 5) << "SSDDriver: libaio backend initialized" << dendl;
#endif
    }

    ldpp_dout(dpp, 5) << "SSDDriver: using " << (use_io_uring_ ? "io_uring" : "libaio") << " I/O backend" << dendl;

    efs::space_info space = efs::space(partition_info.location);
    this->free_space = space.available;

    return 0;
}

int SSDDriver::restore_blocks_objects(const DoutPrefixProvider* dpp, ObjectDataCallback obj_func, BlockDataCallback block_func)
{
    if (dpp->get_cct()->_conf->rgw_d4n_l1_evict_cache_on_start) {
        return 0; //don't do anything as the cache directory must have been evicted during start-up
    }
    std::string cache_location = partition_info.location;
    if (cache_location.back() == '/') {
        ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): cache_location: " << cache_location << dendl;
        cache_location.pop_back();
    }
    for (auto const& dir_entry : efs::directory_iterator{partition_info.location}) {
        std::string bucket_id, object_name;
        if (dir_entry.is_directory()) {
            ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): Is directory, path: " << dir_entry.path() << dendl;
            ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): File Name: " << dir_entry.path().filename() << dendl;
            bucket_id = dir_entry.path().filename();
            for (auto const& sub_dir_entry : efs::directory_iterator{dir_entry.path()}) {
                if (sub_dir_entry.is_directory()) {
                    ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): Is directory, path: " << sub_dir_entry.path() << dendl;
                    ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): File Name: " << sub_dir_entry.path().filename() << dendl;
                    object_name = sub_dir_entry.path().filename();
                    for (auto const& file_entry : efs::directory_iterator{sub_dir_entry.path()}) {
                        try {
                            if (file_entry.is_regular_file()) {
                                ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): filename: " << file_entry.path().filename() << dendl;
                                std::string file_name = file_entry.path().filename();
                                bool parsed = false;
                                std::vector<std::string> parts;
                                std::string part;
                                std::stringstream ss(file_name);
                                while (std::getline(ss, part, CACHE_DELIM)) {
                                    parts.push_back(part);
                                }
                                ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): parts.size(): " << parts.size() << dendl;

                                std::string dirtyStr;
                                bool dirty;
                                auto ret = get_attr(dpp, file_entry.path(), RGW_CACHE_ATTR_DIRTY, dirtyStr, null_yield);
                                if (ret == 0 && dirtyStr == "1") {
                                    ldpp_dout(dpp, 10) << "SSDCache: " << __func__ << "(): Dirty xattr retrieved" << dendl;
                                    dirty = true;
                                } else if (ret < 0) {
                                    ldpp_dout(dpp, 0) << "SSDCache: " << __func__ << "(): Failed to get attr: " << RGW_CACHE_ATTR_DIRTY << ", ret=" << ret << dendl;
                                    dirty = false;
                                } else {
                                    dirty = false;
                                }

                                if (parts.size() == 1 || parts.size() == 3) {
                                    std::string version = url_decode(parts[0]);
                                    ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): version: " << version << dendl;

                                    std::string key = url_encode(bucket_id, true) + CACHE_DELIM + url_encode(version, true) + CACHE_DELIM + url_encode(object_name, true);
                                    ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): key: " << key << dendl;

                                    uint64_t len = 0, offset = 0;
                                    if (parts.size() == 1) {
                                        if (dirtyStr == "0") {
                                            std::string localWeightStr;
                                            ret = get_attr(dpp, file_entry.path(), RGW_CACHE_ATTR_LOCAL_WEIGHT, localWeightStr, null_yield);
                                            if (ret < 0) {
                                                ldpp_dout(dpp, 0) << "SSDCache: " << __func__ << "(): Failed to get attr: " << RGW_CACHE_ATTR_LOCAL_WEIGHT << dendl;
                                            } else {
                                                ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): localWeightStr: " << localWeightStr << dendl;
                                            }
                                            block_func(dpp, key, offset, len, version, false, null_yield, localWeightStr);
                                            parsed = true;
                                        } else if (dirtyStr == "1") {
                                            std::string localWeightStr;
                                            std::string invalidStr;
                                            rgw::sal::Attrs attrs;
                                            get_attrs(dpp, file_entry.path(), attrs, null_yield);
                                            std::string etag, bucket_name;
                                            uint64_t size = 0;
                                            time_t creationTime = time_t(nullptr);
                                            rgw_user user;
                                            rgw_obj_key obj_key;
                                            bool deleteMarker = false;
                                            if (attrs.find(RGW_ATTR_ETAG) != attrs.end()) {
                                                etag = attrs[RGW_ATTR_ETAG].to_str();
                                                ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): etag: " << etag << dendl;
                                            }
                                            if (attrs.find(RGW_CACHE_ATTR_OBJECT_SIZE) != attrs.end()) {
                                                size = std::stoull(attrs[RGW_CACHE_ATTR_OBJECT_SIZE].to_str());
                                                ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): size: " << size << dendl;
                                            }
                                            if (attrs.find(RGW_CACHE_ATTR_MTIME) != attrs.end()) {
                                                creationTime = ceph::real_clock::to_time_t(ceph::real_clock::from_double(std::stod(attrs[RGW_CACHE_ATTR_MTIME].to_str())));
                                                ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): creationTime: " << creationTime << dendl;
                                            }
                                            if (attrs.find(RGW_ATTR_ACL) != attrs.end()) {
                                                bufferlist bl_acl = attrs[RGW_ATTR_ACL];
                                                RGWAccessControlPolicy policy;
                                                auto iter = bl_acl.cbegin();
                                                try {
                                                    policy.decode(iter);
                                                } catch (buffer::error& err) {
                                                    ldpp_dout(dpp, 0) << "ERROR: could not decode policy, caught buffer::error" << dendl;
                                                    continue;
                                                }
                                                user = std::get<rgw_user>(policy.get_owner().id);
                                                ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): rgw_user: " << user.to_str() << dendl;
                                            }
                                            obj_key.name = object_name;
                                            if (attrs.find(RGW_CACHE_ATTR_VERSION_ID) != attrs.end()) {
                                                std::string instance = attrs[RGW_CACHE_ATTR_VERSION_ID].to_str();
                                                if (instance != "null") {
                                                    obj_key.instance = instance;
                                                }
                                            }
                                            if (attrs.find(RGW_CACHE_ATTR_OBJECT_NS) != attrs.end()) {
                                                obj_key.ns = attrs[RGW_CACHE_ATTR_OBJECT_NS].to_str();
                                            }
                                            ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): rgw_obj_key: " << obj_key.get_oid() << dendl;
                                            if (attrs.find(RGW_CACHE_ATTR_BUCKET_NAME) != attrs.end()) {
                                                bucket_name = attrs[RGW_CACHE_ATTR_BUCKET_NAME].to_str();
                                                ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): bucket_name: " << bucket_name << dendl;
                                            }
                                            if (attrs.find(RGW_CACHE_ATTR_LOCAL_WEIGHT) != attrs.end()) {
                                                localWeightStr = attrs[RGW_CACHE_ATTR_LOCAL_WEIGHT].to_str();
                                                ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): localWeightStr: " << localWeightStr << dendl;
                                            }
                                            if (attrs.find(RGW_CACHE_ATTR_DELETE_MARKER) != attrs.end()) {
                                                std::string deleteMarkerStr = attrs[RGW_CACHE_ATTR_DELETE_MARKER].to_str();
                                                deleteMarker = (deleteMarkerStr == "1") ? true : false;
                                                ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): deleteMarker: " << deleteMarker << dendl;
                                            }
                                            if (attrs.find(RGW_CACHE_ATTR_INVALID) != attrs.end()) {
                                                invalidStr = attrs[RGW_CACHE_ATTR_INVALID].to_str();
                                                ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): invalidStr: " << invalidStr << dendl;
                                            }
                                            ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): calling func for: " << key << dendl;
                                            obj_func(dpp, key, version, deleteMarker, size, creationTime, user, etag, bucket_name, bucket_id, obj_key, null_yield, invalidStr);
                                            block_func(dpp, key, offset, len, version, dirty, null_yield, localWeightStr);
                                            parsed = true;
                                        }
                                    } else if (parts.size() == 3) {
                                        offset = std::stoull(parts[1]);
                                        ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): offset: " << offset << dendl;
                                        len = std::stoull(parts[2]);
                                        ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): len: " << len << dendl;
                                        key = key + CACHE_DELIM + std::to_string(offset) + CACHE_DELIM + std::to_string(len);
                                        ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): key: " << key << dendl;
                                        std::string localWeightStr;
                                        auto ret = get_attr(dpp, file_entry.path(), RGW_CACHE_ATTR_LOCAL_WEIGHT, localWeightStr, null_yield);
                                        if (ret < 0) {
                                            ldpp_dout(dpp, 0) << "SSDCache: " << __func__ << "(): Failed to get attr: " << RGW_CACHE_ATTR_LOCAL_WEIGHT << dendl;
                                        } else {
                                            ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): localWeightStr: " << localWeightStr << dendl;
                                        }
                                        block_func(dpp, key, offset, len, version, dirty, null_yield, localWeightStr);
                                        parsed = true;
                                    }
                                    if (!parsed) {
                                        ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): Unable to parse file_name: " << file_name << dendl;
                                        continue;
                                    }
                                }
                            }
                        } catch(...) {
                            ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): Exception while parsing entry: " << file_entry.path() << dendl;
                            continue;
                        }
                    }
                }
            }
        }
    }

    return 0;
}

uint64_t SSDDriver::get_free_space(const DoutPrefixProvider* dpp, optional_yield y)
{
    efs::space_info space = efs::space(partition_info.location);
    return (space.available < partition_info.reserve_size) ? 0 : (space.available - partition_info.reserve_size);
}

void SSDDriver::set_free_space(const DoutPrefixProvider* dpp, uint64_t free_space)
{
    std::lock_guard l(cache_lock);
    this->free_space = free_space;
}

int SSDDriver::put(const DoutPrefixProvider* dpp, const std::string& key, const bufferlist& bl, uint64_t len, const rgw::sal::Attrs& attrs, optional_yield y)
{
    ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): key=" << key << dendl;
    boost::system::error_code ec;
    if (y) {
        using namespace boost::asio;
        yield_context yield = y.get_yield_context();
        auto ex = yield.get_executor();
        this->put_async(dpp, ex, key, bl, len, attrs, yield[ec]);
    } else {
      auto ex = boost::asio::system_executor{};
      this->put_async(dpp, ex, key, bl, len, attrs, ceph::async::use_blocked[ec]);
    }
    if (ec) {
        return ec.value();
    }
    return 0;
}

int SSDDriver::get(const DoutPrefixProvider* dpp, const std::string& key, off_t offset, uint64_t len, bufferlist& bl, rgw::sal::Attrs& attrs, optional_yield y)
{
    char buffer[len];
    std::string location = create_dirs_get_filepath_from_key(dpp, partition_info.location, key);
    ldpp_dout(dpp, 20) << __func__ << "(): location=" << location << dendl;
    FILE *cache_file = nullptr;
    int r = 0;
    size_t nbytes = 0;

    cache_file = fopen(location.c_str(), "r+");
    if (cache_file == nullptr) {
        ldpp_dout(dpp, 0) << "ERROR: get::fopen file has return error, errno=" << errno << dendl;
        return -errno;
    }

    fseek(cache_file, offset, SEEK_SET);

    nbytes = fread(buffer, 1, len, cache_file);
    if (nbytes != len) {
        fclose(cache_file);
        ldpp_dout(dpp, 0) << "ERROR: get::io_read: fread has returned error: nbytes!=len, nbytes=" << nbytes << ", len=" << len << dendl;
        return -EIO;
    }

    r = fclose(cache_file);
    if (r != 0) {
        ldpp_dout(dpp, 0) << "ERROR: get::fclose file has return error, errno=" << errno << dendl;
        return -errno;
    }

    bl.append(buffer, len);

    r = get_attrs(dpp, key, attrs, y);
    if (r < 0) {
        ldpp_dout(dpp, 0) << "ERROR: get::get_attrs: failed to get attrs, r = " << r << dendl;
        return r;
    }

    return 0;
}

int SSDDriver::append_data(const DoutPrefixProvider* dpp, const::std::string& key, const bufferlist& bl_data, optional_yield y)
{
    bufferlist src = bl_data;
    std::string location = create_dirs_get_filepath_from_key(dpp, partition_info.location, key);

    ldpp_dout(dpp, 20) << __func__ << "(): location=" << location << dendl;
    FILE *cache_file = nullptr;
    int r = 0;
    size_t nbytes = 0;

    cache_file = fopen(location.c_str(), "a+");
    if (cache_file == nullptr) {
        ldpp_dout(dpp, 0) << "ERROR: put::fopen file has return error, errno=" << errno << dendl;
        return -errno;
    }

    nbytes = fwrite(src.c_str(), 1, src.length(), cache_file);
    if (nbytes != src.length()) {
        ldpp_dout(dpp, 0) << "ERROR: append_data: fwrite has returned error: nbytes!=len, nbytes=" << nbytes << ", len=" << bl_data.length() << dendl;
        return -EIO;
    }

    r = fclose(cache_file);
    if (r != 0) {
        ldpp_dout(dpp, 0) << "ERROR: append_data::fclose file has return error, errno=" << errno << dendl;
        return -errno;
    }
    std::lock_guard l(cache_lock);
    efs::space_info space = efs::space(partition_info.location);
    this->free_space = space.available;

    return 0;
}

// Template create functions for async operations
#if defined(HAVE_LIBURING)
template <typename Executor1, typename CompletionHandler>
auto SSDDriver::IoUringAsyncReadOp::create(const Executor1& ex1, CompletionHandler&& handler)
{
    auto p = Completion::create(ex1, std::move(handler));
    return p;
}

template <typename Executor1, typename CompletionHandler>
auto SSDDriver::IoUringAsyncWriteRequest::create(const Executor1& ex1, CompletionHandler&& handler)
{
    auto p = Completion::create(ex1, std::move(handler));
    return p;
}
#endif

template <typename Executor1, typename CompletionHandler>
auto SSDDriver::LibaioAsyncReadOp::create(const Executor1& ex1, CompletionHandler&& handler)
{
    auto p = Completion::create(ex1, std::move(handler));
    return p;
}

template <typename Executor1, typename CompletionHandler>
auto SSDDriver::LibaioAsyncWriteRequest::create(const Executor1& ex1, CompletionHandler&& handler)
{
    auto p = Completion::create(ex1, std::move(handler));
    return p;
}

#if defined(HAVE_LIBURING)
// io_uring implementation

template <typename Executor, typename CompletionToken>
auto SSDDriver::get_async_uring(const DoutPrefixProvider *dpp, const Executor& ex, const std::string& key,
                off_t read_ofs, off_t read_len, CompletionToken&& token)
{
  using Op = IoUringAsyncReadOp;
  using Signature = typename Op::Signature;
  return boost::asio::async_initiate<CompletionToken, Signature>(
      [this] (auto handler, const DoutPrefixProvider *dpp,
              const Executor& ex, const std::string& key,
              off_t read_ofs, off_t read_len) {
    struct ReadCqeHandler final : IoUringCqeHandler {
        std::unique_ptr<Op::Completion> completion;

        explicit ReadCqeHandler(std::unique_ptr<Op::Completion>&& p)
            : completion(std::move(p))
        {
        }

        void complete_from_cqe(const io_uring_cqe& cqe) override
        {
            io_uring_cqe cqe_copy = cqe;
            auto& op = completion->user_data;
            auto ec = Op::io_uring_read_completion(&cqe_copy, &op);
            bufferlist result = std::move(op.result);
            ceph::async::post(std::move(completion), ec, std::move(result));
        }

        void complete_from_error(int err) override
        {
            io_uring_cqe fake_cqe{};
            fake_cqe.res = -err;
            auto& op = completion->user_data;
            auto ec = Op::io_uring_read_completion(&fake_cqe, &op);
            ceph::async::post(std::move(completion), ec, bufferlist{});
        }
    };

    auto p = Op::create(ex, handler);
    auto& op = p->user_data;
    op.buffer_pool = &buffer_pool_;

    std::string location = create_dirs_get_filepath_from_key(dpp, partition_info.location, key);
    ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): location=" << location << dendl;

    int ring_ret = ensure_thread_uring(dpp, nullptr);
    if (ring_ret < 0) {
        ldpp_dout(dpp, 0) << "ERROR: get_async_uring::ensure_thread_uring failed: " << ring_ret << dendl;
        auto ec = boost::system::error_code{-ring_ret, boost::system::system_category()};
        ceph::async::post(std::move(p), ec, bufferlist{});
        return;
    }

    auto state = get_thread_uring_state();
    int watcher_ret = ensure_thread_uring_reaper(state, ex);
    if (watcher_ret < 0) {
        auto ec = boost::system::error_code{-watcher_ret, boost::system::system_category()};
        ceph::async::post(std::move(p), ec, bufferlist{});
        return;
    }

    int ret = op.prepare_io_uring_read_op(dpp, location, read_ofs, read_len);
    if (ret < 0) {
        auto ec = boost::system::error_code{-ret, boost::system::system_category()};
        ceph::async::post(std::move(p), ec, bufferlist{});
        return;
    }

    auto cqe_handler = std::make_unique<ReadCqeHandler>(std::move(p));
    int submit_ret = 0;
    {
        std::lock_guard<std::mutex> lock(state->ring_mutex);
        io_uring_sqe* sqe = io_uring_get_sqe(&state->ring);
        if (!sqe) {
            submit_ret = -EAGAIN;
        } else {
            io_uring_prep_read(sqe, op.fd, op.buffer, op.length, op.offset);
            io_uring_sqe_set_data(sqe, cqe_handler.get());
            submit_ret = io_uring_submit(&state->ring);
            if (submit_ret == 0) {
                submit_ret = -EAGAIN;
            }
        }
    }
    if (submit_ret < 0) {
        cqe_handler->complete_from_error(-submit_ret);
        return;
    }
    // coverity[leaked_storage:SUPPRESS]
    (void)cqe_handler.release();
  }, token, dpp, ex, key, read_ofs, read_len);
}

template <typename Executor, typename CompletionToken>
void SSDDriver::put_async_uring(const DoutPrefixProvider *dpp, const Executor& ex, const std::string& key,
                const bufferlist& bl, uint64_t len, const rgw::sal::Attrs& attrs, CompletionToken&& token)
{
  using Op = IoUringAsyncWriteRequest;
  using Signature = typename Op::Signature;
  return boost::asio::async_initiate<CompletionToken, Signature>(
      [this] (auto handler, const DoutPrefixProvider *dpp,
              const Executor& ex, const std::string& key, const bufferlist& bl,
              uint64_t len, const rgw::sal::Attrs& attrs) {
    struct WriteCqeHandler final : IoUringCqeHandler {
        std::unique_ptr<Op::Completion> completion;

        explicit WriteCqeHandler(std::unique_ptr<Op::Completion>&& p)
            : completion(std::move(p))
        {
        }

        void complete_from_cqe(const io_uring_cqe& cqe) override
        {
            io_uring_cqe cqe_copy = cqe;
            auto& op = completion->user_data;
            auto ec = Op::io_uring_write_completion(&cqe_copy, &op);
            ceph::async::dispatch(std::move(completion), ec);
        }

        void complete_from_error(int err) override
        {
            io_uring_cqe fake_cqe{};
            fake_cqe.res = -err;
            auto& op = completion->user_data;
            auto ec = Op::io_uring_write_completion(&fake_cqe, &op);
            ceph::async::dispatch(std::move(completion), ec);
        }
    };

    auto p = Op::create(ex, handler);
    auto& op = p->user_data;
    op.buffer_pool = &buffer_pool_;

    op.file_path = create_dirs_get_filepath_from_key(dpp, partition_info.location, key);
    ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): op.file_path=" << op.file_path << dendl;

    op.temp_file_path = create_dirs_get_filepath_from_key(dpp, partition_info.location, key, true);
    ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): op.temp_file_path=" << op.temp_file_path << dendl;

    bufferlist src = bl;
    int ring_ret = ensure_thread_uring(dpp, nullptr);
    if (ring_ret < 0) {
        ldpp_dout(dpp, 0) << "ERROR: put_async_uring::ensure_thread_uring failed: " << ring_ret << dendl;
        auto ec = boost::system::error_code{-ring_ret, boost::system::system_category()};
        ceph::async::dispatch(std::move(p), ec);
        return;
    }

    auto state = get_thread_uring_state();
    int watcher_ret = ensure_thread_uring_reaper(state, ex);
    if (watcher_ret < 0) {
        auto ec = boost::system::error_code{-watcher_ret, boost::system::system_category()};
        ceph::async::dispatch(std::move(p), ec);
        return;
    }

    int r = op.prepare_io_uring_write_op(dpp, src, len, op.temp_file_path);
    op.dpp = dpp;
    op.priv_data = this;
    op.attrs = attrs;
    if (r < 0) {
        ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): ::prepare_io_uring_write_op(), r=" << r << dendl;
        auto ec = boost::system::error_code{-r, boost::system::system_category()};
        ceph::async::dispatch(std::move(p), ec);
        return;
    }

    auto cqe_handler = std::make_unique<WriteCqeHandler>(std::move(p));
    int submit_ret = 0;
    {
        std::lock_guard<std::mutex> lock(state->ring_mutex);
        io_uring_sqe* sqe = io_uring_get_sqe(&state->ring);
        if (!sqe) {
            submit_ret = -EAGAIN;
        } else {
            io_uring_prep_write(sqe, op.fd, op.data, op.length, 0);
            io_uring_sqe_set_data(sqe, cqe_handler.get());
            submit_ret = io_uring_submit(&state->ring);
            if (submit_ret == 0) {
                submit_ret = -EAGAIN;
            }
        }
    }
    if (submit_ret < 0) {
        cqe_handler->complete_from_error(-submit_ret);
        return;
    }
    // coverity[leaked_storage:SUPPRESS]
    (void)cqe_handler.release();
  }, token, dpp, ex, key, bl, len, attrs);
}

// io_uring read completion handler
boost::system::error_code SSDDriver::IoUringAsyncReadOp::io_uring_read_completion(struct io_uring_cqe* cqe, IoUringAsyncReadOp* op)
{
    boost::system::error_code ec;
    int ret = cqe->res;
    if (ret < 0) {
        ec.assign(-ret, boost::system::system_category());
    }
    // If read was successful, append data to result bufferlist
    if (ret > 0 && op->buffer) {
        if (op->direct_io && op->offset != op->orig_offset) {
            // With O_DIRECT we may have read from an earlier aligned offset.
            // Skip the leading padding bytes and trim to the originally requested length.
            size_t skip = (size_t)(op->orig_offset - op->offset);
            size_t available = (size_t)ret > skip ? (size_t)ret - skip : 0;
            size_t to_copy = std::min(available, op->orig_length);
            if (to_copy > 0) {
                op->result.append(static_cast<const char*>(op->buffer) + skip, to_copy);
            }
        } else if (op->direct_io) {
            // Offset was already aligned but we may have read more than requested
            size_t to_copy = std::min((size_t)ret, op->orig_length);
            op->result.append(static_cast<const char*>(op->buffer), to_copy);
        } else {
            op->result.append(static_cast<const char*>(op->buffer), ret);
        }
    }
    // Resource cleanup
    if (op->fd >= 0) {
        ::close(op->fd);
        op->fd = -1;
    }
    if (op->buffer) {
        if (op->buffer_pool && op->length > 0) {
            op->buffer_pool->deallocate(op->buffer, op->length);
        } else {
            ::free(op->buffer);
        }
        op->buffer = nullptr;
    }
    return ec;
}

// Prepare an io_uring read operation
int SSDDriver::IoUringAsyncReadOp::prepare_io_uring_read_op(
    const DoutPrefixProvider *dpp,
    const std::string& file_path,
    off_t read_ofs,
    size_t read_len)
{
    ldpp_dout(dpp, 20) << "SSDCache: IoUringAsyncReadOp::prepare_io_uring_read_op(): file_path=" << file_path << dendl;

    // Use O_DIRECT when configured for better NVMe performance
    int open_flags = O_RDONLY;
    direct_io = dpp->get_cct()->_conf->rgw_d4n_io_uring_direct_io;
    if (direct_io) {
        open_flags |= O_DIRECT;
    }

    fd = TEMP_FAILURE_RETRY(::open(file_path.c_str(), open_flags));
    if (fd < 0) {
        // If O_DIRECT fails, retry without it (may fail on some filesystems)
        if (direct_io && errno == EINVAL) {
            fd = TEMP_FAILURE_RETRY(::open(file_path.c_str(), O_RDONLY));
            if (fd >= 0) {
                direct_io = false;
            }
        }
        if (fd < 0) {
            ldpp_dout(dpp, 0) << "ERROR: IoUringAsyncReadOp::prepare_io_uring_read_op: open file failed, errno=" << errno << ", location='" << file_path << "'" << dendl;
            return -errno;
        }
    }
    if (!direct_io && dpp->get_cct()->_conf->rgw_d4n_l1_fadvise != POSIX_FADV_NORMAL)
        posix_fadvise(fd, 0, 0, dpp->get_cct()->_conf->rgw_d4n_l1_fadvise);

    // Save original requested offset/length before alignment
    orig_offset = read_ofs;
    orig_length = read_len;

    // For O_DIRECT, offset, length, and buffer must all be aligned
    off_t aligned_ofs = read_ofs;
    size_t aligned_len = read_len;
    if (direct_io) {
        aligned_ofs = read_ofs & ~(off_t)(IO_BUFFER_ALIGNMENT - 1);
        off_t end = (off_t)(read_ofs + read_len);
        off_t aligned_end = (off_t)align_size((size_t)end);
        aligned_len = (size_t)(aligned_end - aligned_ofs);
    }

    buffer = buffer_pool ? buffer_pool->allocate(aligned_len) : nullptr;
    if (!buffer) {
        if (posix_memalign(&buffer, IO_BUFFER_ALIGNMENT, align_size(aligned_len)) != 0) {
            ldpp_dout(dpp, 0) << "ERROR: IoUringAsyncReadOp::prepare_io_uring_read_op: memory allocation failed" << dendl;
            ::close(fd);
            fd = -1;
            return -ENOMEM;
        }
    }
    offset = aligned_ofs;
    length = aligned_len;
    return 0;
}

// io_uring write completion handler
boost::system::error_code SSDDriver::IoUringAsyncWriteRequest::io_uring_write_completion(struct io_uring_cqe* cqe, IoUringAsyncWriteRequest* op)
{
    boost::system::error_code ec;
    int ret = cqe->res;
    if (ret < 0) {
        ec.assign(-ret, boost::system::system_category());
        ldpp_dout(op->dpp, 0) << "ERROR: io_uring_write_completion: I/O write failed, ret=" << ret << dendl;
        // Skip post-I/O work (set_attrs, rename) on write failure;
        // fall through to resource cleanup below
    } else {
        // With O_DIRECT we wrote an aligned (possibly larger) length.
        // Truncate the file to the real data size before rename.
        if (op->direct_io && op->orig_length < op->length && op->fd >= 0) {
            if (::ftruncate(op->fd, op->orig_length) < 0) {
                ldpp_dout(op->dpp, 0) << "ERROR: io_uring_write_completion: ftruncate failed, errno=" << errno << dendl;
                ec.assign(errno, boost::system::system_category());
            }
        }
        int attr_ret = 0;
        if (op->attrs.size() > 0) {
            optional_yield y{null_yield};
            attr_ret = op->priv_data->set_attrs(op->dpp, op->temp_file_path, op->attrs, y);
            if (attr_ret < 0) {
                ldpp_dout(op->dpp, 0) << "ERROR: io_uring_write_completion::set_attrs: failed to set attrs, ret = " << attr_ret << dendl;
                if (!ec) {
                    ec.assign(-attr_ret, boost::system::system_category());
                }
            }
        }
        Partition partition_info = op->priv_data->get_current_partition_info(op->dpp);
        efs::space_info space = efs::space(partition_info.location);
        op->priv_data->set_free_space(op->dpp, space.available);
        ldpp_dout(op->dpp, 20) << "INFO: io_uring_write_completion: new_path: " << op->file_path << dendl;
        ldpp_dout(op->dpp, 20) << "INFO: io_uring_write_completion: old_path: " << op->temp_file_path << dendl;
        ret = std::rename(op->temp_file_path.c_str(), op->file_path.c_str());
        if (ret < 0) {
            ret = errno;
            ldpp_dout(op->dpp, 0) << "ERROR: put::rename: failed to rename file: " << ret << dendl;
            if (!ec) {
                ec.assign(ret, boost::system::system_category());
            }
        }
    }

    if (op->fd >= 0) {
        ::close(op->fd);
        op->fd = -1;
    }
    if (op->data) {
        if (op->buffer_pool && op->length > 0) {
            op->buffer_pool->deallocate(op->data, op->length);
        } else {
            ::free(op->data);
        }
        op->data = nullptr;
    }
    return ec;
}

// Prepare an io_uring write operation
int SSDDriver::IoUringAsyncWriteRequest::prepare_io_uring_write_op(const DoutPrefixProvider *dpp, bufferlist& bl, unsigned int len, std::string file_path)
{
    ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): Write To Cache, location=" << file_path << dendl;
    mode_t mode = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;

    // Build open flags with optional O_DIRECT for NVMe optimization
    int open_flags = O_WRONLY | O_CREAT | O_TRUNC | dpp->get_cct()->_conf->rgw_d4n_l1_write_open_flags;
    direct_io = dpp->get_cct()->_conf->rgw_d4n_io_uring_direct_io;
    if (direct_io) {
        open_flags |= O_DIRECT;
    }

    fd = TEMP_FAILURE_RETRY(::open(file_path.c_str(), open_flags, mode));
    if (fd < 0) {
        int saved_errno = errno;
        // If O_DIRECT fails, retry without it
        if (direct_io && saved_errno == EINVAL) {
            open_flags &= ~O_DIRECT;
            direct_io = false;
            fd = TEMP_FAILURE_RETRY(::open(file_path.c_str(), open_flags, mode));
            saved_errno = errno;
        }
        // directories might have been deleted by a parallel delete of the last version of an object
        if (fd < 0 && saved_errno == ENOENT) {
            // retry after creating directories
            std::string dir_path = file_path;
            auto pos = dir_path.find_last_of('/');
            if (pos != std::string::npos) {
                dir_path.erase(pos, (dir_path.length() - pos));
            }
            ldpp_dout(dpp, 20) << "INFO: IoUringAsyncWriteRequest::prepare_io_uring_write_op: dir_path for creating directories=" << dir_path << dendl;
            create_directories(dpp, dir_path);
            fd = TEMP_FAILURE_RETRY(::open(file_path.c_str(), open_flags, mode));
            if (fd < 0) {
                ldpp_dout(dpp, 0) << "ERROR: IoUringAsyncWriteRequest::prepare_io_uring_write_op: open file failed, errno=" << errno << ", location='" << file_path.c_str() << "'" << dendl;
                return -errno;
            }
        } else if (fd < 0) {
            ldpp_dout(dpp, 0) << "ERROR: IoUringAsyncWriteRequest::prepare_io_uring_write_op: open file failed, errno=" << errno << ", location='" << file_path.c_str() << "'" << dendl;
            return -errno;
        }
    }
    if (!direct_io && dpp->get_cct()->_conf->rgw_d4n_l1_fadvise != POSIX_FADV_NORMAL)
        posix_fadvise(fd, 0, 0, dpp->get_cct()->_conf->rgw_d4n_l1_fadvise);

    // Save original length; for O_DIRECT the I/O length must be aligned
    orig_length = len;
    size_t io_len = direct_io ? align_size(len) : len;

    data = buffer_pool ? buffer_pool->allocate(io_len) : nullptr;
    if (!data) {
        if (posix_memalign(&data, IO_BUFFER_ALIGNMENT, align_size(io_len)) != 0) {
            ldpp_dout(dpp, 0) << "ERROR: IoUringAsyncWriteRequest::prepare_io_uring_write_op: memory allocation failed" << dendl;
            if (fd >= 0) {
                ::close(fd);
                fd = -1;
            }
            return -ENOMEM;
        }
    }
    // Zero the padding bytes beyond the real data for O_DIRECT aligned writes
    if (io_len > len) {
        memset(static_cast<char*>(data) + len, 0, io_len - len);
    }
    memcpy(data, bl.c_str(), len);
    length = io_len;
    return 0;
}
#endif // HAVE_LIBURING

// libaio implementation (always available)

template <typename Executor, typename CompletionToken>
auto SSDDriver::get_async_libaio(const DoutPrefixProvider *dpp, const Executor& ex, const std::string& key,
                off_t read_ofs, off_t read_len, CompletionToken&& token)
{
  using Op = LibaioAsyncReadOp;
  using Signature = typename Op::Signature;
  return boost::asio::async_initiate<CompletionToken, Signature>(
      [this] (auto handler, const DoutPrefixProvider *dpp,
              const Executor& ex, const std::string& key,
              off_t read_ofs, off_t read_len) {
    auto p = Op::create(ex, handler);
    auto& op = p->user_data;

    std::string location = create_dirs_get_filepath_from_key(dpp, partition_info.location, key);
    ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): location=" << location << dendl;

    int ret = op.prepare_libaio_read_op(dpp, location, read_ofs, read_len, p.get());
    if(0 == ret) {
        ret = ::aio_read(op.aio_cb.get());
    }
    ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): ::aio_read(), ret=" << ret << dendl;
    if(ret < 0) {
        auto ec = boost::system::error_code{-ret, boost::system::system_category()};
        ceph::async::post(std::move(p), ec, bufferlist{});
    } else {
        // coverity[leaked_storage:SUPPRESS]
        (void)p.release();
    }
  }, token, dpp, ex, key, read_ofs, read_len);
}

template <typename Executor, typename CompletionToken>
void SSDDriver::put_async_libaio(const DoutPrefixProvider *dpp, const Executor& ex, const std::string& key,
                const bufferlist& bl, uint64_t len, const rgw::sal::Attrs& attrs, CompletionToken&& token)
{
  using Op = LibaioAsyncWriteRequest;
  using Signature = typename Op::Signature;
  return boost::asio::async_initiate<CompletionToken, Signature>(
      [this] (auto handler, const DoutPrefixProvider *dpp,
              const Executor& ex, const std::string& key, const bufferlist& bl,
              uint64_t len, const rgw::sal::Attrs& attrs) {
    auto p = Op::create(ex, handler);
    auto& op = p->user_data;

    op.file_path = create_dirs_get_filepath_from_key(dpp, partition_info.location, key);
    ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): op.file_path=" << op.file_path << dendl;

    op.temp_file_path = create_dirs_get_filepath_from_key(dpp, partition_info.location, key, true);
    ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): op.temp_file_path=" << op.temp_file_path << dendl;

    int r = 0;
    bufferlist src = bl;
    r = op.prepare_libaio_write_op(dpp, src, len, op.temp_file_path);
    op.cb->aio_sigevent.sigev_notify = SIGEV_THREAD;
    op.cb->aio_sigevent.sigev_notify_function = SSDDriver::LibaioAsyncWriteRequest::libaio_write_cb;
    op.cb->aio_sigevent.sigev_notify_attributes = nullptr;
    op.cb->aio_sigevent.sigev_value.sival_ptr = (void*)p.get();
    op.dpp = dpp;
    op.priv_data = this;
    op.attrs = std::move(attrs);
    bool prepare_succeeded = (r >= 0);
    if (prepare_succeeded) {
        r = ::aio_write(op.cb.get());
    } else {
        ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): ::prepare_libaio_write_op(), r=" << r << dendl;
    }

    ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): ::aio_write(), r=" << r << dendl;
    if(r < 0) {
        // If prepare succeeded but aio_write failed, we need to free the data buffer
        // (libaio_aiocb_deleter will close the fd, but doesn't free the data)
        if (prepare_succeeded && op.data) {
            ::free(op.data);
            op.data = nullptr;
        }
        auto ec = boost::system::error_code{-r, boost::system::system_category()};
        ceph::async::dispatch(std::move(p), ec);
    } else {
        (void)p.release();
    }
  }, token, dpp, ex, key, bl, len, attrs);
}

int SSDDriver::LibaioAsyncWriteRequest::prepare_libaio_write_op(const DoutPrefixProvider *dpp, bufferlist& bl, unsigned int len, std::string file_path)
{
    int r = 0;
    ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): Write To Cache, location=" << file_path << dendl;
    cb.reset(new struct aiocb);
    memset(cb.get(), 0, sizeof(struct aiocb));
    mode_t mode = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;
    r = fd = TEMP_FAILURE_RETRY(::open(file_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC | dpp->get_cct()->_conf->rgw_d4n_l1_write_open_flags, mode));
    if (fd < 0) {
        if (errno == ENOENT) {
            std::string dir_path = file_path;
            auto pos = dir_path.find_last_of('/');
            if (pos != std::string::npos) {
                dir_path.erase(pos, (dir_path.length() - pos));
            }
            ldpp_dout(dpp, 20) << "INFO: LibaioAsyncWriteRequest::prepare_libaio_write_op: dir_path for creating directories=" << dir_path << dendl;
            create_directories(dpp, dir_path);
            r = fd = TEMP_FAILURE_RETRY(::open(file_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC | dpp->get_cct()->_conf->rgw_d4n_l1_write_open_flags, mode));
            if (fd < 0) {
                ldpp_dout(dpp, 0) << "ERROR: LibaioAsyncWriteRequest::prepare_libaio_write_op: open file failed, errno=" << errno << ", location='" << file_path.c_str() << "'" << dendl;
                return r;
            }
        } else {
            ldpp_dout(dpp, 0) << "ERROR: LibaioAsyncWriteRequest::prepare_libaio_write_op: open file failed, errno=" << errno << ", location='" << file_path.c_str() << "'" << dendl;
            return r;
        }
    }
    if (dpp->get_cct()->_conf->rgw_d4n_l1_fadvise != POSIX_FADV_NORMAL)
        posix_fadvise(fd, 0, 0, dpp->get_cct()->_conf->rgw_d4n_l1_fadvise);
    cb->aio_fildes = fd;

    data = malloc(len);
    if (!data) {
        ldpp_dout(dpp, 0) << "ERROR: LibaioAsyncWriteRequest::prepare_libaio_write_op: memory allocation failed" << dendl;
        ::close(fd);
        return r;
    }
    cb->aio_buf = data;
    memcpy((void*)data, bl.c_str(), len);
    cb->aio_nbytes = len;
    return r;
}

void SSDDriver::LibaioAsyncWriteRequest::libaio_write_cb(sigval sigval) {
    auto p = std::unique_ptr<Completion>{static_cast<Completion*>(sigval.sival_ptr)};
    auto op = std::move(p->user_data);
    ldpp_dout(op.dpp, 20) << "INFO: LibaioAsyncWriteRequest::libaio_write_cb: key: " << op.file_path << dendl;
    int ret = -aio_error(op.cb.get());
    boost::system::error_code ec;
    if (ret < 0) {
        ec.assign(-ret, boost::system::system_category());
        // Free the data buffer before returning
        if (op.data) {
            ::free(op.data);
            op.data = nullptr;
        }
        ceph::async::dispatch(std::move(p), ec);
        return;
    }
    int attr_ret = 0;
    if (op.attrs.size() > 0) {
        optional_yield y{null_yield};
        attr_ret = op.priv_data->set_attrs(op.dpp, op.temp_file_path, op.attrs, y);
        if (attr_ret < 0) {
            ldpp_dout(op.dpp, 0) << "ERROR: LibaioAsyncWriteRequest::libaio_write_cb::set_attrs: failed to set attrs, ret = " << attr_ret << dendl;
            ec.assign(-ret, boost::system::system_category());
            // Free the data buffer before returning
            if (op.data) {
                ::free(op.data);
                op.data = nullptr;
            }
            ceph::async::dispatch(std::move(p), ec);
            return;
        }
    }

    Partition partition_info = op.priv_data->get_current_partition_info(op.dpp);
    efs::space_info space = efs::space(partition_info.location);
    op.priv_data->set_free_space(op.dpp, space.available);

    ldpp_dout(op.dpp, 20) << "INFO: LibaioAsyncWriteRequest::libaio_write_cb: new_path: " << op.file_path << dendl;
    ldpp_dout(op.dpp, 20) << "INFO: LibaioAsyncWriteRequest::libaio_write_cb: old_path: " << op.temp_file_path << dendl;

    ret = std::rename(op.temp_file_path.c_str(), op.file_path.c_str());
    if (ret < 0) {
        ret = errno;
        ldpp_dout(op.dpp, 0) << "ERROR: put::rename: failed to rename file: " << ret << dendl;
        ec.assign(-ret, boost::system::system_category());
    }
    // Free the data buffer before returning
    if (op.data) {
        ::free(op.data);
        op.data = nullptr;
    }
    ceph::async::dispatch(std::move(p), ec);
}

int SSDDriver::LibaioAsyncReadOp::prepare_libaio_read_op(const DoutPrefixProvider *dpp, const std::string& file_path, off_t read_ofs, off_t read_len, void* arg)
{
    ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): file_path=" << file_path << dendl;
    aio_cb.reset(new struct aiocb);
    memset(aio_cb.get(), 0, sizeof(struct aiocb));
    aio_cb->aio_fildes = TEMP_FAILURE_RETRY(::open(file_path.c_str(), O_RDONLY|O_CLOEXEC|O_BINARY));
    if(aio_cb->aio_fildes < 0) {
        int err = errno;
        ldpp_dout(dpp, 1) << "ERROR: SSDCache: " << __func__ << "(): can't open " << file_path << " : " << " error: " << err << dendl;
        return -err;
    }
    if (dpp->get_cct()->_conf->rgw_d4n_l1_fadvise != POSIX_FADV_NORMAL) {
        posix_fadvise(aio_cb->aio_fildes, 0, 0, g_conf()->rgw_d4n_l1_fadvise);
    }

    bufferptr bp(read_len);
    aio_cb->aio_buf = bp.c_str();
    result.append(std::move(bp));

    aio_cb->aio_nbytes = read_len;
    aio_cb->aio_offset = read_ofs;
    aio_cb->aio_sigevent.sigev_notify = SIGEV_THREAD;
    aio_cb->aio_sigevent.sigev_notify_function = libaio_cb_aio_dispatch;
    aio_cb->aio_sigevent.sigev_notify_attributes = nullptr;
    aio_cb->aio_sigevent.sigev_value.sival_ptr = arg;

    return 0;
}

void SSDDriver::LibaioAsyncReadOp::libaio_cb_aio_dispatch(sigval sigval)
{
    auto p = std::unique_ptr<Completion>{static_cast<Completion*>(sigval.sival_ptr)};
    auto op = std::move(p->user_data);
    const int ret = -aio_error(op.aio_cb.get());
    boost::system::error_code ec;
    if (ret < 0) {
        ec.assign(-ret, boost::system::system_category());
    }

    ceph::async::dispatch(std::move(p), ec, std::move(op.result));
}

// Dispatcher functions that select the appropriate backend based on use_io_uring_ flag

template <typename Executor, typename CompletionToken>
auto SSDDriver::get_async(const DoutPrefixProvider *dpp, const Executor& ex, const std::string& key,
                 off_t read_ofs, off_t read_len, CompletionToken&& token)
{
#if defined(HAVE_LIBURING)
    if (use_io_uring_) {
        return get_async_uring(dpp, ex, key, read_ofs, read_len, std::forward<CompletionToken>(token));
    }
#endif
    return get_async_libaio(dpp, ex, key, read_ofs, read_len, std::forward<CompletionToken>(token));
}

template <typename Executor, typename CompletionToken>
void SSDDriver::put_async(const DoutPrefixProvider *dpp, const Executor& ex, const std::string& key,
                 const bufferlist& bl, uint64_t len, const rgw::sal::Attrs& attrs, CompletionToken&& token)
{
#if defined(HAVE_LIBURING)
    if (use_io_uring_) {
        put_async_uring(dpp, ex, key, bl, len, attrs, std::forward<CompletionToken>(token));
        return;
    }
#endif
    put_async_libaio(dpp, ex, key, bl, len, attrs, std::forward<CompletionToken>(token));
}

rgw::Aio::OpFunc SSDDriver::ssd_cache_read_op(const DoutPrefixProvider *dpp, optional_yield y, rgw::cache::CacheDriver* cache_driver,
                                off_t read_ofs, off_t read_len, const std::string& key) {
  return [this, dpp, y, read_ofs, read_len, key] (Aio* aio, AioResult& r) mutable {
    ceph_assert(y);
    ldpp_dout(dpp, 20) << "SSDCache: cache_read_op(): Read From Cache, oid=" << r.obj.oid << dendl;

    using namespace boost::asio;
    yield_context yield = y.get_yield_context();
    auto ex = yield.get_executor();

    ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): key=" << key << dendl;
    this->get_async(dpp, ex, key, read_ofs, read_len, bind_executor(ex, SSDDriver::libaio_read_handler{aio, r}));
  };
}

rgw::Aio::OpFunc SSDDriver::ssd_cache_write_op(const DoutPrefixProvider *dpp, optional_yield y, rgw::cache::CacheDriver* cache_driver,
                                const bufferlist& bl, uint64_t len, const rgw::sal::Attrs& attrs, const std::string& key) {
  return [this, dpp, y, bl, len, attrs, key] (Aio* aio, AioResult& r) mutable {
    ceph_assert(y);
    ldpp_dout(dpp, 20) << "SSDCache: cache_write_op(): Write to Cache, oid=" << r.obj.oid << dendl;

    using namespace boost::asio;
    yield_context yield = y.get_yield_context();
    auto ex = yield.get_executor();

    ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): key=" << key << dendl;
    this->put_async(dpp, ex, key, bl, len, attrs, bind_executor(ex, SSDDriver::libaio_write_handler{aio, r}));
  };
}

rgw::AioResultList SSDDriver::get_async(const DoutPrefixProvider* dpp, optional_yield y, rgw::Aio* aio, const std::string& key, off_t ofs, uint64_t len, uint64_t cost, uint64_t id)
{
    rgw_raw_obj r_obj;
    r_obj.oid = key;
    return aio->get(r_obj, ssd_cache_read_op(dpp, y, this, ofs, len, key), cost, id);
}

rgw::AioResultList SSDDriver::put_async(const DoutPrefixProvider* dpp, optional_yield y, rgw::Aio* aio, const std::string& key, const bufferlist& bl, uint64_t len, const rgw::sal::Attrs& attrs, uint64_t cost, uint64_t id)
{
    rgw_raw_obj r_obj;
    r_obj.oid = key;
    return aio->get(r_obj, ssd_cache_write_op(dpp, y, this, bl, len, attrs, key), cost, id);
}

int SSDDriver::delete_data(const DoutPrefixProvider* dpp, const::std::string& key, optional_yield y)
{
    std::string dir_path, file_name;
    parse_key(dpp, partition_info.location, key, dir_path, file_name);
    std::string location = get_file_path(dpp, dir_path, file_name);
    ldpp_dout(dpp, 20) << "INFO: delete_data::file to remove: " << location << dendl;
    std::error_code ec;

    if (!efs::remove(location, ec)) {
        ldpp_dout(dpp, 0) << "ERROR: delete_data::remove has failed to remove the file: " << location << dendl;
        return -ec.value();
    }

    if (efs::is_empty(dir_path, ec)) {
        ldpp_dout(dpp, 20) << "INFO: delete_data::object directory to remove: " << dir_path << " :" << ec.value() << dendl;
        if (!efs::remove(dir_path, ec)) {
            ldpp_dout(dpp, 0) << "ERROR: delete_data::remove has failed to remove the directory: " << dir_path  << " :" << ec.value() << dendl;
        }
    }
    auto pos = dir_path.find_last_of('/');
    if (pos != std::string::npos) {
        dir_path.erase(pos, (dir_path.length() - pos));

        if (efs::is_empty(dir_path, ec)) {
            ldpp_dout(dpp, 20) << "INFO: delete_data::bucket directory to remove: " << dir_path << " :" << ec.value() << dendl;
            if (!efs::remove(dir_path, ec)) {
                ldpp_dout(dpp, 0) << "ERROR: delete_data::remove has failed to remove the directory: " << dir_path << " :" << ec.value() << dendl;
            }
        }
    }

    efs::space_info space = efs::space(partition_info.location);
    this->free_space = space.available;

    return 0;
}

int SSDDriver::rename(const DoutPrefixProvider* dpp, const::std::string& oldKey, const::std::string& newKey, optional_yield y)
{
    std::string old_file_path = create_dirs_get_filepath_from_key(dpp, partition_info.location, oldKey);
    std::string new_file_path = create_dirs_get_filepath_from_key(dpp, partition_info.location, newKey);
    int ret = std::rename(old_file_path.c_str(), new_file_path.c_str());
    if (ret < 0) {
        ldpp_dout(dpp, 0) << "SSDDriver: ERROR: failed to rename the file: " << old_file_path << dendl;
        return ret;
    }

    return 0;
}

int SSDDriver::update_attrs(const DoutPrefixProvider* dpp, const std::string& key, const rgw::sal::Attrs& attrs, optional_yield y)
{
    std::string location = create_dirs_get_filepath_from_key(dpp, partition_info.location, key);
    ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): location=" << location << dendl;

    for (auto& it : attrs) {
        std::string attr_name = it.first;
        std::string attr_val = it.second.to_str();
        auto ret = setxattr(location.c_str(), attr_name.c_str(), attr_val.c_str(), attr_val.size(), XATTR_REPLACE);
        if (ret < 0 && errno == ENODATA) {
            ret = setxattr(location.c_str(), attr_name.c_str(), attr_val.c_str(), attr_val.size(), XATTR_CREATE);
        }
        if (ret < 0) {
            ldpp_dout(dpp, 0) << "SSDCache: " << __func__ << "(): could not modify attr value for attr name: " << attr_name << " key: " << key << " ERROR: " << cpp_strerror(errno) <<dendl;
            return ret;
        }
    }

    efs::space_info space = efs::space(partition_info.location);
    this->free_space = space.available;
    return 0;
}

int SSDDriver::delete_attrs(const DoutPrefixProvider* dpp, const std::string& key, rgw::sal::Attrs& del_attrs, optional_yield y)
{
    std::string location = create_dirs_get_filepath_from_key(dpp, partition_info.location, key);
    ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): location=" << location << dendl;

    for (auto& it : del_attrs) {
        auto ret = delete_attr(dpp, key, it.first);
        if (ret < 0) {
            ldpp_dout(dpp, 0) << "SSDCache: " << __func__ << "(): could not remove attr value for attr name: " << it.first << " key: " << key << cpp_strerror(errno) << dendl;
            return ret;
        }
    }

    efs::space_info space = efs::space(partition_info.location);
    this->free_space = space.available;

    return 0;
}

int SSDDriver::get_attrs(const DoutPrefixProvider* dpp, const std::string& key, rgw::sal::Attrs& attrs, optional_yield y)
{
    std::string location;
    if(key.find(partition_info.location, 0) == 0) {
        location = key;
    } else {
        location = create_dirs_get_filepath_from_key(dpp, partition_info.location, key);
    }

    ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): location=" << location << dendl;

    char namebuf[64 * 1024];
    int ret;
    ssize_t buflen = listxattr(location.c_str(), namebuf, sizeof(namebuf));
    if (buflen < 0) {
        ret = errno;
        ldpp_dout(dpp, 0) << "ERROR: could not get attributes for key: " << key << ": " << ret << dendl;
        return -ret;
    }
    char *keyptr = namebuf;
    while (buflen > 0) {
        ssize_t keylen;

        keylen = strlen(keyptr) + 1;
        std::string attr_name(keyptr);
        std::string::size_type prefixloc = attr_name.find(RGW_ATTR_PREFIX);
        buflen -= keylen;
        keyptr += keylen;
        if (prefixloc == std::string::npos) {
            continue;
        }
        std::string attr_value;
        get_attr(dpp, location, attr_name, attr_value, y);
        bufferlist bl_value;
        bl_value.append(attr_value);
        attrs.emplace(std::move(attr_name), std::move(bl_value));
    }
    return 0;
}

int SSDDriver::set_attrs(const DoutPrefixProvider* dpp, const std::string& key, const rgw::sal::Attrs& attrs, optional_yield y)
{
    std::string location;
    if(key.find(partition_info.location, 0) == 0) {
        location = key;
    } else {
        location = create_dirs_get_filepath_from_key(dpp, partition_info.location, key);
    }

    ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): location=" << location << dendl;

    for (auto& [attr_name, attr_val_bl] : attrs) {
        ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): attr_name = " << attr_name << " attr_val_bl length: " << attr_val_bl.length() << dendl;
        if (attr_val_bl.length() != 0) {
            auto ret = set_attr(dpp, key, attr_name, attr_val_bl.to_str(), y);
            if (ret < 0) {
                ldpp_dout(dpp, 0) << "SSDCache: " << __func__ << "(): could not set attr value for attr name: " << attr_name << " key: " << key << cpp_strerror(errno) << dendl;
                return ret;
            }
        }
    }

    efs::space_info space = efs::space(partition_info.location);
    this->free_space = space.available;

    return 0;
}

int SSDDriver::get_attr(const DoutPrefixProvider* dpp, const std::string& key, const std::string& attr_name, std::string& attr_val, optional_yield y)
{
    std::string location;
    if(key.find(partition_info.location, 0) == 0) {
        location = key;
    } else {
        location = create_dirs_get_filepath_from_key(dpp, partition_info.location, key);
    }

    ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): location=" << location << dendl;
    ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): get_attr: key: " << attr_name << dendl;

    size_t buffer_size = 256;
    while (true) {
        attr_val.resize(buffer_size);
        ssize_t attr_size = getxattr(location.c_str(), attr_name.c_str(), attr_val.data(), attr_val.size());
        if (attr_size < 0) {
            if (errno == ERANGE) {
                attr_size = getxattr(location.c_str(), attr_name.c_str(), nullptr, 0);
                if (attr_size < 0) {
                    ldpp_dout(dpp, 0) << "ERROR: could not get attribute " << attr_name << ": " << cpp_strerror(errno) << dendl;
                    attr_val = "";
                    return errno;
                }
                if (attr_size == 0) {
                    ldpp_dout(dpp, 0) << "ERROR: no attribute value found for attr_name: " << attr_name << dendl;
                    attr_val = "";
                    return 0;
                }
                buffer_size = static_cast<size_t>(attr_size);
                continue;
            }
            ldpp_dout(dpp, 0) << "SSDCache: " << __func__ << "(): could not get attribute " << attr_name << ": " << cpp_strerror(errno) << dendl;
            attr_val = "";
            return errno;
        }
        if (attr_size == 0) {
            ldpp_dout(dpp, 0) << "ERROR: no attribute value found for attr_name: " << attr_name << dendl;
            attr_val = "";
            return 0;
        }
        ldpp_dout(dpp, 20) << "INFO: attr_size is: " << attr_size << dendl;
        attr_val.resize(static_cast<size_t>(attr_size));
        return 0;
    }
}

int SSDDriver::set_attr(const DoutPrefixProvider* dpp, const std::string& key, const std::string& attr_name, const std::string& attr_val, optional_yield y)
{
    std::string location;
    if(key.find(partition_info.location, 0) == 0) {
        location = key;
    } else {
        location = create_dirs_get_filepath_from_key(dpp, partition_info.location, key);
    }

    ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): location=" << location << dendl;

    if (attr_name == RGW_ATTR_ACL) {
      if (dpp->get_cct()->_conf->subsys.should_gather(ceph_subsys_rgw, 20)) {
        std::string policy_json;
        RGWAccessControlPolicy policy;
        bufferlist bl;
        bl.append(attr_val);
        auto bliter = bl.cbegin();
        try {
          policy.decode(bliter);
          Formatter *f = Formatter::create("json");
          policy.dump(f);
          std::stringstream ss;
          f->flush(ss);
          policy_json = ss.str();
          delete f;
        } catch (buffer::error& err) {
          ldpp_dout(dpp, 0) << "ERROR: decode policy failed" << err.what() << dendl;
          policy_json = "ERROR: decode policy failed";
        }
        ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): set_attr: key: " << attr_name << " val: " << policy_json << dendl;
      }
    } else {
      ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): set_attr: key: " << attr_name << " val: " << attr_val << dendl;
    }

    auto ret = setxattr(location.c_str(), attr_name.c_str(), attr_val.c_str(), attr_val.size(), 0);
    if (ret < 0) {
        ldpp_dout(dpp, 0) << "SSDCache: " << __func__ << "(): could not set attr value for attr name: " << attr_name << " key: " << key << cpp_strerror(errno) << dendl;
        return ret;
    }

    efs::space_info space = efs::space(partition_info.location);
    this->free_space = space.available;

    return 0;
}

int SSDDriver::delete_attr(const DoutPrefixProvider* dpp, const std::string& key, const std::string& attr_name)
{
    std::string location = create_dirs_get_filepath_from_key(dpp, partition_info.location, key);
    ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): location=" << location << dendl;

    auto ret = removexattr(location.c_str(), attr_name.c_str());
    if (ret < 0) {
        ldpp_dout(dpp, 0) << "SSDCache: " << __func__ << "(): could not remove attr value for attr name: " << attr_name << " key: " << key << cpp_strerror(errno) << dendl;
        return ret;
    }

    efs::space_info space = efs::space(partition_info.location);
    this->free_space = space.available;

    return 0;
}

} } // namespace rgw::cache
