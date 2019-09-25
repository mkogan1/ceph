// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/api/Config.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "test/rbd_mirror/test_mock_fixture.h"
#include "test/rbd_mirror/mock/MockContextWQ.h"
#include "test/rbd_mirror/mock/MockSafeTimer.h"
#include "tools/rbd_mirror/NamespaceReplayer.h"
#include "tools/rbd_mirror/ImageDeleter.h"
#include "tools/rbd_mirror/ImageMap.h"
#include "tools/rbd_mirror/InstanceWatcher.h"
#include "tools/rbd_mirror/InstanceReplayer.h"
#include "tools/rbd_mirror/MirrorStatusUpdater.h"
#include "tools/rbd_mirror/PoolWatcher.h"
#include "tools/rbd_mirror/ServiceDaemon.h"
#include "tools/rbd_mirror/Threads.h"

namespace librbd {

namespace {

struct MockTestImageCtx : public MockImageCtx {
  MockTestImageCtx(librbd::ImageCtx &image_ctx)
    : librbd::MockImageCtx(image_ctx) {
  }
};

} // anonymous namespace

} // namespace librbd

namespace rbd {
namespace mirror {

template <>
struct ImageDeleter<librbd::MockTestImageCtx> {
  static ImageDeleter* s_instance;

  static ImageDeleter* create(
      librados::IoCtx &ioctx, Threads<librbd::MockTestImageCtx> *threads,
      Throttler<librbd::MockTestImageCtx> *image_deletion_throttler,
      ServiceDaemon<librbd::MockTestImageCtx> *service_daemon) {
    ceph_assert(s_instance != nullptr);
    return s_instance;
  }

  MOCK_METHOD1(init, void(Context*));
  MOCK_METHOD1(shut_down, void(Context*));
  MOCK_METHOD2(print_status, void(Formatter*, std::stringstream*));

  ImageDeleter() {
    s_instance = this;
  }
};

ImageDeleter<librbd::MockTestImageCtx>* ImageDeleter<librbd::MockTestImageCtx>::s_instance = nullptr;

template<>
struct ImageMap<librbd::MockTestImageCtx> {
  static ImageMap* s_instance;

  static ImageMap *create(librados::IoCtx &ioctx,
                          Threads<librbd::MockTestImageCtx> *threads,
                          const std::string& instance_id,
                          image_map::Listener &listener) {
    ceph_assert(s_instance != nullptr);
    return s_instance;
  }

  MOCK_METHOD1(init, void(Context*));
  MOCK_METHOD1(shut_down, void(Context*));

  MOCK_METHOD1(update_instances_added, void(const std::vector<std::string>&));
  MOCK_METHOD1(update_instances_removed, void(const std::vector<std::string>&));

  MOCK_METHOD3(update_images_mock, void(const std::string&,
                                        const std::set<std::string>&,
                                        const std::set<std::string>&));
  void update_images(const std::string& mirror_uuid,
                     std::set<std::string>&& added,
                     std::set<std::string>&& removed) {
    update_images_mock(mirror_uuid, added, removed);
  }

  ImageMap() {
    s_instance = this;
  }
};

ImageMap<librbd::MockTestImageCtx>* ImageMap<librbd::MockTestImageCtx>::s_instance = nullptr;

template<>
struct InstanceReplayer<librbd::MockTestImageCtx> {
  static InstanceReplayer* s_instance;

  static InstanceReplayer* create(
      librados::IoCtx &local_io_ctx, const std::string &local_mirror_uuid,
      Threads<librbd::MockTestImageCtx> *threads,
      ServiceDaemon<librbd::MockTestImageCtx> *service_daemon,
      MirrorStatusUpdater<librbd::MockTestImageCtx>* local_status_updater,
      journal::CacheManagerHandler *cache_manager_handler) {
    ceph_assert(s_instance != nullptr);
    return s_instance;
  }

  MOCK_METHOD0(start, void());
  MOCK_METHOD0(stop, void());
  MOCK_METHOD0(restart, void());
  MOCK_METHOD0(flush, void());

  MOCK_METHOD1(stop, void(Context *));

  MOCK_METHOD2(print_status, void(Formatter*, std::stringstream*));

  MOCK_METHOD2(add_peer, void(const std::string&, librados::IoCtx&));

  MOCK_METHOD1(init, void(Context*));
  MOCK_METHOD1(shut_down, void(Context*));
  MOCK_METHOD1(release_all, void(Context*));

  InstanceReplayer() {
    s_instance = this;
  }
};

InstanceReplayer<librbd::MockTestImageCtx>* InstanceReplayer<librbd::MockTestImageCtx>::s_instance = nullptr;

template<>
struct InstanceWatcher<librbd::MockTestImageCtx> {
  static InstanceWatcher* s_instance;

  static InstanceWatcher* create(
      librados::IoCtx &ioctx, ContextWQ* work_queue,
      InstanceReplayer<librbd::MockTestImageCtx>* instance_replayer,
      Throttler<librbd::MockTestImageCtx> *image_sync_throttler) {
    ceph_assert(s_instance != nullptr);
    return s_instance;
  }

  MOCK_METHOD0(handle_acquire_leader, void());
  MOCK_METHOD0(handle_release_leader, void());

  MOCK_METHOD0(get_instance_id, std::string());

  MOCK_METHOD2(print_sync_status, void(Formatter*, std::stringstream*));

  MOCK_METHOD1(init, void(Context *));
  MOCK_METHOD1(shut_down, void(Context *));

  MOCK_METHOD3(notify_image_acquire, void(const std::string&,
                                          const std::string&,
                                          Context*));
  MOCK_METHOD3(notify_image_release, void(const std::string&,
                                          const std::string&,
                                          Context*));
  MOCK_METHOD4(notify_peer_image_removed, void(const std::string&,
                                               const std::string&,
                                               const std::string&,
                                               Context*));

  MOCK_METHOD1(handle_update_leader, void(const std::string&));

  InstanceWatcher() {
    s_instance = this;
  }

};

InstanceWatcher<librbd::MockTestImageCtx>* InstanceWatcher<librbd::MockTestImageCtx>::s_instance = nullptr;

template <>
struct MirrorStatusUpdater<librbd::MockTestImageCtx> {
  static MirrorStatusUpdater* s_instance;

  static MirrorStatusUpdater *create(librados::IoCtx &io_ctx,
                                     Threads<librbd::MockTestImageCtx> *threads) {
    ceph_assert(s_instance != nullptr);
    return s_instance;
  }

  MirrorStatusUpdater() {
    ceph_assert(s_instance == nullptr);
    s_instance = this;
  }

  ~MirrorStatusUpdater() {
    ceph_assert(s_instance == this);
    s_instance = nullptr;
  }

  MOCK_METHOD1(init, void(Context *));
  MOCK_METHOD1(shut_down, void(Context *));
};

MirrorStatusUpdater<librbd::MockTestImageCtx> *MirrorStatusUpdater<librbd::MockTestImageCtx>::s_instance = nullptr;

template<>
struct PoolWatcher<librbd::MockTestImageCtx> {
  static std::map<int64_t, PoolWatcher *> s_instances;

  static PoolWatcher *create(Threads<librbd::MockTestImageCtx> *threads,
                             librados::IoCtx &ioctx,
                             pool_watcher::Listener& listener) {
    auto pool_id = ioctx.get_id();
    ceph_assert(s_instances.count(pool_id));
    return s_instances[pool_id];
  }

  MOCK_METHOD0(is_blacklisted, bool());

  MOCK_METHOD0(get_image_count, uint64_t());

  MOCK_METHOD1(init, void(Context*));
  MOCK_METHOD1(shut_down, void(Context*));

  PoolWatcher(int64_t pool_id) {
    ceph_assert(!s_instances.count(pool_id));
    s_instances[pool_id] = this;
  }
};

std::map<int64_t, PoolWatcher<librbd::MockTestImageCtx> *> PoolWatcher<librbd::MockTestImageCtx>::s_instances;

template<>
struct ServiceDaemon<librbd::MockTestImageCtx> {
  MOCK_METHOD3(add_or_update_attribute,
               void(int64_t, const std::string&,
                    const service_daemon::AttributeValue&));
  MOCK_METHOD2(remove_attribute,
               void(int64_t, const std::string&));

  MOCK_METHOD4(add_or_update_callout, uint64_t(int64_t, uint64_t,
                                               service_daemon::CalloutLevel,
                                               const std::string&));
  MOCK_METHOD2(remove_callout, void(int64_t, uint64_t));
};

template <>
struct Threads<librbd::MockTestImageCtx> {
  ceph::mutex &timer_lock;
  SafeTimer *timer;
  ContextWQ *work_queue;

  Threads(Threads<librbd::ImageCtx> *threads)
    : timer_lock(threads->timer_lock), timer(threads->timer),
      work_queue(threads->work_queue) {
  }
};

} // namespace mirror
} // namespace rbd

// template definitions
#include "tools/rbd_mirror/NamespaceReplayer.cc"

namespace rbd {
namespace mirror {

using ::testing::_;
using ::testing::DoAll;
using ::testing::InSequence;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::StrEq;
using ::testing::WithArg;

class TestMockNamespaceReplayer : public TestMockFixture {
public:
  typedef NamespaceReplayer<librbd::MockTestImageCtx> MockNamespaceReplayer;
  typedef ImageDeleter<librbd::MockTestImageCtx> MockImageDeleter;
  typedef ImageMap<librbd::MockTestImageCtx> MockImageMap;
  typedef InstanceReplayer<librbd::MockTestImageCtx> MockInstanceReplayer;
  typedef InstanceWatcher<librbd::MockTestImageCtx> MockInstanceWatcher;
  typedef MirrorStatusUpdater<librbd::MockTestImageCtx> MockMirrorStatusUpdater;
  typedef PoolWatcher<librbd::MockTestImageCtx> MockPoolWatcher;
  typedef ServiceDaemon<librbd::MockTestImageCtx> MockServiceDaemon;
  typedef Threads<librbd::MockTestImageCtx> MockThreads;

  void SetUp() override {
    TestMockFixture::SetUp();
    m_mock_threads = new MockThreads(m_threads);
  }

  void TearDown() override {
    delete m_mock_threads;
    TestMockFixture::TearDown();
  }

  void expect_mirror_status_watcher_init(
      MockMirrorStatusUpdater &mock_mirror_status_watcher, int r) {
    EXPECT_CALL(mock_mirror_status_watcher, init(_))
      .WillOnce(CompleteContext(m_mock_threads->work_queue, r));
  }

  void expect_mirror_status_watcher_shut_down(
      MockMirrorStatusUpdater &mock_mirror_status_watcher) {
    EXPECT_CALL(mock_mirror_status_watcher, shut_down(_))
      .WillOnce(CompleteContext(m_mock_threads->work_queue, 0));
  }

  void expect_instance_replayer_init(
      MockInstanceReplayer& mock_instance_replayer, int r) {
    EXPECT_CALL(mock_instance_replayer, init(_))
      .WillOnce(CompleteContext(m_mock_threads->work_queue, r));
  }

  void expect_instance_replayer_shut_down(
      MockInstanceReplayer& mock_instance_replayer) {
    EXPECT_CALL(mock_instance_replayer, shut_down(_))
      .WillOnce(CompleteContext(m_mock_threads->work_queue, 0));
  }

  void expect_instance_replayer_stop(
      MockInstanceReplayer& mock_instance_replayer) {
    EXPECT_CALL(mock_instance_replayer, stop(_))
      .WillOnce(CompleteContext(m_mock_threads->work_queue, 0));
  }

  void expect_instance_replayer_add_peer(
      MockInstanceReplayer& mock_instance_replayer, const std::string& uuid) {
    EXPECT_CALL(mock_instance_replayer, add_peer(uuid, _));
  }

  void expect_instance_replayer_release_all(
      MockInstanceReplayer& mock_instance_replayer) {
    EXPECT_CALL(mock_instance_replayer, release_all(_))
      .WillOnce(CompleteContext(m_mock_threads->work_queue, 0));
  }

  void expect_instance_watcher_get_instance_id(
      MockInstanceWatcher& mock_instance_watcher,
      const std::string &instance_id) {
    EXPECT_CALL(mock_instance_watcher, get_instance_id())
      .WillOnce(Return(instance_id));
  }

  void expect_instance_watcher_init(
      MockInstanceWatcher& mock_instance_watcher, int r) {
    EXPECT_CALL(mock_instance_watcher, init(_))
      .WillOnce(CompleteContext(m_mock_threads->work_queue, r));
  }

  void expect_instance_watcher_shut_down(
      MockInstanceWatcher& mock_instance_watcher) {
    EXPECT_CALL(mock_instance_watcher, shut_down(_))
      .WillOnce(CompleteContext(m_mock_threads->work_queue, 0));
  }

  void expect_instance_watcher_handle_acquire_leader(
      MockInstanceWatcher& mock_instance_watcher) {
    EXPECT_CALL(mock_instance_watcher, handle_acquire_leader());
  }

  void expect_instance_watcher_handle_release_leader(
      MockInstanceWatcher& mock_instance_watcher) {
    EXPECT_CALL(mock_instance_watcher, handle_release_leader());
  }

  void expect_image_map_init(MockInstanceWatcher &mock_instance_watcher,
                             MockImageMap& mock_image_map, int r) {
    expect_instance_watcher_get_instance_id(mock_instance_watcher, "1234");
    EXPECT_CALL(mock_image_map, init(_))
      .WillOnce(CompleteContext(m_mock_threads->work_queue, r));
  }

  void expect_image_map_shut_down(MockImageMap& mock_image_map) {
    EXPECT_CALL(mock_image_map, shut_down(_))
      .WillOnce(CompleteContext(m_mock_threads->work_queue, 0));
  }

  void expect_pool_watcher_init(MockPoolWatcher& mock_pool_watcher, int r) {
    EXPECT_CALL(mock_pool_watcher, init(_))
      .WillOnce(CompleteContext(m_mock_threads->work_queue, r));
  }

  void expect_pool_watcher_shut_down(MockPoolWatcher& mock_pool_watcher) {
    EXPECT_CALL(mock_pool_watcher, shut_down(_))
      .WillOnce(CompleteContext(m_mock_threads->work_queue, 0));
  }

  void expect_image_deleter_init(MockImageDeleter& mock_image_deleter, int r) {
    EXPECT_CALL(mock_image_deleter, init(_))
      .WillOnce(CompleteContext(m_mock_threads->work_queue, r));
  }

  void expect_image_deleter_shut_down(MockImageDeleter& mock_image_deleter) {
    EXPECT_CALL(mock_image_deleter, shut_down(_))
      .WillOnce(CompleteContext(m_mock_threads->work_queue, 0));
  }

  void expect_service_daemon_add_or_update_attribute(
      MockServiceDaemon &mock_service_daemon, const std::string& key,
      const service_daemon::AttributeValue& value) {
    EXPECT_CALL(mock_service_daemon, add_or_update_attribute(_, key, value));
  }

  void expect_service_daemon_add_or_update_instance_id_attribute(
      MockInstanceWatcher &mock_instance_watcher,
      MockServiceDaemon &mock_service_daemon) {
    expect_instance_watcher_get_instance_id(mock_instance_watcher, "1234");
    expect_service_daemon_add_or_update_attribute(
        mock_service_daemon, "instance_id", {std::string("1234")});
  }

  MockThreads *m_mock_threads;
};

TEST_F(TestMockNamespaceReplayer, Init_MirrorStatusUpdaterError) {
  InSequence seq;

  auto mock_mirror_status_watcher = new MockMirrorStatusUpdater;
  expect_mirror_status_watcher_init(*mock_mirror_status_watcher, -EINVAL);

  MockNamespaceReplayer namespace_replayer(
      {}, m_local_io_ctx, m_remote_io_ctx, "local mirror uuid",
      "remote mirror uuid", "siteA", m_mock_threads, nullptr, nullptr, nullptr,
      nullptr);

  C_SaferCond on_init;
  namespace_replayer.init(&on_init);
  ASSERT_EQ(-EINVAL, on_init.wait());
}

TEST_F(TestMockNamespaceReplayer, Init_InstanceReplayerError) {
  InSequence seq;

  auto mock_mirror_status_watcher = new MockMirrorStatusUpdater;
  expect_mirror_status_watcher_init(*mock_mirror_status_watcher, 0);

  auto mock_instance_replayer = new MockInstanceReplayer();
  expect_instance_replayer_init(*mock_instance_replayer, -EINVAL);

  expect_mirror_status_watcher_shut_down(*mock_mirror_status_watcher);

  MockNamespaceReplayer namespace_replayer(
      {}, m_local_io_ctx, m_remote_io_ctx, "local mirror uuid",
      "remote mirror uuid", "siteA", m_mock_threads, nullptr, nullptr, nullptr,
      nullptr);

  C_SaferCond on_init;
  namespace_replayer.init(&on_init);
  ASSERT_EQ(-EINVAL, on_init.wait());
}

TEST_F(TestMockNamespaceReplayer, Init_InstanceWatcherError) {
  InSequence seq;

  auto mock_mirror_status_watcher = new MockMirrorStatusUpdater;
  expect_mirror_status_watcher_init(*mock_mirror_status_watcher, 0);

  auto mock_instance_replayer = new MockInstanceReplayer();
  expect_instance_replayer_init(*mock_instance_replayer, 0);
  expect_instance_replayer_add_peer(*mock_instance_replayer,
                                    "remote mirror uuid");

  auto mock_instance_watcher = new MockInstanceWatcher();
  expect_instance_watcher_init(*mock_instance_watcher, -EINVAL);

  expect_instance_replayer_shut_down(*mock_instance_replayer);
  expect_mirror_status_watcher_shut_down(*mock_mirror_status_watcher);

  MockNamespaceReplayer namespace_replayer(
      {}, m_local_io_ctx, m_remote_io_ctx, "local mirror uuid",
      "remote mirror uuid", "siteA", m_mock_threads, nullptr, nullptr, nullptr,
      nullptr);

  C_SaferCond on_init;
  namespace_replayer.init(&on_init);
  ASSERT_EQ(-EINVAL, on_init.wait());
}

TEST_F(TestMockNamespaceReplayer, Init) {
  InSequence seq;

  auto mock_mirror_status_watcher = new MockMirrorStatusUpdater;
  expect_mirror_status_watcher_init(*mock_mirror_status_watcher, 0);

  auto mock_instance_replayer = new MockInstanceReplayer();
  expect_instance_replayer_init(*mock_instance_replayer, 0);
  expect_instance_replayer_add_peer(*mock_instance_replayer,
                                    "remote mirror uuid");

  auto mock_instance_watcher = new MockInstanceWatcher();
  expect_instance_watcher_init(*mock_instance_watcher, 0);

  MockServiceDaemon mock_service_daemon;
  expect_service_daemon_add_or_update_instance_id_attribute(
      *mock_instance_watcher, mock_service_daemon);

  MockNamespaceReplayer namespace_replayer(
      {}, m_local_io_ctx, m_remote_io_ctx, "local mirror uuid",
      "remote mirror uuid", "siteA", m_mock_threads, nullptr, nullptr,
      &mock_service_daemon, nullptr);

  C_SaferCond on_init;
  namespace_replayer.init(&on_init);
  ASSERT_EQ(0, on_init.wait());

  expect_instance_replayer_stop(*mock_instance_replayer);
  expect_instance_watcher_shut_down(*mock_instance_watcher);
  expect_instance_replayer_shut_down(*mock_instance_replayer);
  expect_mirror_status_watcher_shut_down(*mock_mirror_status_watcher);

  C_SaferCond on_shut_down;
  namespace_replayer.shut_down(&on_shut_down);
  ASSERT_EQ(0, on_shut_down.wait());
}

TEST_F(TestMockNamespaceReplayer, AcuqireLeader) {
  InSequence seq;

  // init

  auto mock_mirror_status_watcher = new MockMirrorStatusUpdater;
  expect_mirror_status_watcher_init(*mock_mirror_status_watcher, 0);

  auto mock_instance_replayer = new MockInstanceReplayer();
  expect_instance_replayer_init(*mock_instance_replayer, 0);
  expect_instance_replayer_add_peer(*mock_instance_replayer,
                                    "remote mirror uuid");

  auto mock_instance_watcher = new MockInstanceWatcher();
  expect_instance_watcher_init(*mock_instance_watcher, 0);

  MockServiceDaemon mock_service_daemon;
  expect_service_daemon_add_or_update_instance_id_attribute(
      *mock_instance_watcher, mock_service_daemon);

  MockNamespaceReplayer namespace_replayer(
      {}, m_local_io_ctx, m_remote_io_ctx, "local mirror uuid",
      "remote mirror uuid", "siteA", m_mock_threads, nullptr, nullptr,
      &mock_service_daemon, nullptr);

  C_SaferCond on_init;
  namespace_replayer.init(&on_init);
  ASSERT_EQ(0, on_init.wait());

  // acquire leader

  expect_instance_watcher_handle_acquire_leader(*mock_instance_watcher);

  auto mock_image_map = new MockImageMap();
  expect_image_map_init(*mock_instance_watcher, *mock_image_map, 0);

  auto mock_local_pool_watcher = new MockPoolWatcher(m_local_io_ctx.get_id());
  expect_pool_watcher_init(*mock_local_pool_watcher, 0);

  auto mock_remote_pool_watcher = new MockPoolWatcher(m_remote_io_ctx.get_id());
  expect_pool_watcher_init(*mock_remote_pool_watcher, 0);

  auto mock_image_deleter = new MockImageDeleter();
  expect_image_deleter_init(*mock_image_deleter, 0);

  C_SaferCond on_acquire;
  namespace_replayer.handle_acquire_leader(&on_acquire);
  ASSERT_EQ(0, on_acquire.wait());

  // release leader

  expect_instance_watcher_handle_release_leader(*mock_instance_watcher);
  expect_image_deleter_shut_down(*mock_image_deleter);
  expect_pool_watcher_shut_down(*mock_local_pool_watcher);
  expect_pool_watcher_shut_down(*mock_remote_pool_watcher);
  expect_image_map_shut_down(*mock_image_map);
  expect_instance_replayer_release_all(*mock_instance_replayer);

  // shut down

  expect_instance_replayer_stop(*mock_instance_replayer);
  expect_instance_watcher_shut_down(*mock_instance_watcher);
  expect_instance_replayer_shut_down(*mock_instance_replayer);
  expect_mirror_status_watcher_shut_down(*mock_mirror_status_watcher);

  C_SaferCond on_shut_down;
  namespace_replayer.shut_down(&on_shut_down);
  ASSERT_EQ(0, on_shut_down.wait());
}

} // namespace mirror
} // namespace rbd
