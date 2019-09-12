#include "common/ceph_time.h"
#include "messages/MPing.h"
#include "messages/MCommand.h"
#include "messages/MCommandReply.h"
#include "messages/MOSDOp.h"
#include "messages/MOSDOpReply.h"
#include "crimson/auth/DummyAuth.h"
#include "crimson/common/log.h"
#include "crimson/net/Connection.h"
#include "crimson/net/Config.h"
#include "crimson/net/Dispatcher.h"
#include "crimson/net/Messenger.h"
#include "crimson/net/Interceptor.h"

#include <map>
#include <random>
#include <boost/program_options.hpp>
#include <fmt/format.h>
#include <fmt/ostream.h>
#include <seastar/core/app-template.hh>
#include <seastar/core/do_with.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/sleep.hh>

namespace bpo = boost::program_options;

namespace {

seastar::logger& logger() {
  return ceph::get_logger(ceph_subsys_ms);
}

static std::random_device rd;
static std::default_random_engine rng{rd()};
static bool verbose = false;

static seastar::future<> test_echo(unsigned rounds,
                                   double keepalive_ratio,
                                   bool v2)
{
  struct test_state {
    struct Server final
        : public ceph::net::Dispatcher,
          public seastar::peering_sharded_service<Server> {
      ceph::net::Messenger *msgr = nullptr;
      ceph::auth::DummyAuthClientServer dummy_auth;

      Dispatcher* get_local_shard() override {
        return &(container().local());
      }
      seastar::future<> stop() {
        return seastar::make_ready_future<>();
      }
      seastar::future<> ms_dispatch(ceph::net::Connection* c,
                                    MessageRef m) override {
        if (verbose) {
          logger().info("server got {}", *m);
        }
        // reply with a pong
        return c->send(make_message<MPing>());
      }

      seastar::future<> init(const entity_name_t& name,
                             const std::string& lname,
                             const uint64_t nonce,
                             const entity_addr_t& addr) {
        auto&& fut = ceph::net::Messenger::create(name, lname, nonce);
        return fut.then([this, addr](ceph::net::Messenger *messenger) {
            return container().invoke_on_all([messenger](auto& server) {
                server.msgr = messenger->get_local_shard();
                server.msgr->set_default_policy(ceph::net::SocketPolicy::stateless_server(0));
                server.msgr->set_require_authorizer(false);
                server.msgr->set_auth_client(&server.dummy_auth);
                server.msgr->set_auth_server(&server.dummy_auth);
              }).then([messenger, addr] {
                return messenger->bind(entity_addrvec_t{addr});
              }).then([this, messenger] {
                return messenger->start(this);
              });
          });
      }
      seastar::future<> shutdown() {
        ceph_assert(msgr);
        return msgr->shutdown();
      }
    };

    struct Client final
        : public ceph::net::Dispatcher,
          public seastar::peering_sharded_service<Client> {

      struct PingSession : public seastar::enable_shared_from_this<PingSession> {
        unsigned count = 0u;
        mono_time connected_time;
        mono_time finish_time;
      };
      using PingSessionRef = seastar::shared_ptr<PingSession>;

      unsigned rounds;
      std::bernoulli_distribution keepalive_dist;
      ceph::net::Messenger *msgr = nullptr;
      std::map<ceph::net::Connection*, seastar::promise<>> pending_conns;
      std::map<ceph::net::Connection*, PingSessionRef> sessions;
      ceph::auth::DummyAuthClientServer dummy_auth;

      Client(unsigned rounds, double keepalive_ratio)
        : rounds(rounds),
          keepalive_dist(std::bernoulli_distribution{keepalive_ratio}) {}

      PingSessionRef find_session(ceph::net::Connection* c) {
        auto found = sessions.find(c);
        if (found == sessions.end()) {
          ceph_assert(false);
        }
        return found->second;
      }

      Dispatcher* get_local_shard() override {
        return &(container().local());
      }
      seastar::future<> stop() {
        return seastar::now();
      }
      seastar::future<> ms_handle_connect(ceph::net::ConnectionRef conn) override {
        auto session = seastar::make_shared<PingSession>();
        auto [i, added] = sessions.emplace(conn.get(), session);
        std::ignore = i;
        ceph_assert(added);
        session->connected_time = mono_clock::now();
        return seastar::now();
      }
      seastar::future<> ms_dispatch(ceph::net::Connection* c,
                                    MessageRef m) override {
        auto session = find_session(c);
        ++(session->count);
        if (verbose) {
          logger().info("client ms_dispatch {}", session->count);
        }

        if (session->count == rounds) {
          logger().info("{}: finished receiving {} pongs", *c, session->count);
          session->finish_time = mono_clock::now();
          return container().invoke_on_all([c](auto &client) {
              auto found = client.pending_conns.find(c);
              ceph_assert(found != client.pending_conns.end());
              found->second.set_value();
            });
        } else {
          return seastar::now();
        }
      }

      seastar::future<> init(const entity_name_t& name,
                             const std::string& lname,
                             const uint64_t nonce) {
        return ceph::net::Messenger::create(name, lname, nonce)
          .then([this](ceph::net::Messenger *messenger) {
            return container().invoke_on_all([messenger](auto& client) {
                client.msgr = messenger->get_local_shard();
                client.msgr->set_default_policy(ceph::net::SocketPolicy::lossy_client(0));
                client.msgr->set_auth_client(&client.dummy_auth);
                client.msgr->set_auth_server(&client.dummy_auth);
              }).then([this, messenger] {
                return messenger->start(this);
              });
          });
      }

      seastar::future<> shutdown() {
        ceph_assert(msgr);
        return msgr->shutdown();
      }

      // Note: currently we don't support foreign dispatch a message because:
      // 1. it is not effecient because each ref-count modification needs
      //    a cross-core jump, so it should be discouraged.
      // 2. messenger needs to be modified to hold a wrapper for the sending
      //    message because it can be a nested seastar smart ptr or not.
      // 3. in 1:1 mapping OSD, there is no need to do foreign dispatch.
      seastar::future<> dispatch_pingpong(const entity_addr_t& peer_addr,
					  bool foreign_dispatch) {
#ifndef CRIMSON_MSGR_SEND_FOREIGN
	ceph_assert(!foreign_dispatch);
#endif
        mono_time start_time = mono_clock::now();
        return msgr->connect(peer_addr, entity_name_t::TYPE_OSD)
          .then([this, foreign_dispatch, start_time](auto conn) {
            return seastar::futurize_apply([this, conn, foreign_dispatch] {
                if (foreign_dispatch) {
                  return do_dispatch_pingpong(&**conn);
                } else {
                  // NOTE: this could be faster if we don't switch cores in do_dispatch_pingpong().
                  return container().invoke_on(conn->get()->shard_id(), [conn = &**conn](auto &client) {
                      return client.do_dispatch_pingpong(conn);
                    });
                }
              }).finally([this, conn, start_time] {
                return container().invoke_on(conn->get()->shard_id(), [conn, start_time](auto &client) {
                    auto session = client.find_session(&**conn);
                    std::chrono::duration<double> dur_handshake = session->connected_time - start_time;
                    std::chrono::duration<double> dur_pingpong = session->finish_time - session->connected_time;
                    logger().info("{}: handshake {}, pingpong {}",
                                  **conn, dur_handshake.count(), dur_pingpong.count());
                  });
              });
          });
      }

     private:
      seastar::future<> do_dispatch_pingpong(ceph::net::Connection* conn) {
        return container().invoke_on_all([conn](auto& client) {
            auto [i, added] = client.pending_conns.emplace(conn, seastar::promise<>());
            std::ignore = i;
            ceph_assert(added);
          }).then([this, conn] {
            return seastar::do_with(0u, 0u,
                                    [this, conn](auto &count_ping, auto &count_keepalive) {
                return seastar::do_until(
                  [this, conn, &count_ping, &count_keepalive] {
                    bool stop = (count_ping == rounds);
                    if (stop) {
                      logger().info("{}: finished sending {} pings with {} keepalives",
                                    *conn, count_ping, count_keepalive);
                    }
                    return stop;
                  },
                  [this, conn, &count_ping, &count_keepalive] {
                    return seastar::repeat([this, conn, &count_ping, &count_keepalive] {
                        if (keepalive_dist(rng)) {
                          return conn->keepalive()
                            .then([&count_keepalive] {
                              count_keepalive += 1;
                              return seastar::make_ready_future<seastar::stop_iteration>(
                                seastar::stop_iteration::no);
                            });
                        } else {
                          return conn->send(make_message<MPing>())
                            .then([&count_ping] {
                              count_ping += 1;
                              return seastar::make_ready_future<seastar::stop_iteration>(
                                seastar::stop_iteration::yes);
                            });
                        }
                      });
                  }).then([this, conn] {
                    auto found = pending_conns.find(conn);
                    return found->second.get_future();
                  });
              });
          });
      }
    };
  };

  logger().info("test_echo(rounds={}, keepalive_ratio={}, v2={}):",
                rounds, keepalive_ratio, v2);
  return seastar::when_all_succeed(
      ceph::net::create_sharded<test_state::Server>(),
      ceph::net::create_sharded<test_state::Server>(),
      ceph::net::create_sharded<test_state::Client>(rounds, keepalive_ratio),
      ceph::net::create_sharded<test_state::Client>(rounds, keepalive_ratio))
    .then([rounds, keepalive_ratio, v2](test_state::Server *server1,
                                        test_state::Server *server2,
                                        test_state::Client *client1,
                                        test_state::Client *client2) {
      // start servers and clients
      entity_addr_t addr1;
      addr1.parse("127.0.0.1:9010", nullptr);
      entity_addr_t addr2;
      addr2.parse("127.0.0.1:9011", nullptr);
      if (v2) {
        addr1.set_type(entity_addr_t::TYPE_MSGR2);
        addr2.set_type(entity_addr_t::TYPE_MSGR2);
      } else {
        addr1.set_type(entity_addr_t::TYPE_LEGACY);
        addr2.set_type(entity_addr_t::TYPE_LEGACY);
      }
      return seastar::when_all_succeed(
          server1->init(entity_name_t::OSD(0), "server1", 1, addr1),
          server2->init(entity_name_t::OSD(1), "server2", 2, addr2),
          client1->init(entity_name_t::OSD(2), "client1", 3),
          client2->init(entity_name_t::OSD(3), "client2", 4))
      // dispatch pingpoing
        .then([client1, client2, server1, server2] {
          return seastar::when_all_succeed(
              // test connecting in parallel, accepting in parallel
#ifdef CRIMSON_MSGR_SEND_FOREIGN
	      // operate the connection reference from a foreign core
	      client1->dispatch_pingpong(server1->msgr->get_myaddr(), true),
	      client2->dispatch_pingpong(server2->msgr->get_myaddr(), true),
#endif
	      // operate the connection reference from a local core
              client1->dispatch_pingpong(server2->msgr->get_myaddr(), false),
              client2->dispatch_pingpong(server1->msgr->get_myaddr(), false));
      // shutdown
        }).finally([client1] {
          logger().info("client1 shutdown...");
          return client1->shutdown();
        }).finally([client2] {
          logger().info("client2 shutdown...");
          return client2->shutdown();
        }).finally([server1] {
          logger().info("server1 shutdown...");
          return server1->shutdown();
        }).finally([server2] {
          logger().info("server2 shutdown...");
          return server2->shutdown();
        }).finally([] {
          logger().info("test_echo() done!\n");
        });
    });
}

static seastar::future<> test_concurrent_dispatch(bool v2)
{
  struct test_state {
    struct Server final
      : public ceph::net::Dispatcher,
        public seastar::peering_sharded_service<Server> {
      ceph::net::Messenger *msgr = nullptr;
      int count = 0;
      seastar::promise<> on_second; // satisfied on second dispatch
      seastar::promise<> on_done; // satisfied when first dispatch unblocks
      ceph::auth::DummyAuthClientServer dummy_auth;

      seastar::future<> ms_dispatch(ceph::net::Connection* c,
                                    MessageRef m) override {
        switch (++count) {
        case 1:
          // block on the first request until we reenter with the second
          return on_second.get_future()
            .then([this] {
              return container().invoke_on_all([](Server& server) {
                  server.on_done.set_value();
                });
            });
        case 2:
          on_second.set_value();
          return seastar::now();
        default:
          throw std::runtime_error("unexpected count");
        }
      }

      seastar::future<> wait() { return on_done.get_future(); }

      seastar::future<> init(const entity_name_t& name,
                             const std::string& lname,
                             const uint64_t nonce,
                             const entity_addr_t& addr) {
        return ceph::net::Messenger::create(name, lname, nonce, 0)
          .then([this, addr](ceph::net::Messenger *messenger) {
            return container().invoke_on_all([messenger](auto& server) {
                server.msgr = messenger->get_local_shard();
                server.msgr->set_default_policy(ceph::net::SocketPolicy::stateless_server(0));
                server.msgr->set_auth_client(&server.dummy_auth);
                server.msgr->set_auth_server(&server.dummy_auth);
              }).then([messenger, addr] {
                return messenger->bind(entity_addrvec_t{addr});
              }).then([this, messenger] {
                return messenger->start(this);
              });
          });
      }

      Dispatcher* get_local_shard() override {
        return &(container().local());
      }
      seastar::future<> stop() {
        return seastar::make_ready_future<>();
      }
    };

    struct Client final
      : public ceph::net::Dispatcher,
        public seastar::peering_sharded_service<Client> {
      ceph::net::Messenger *msgr = nullptr;
      ceph::auth::DummyAuthClientServer dummy_auth;

      seastar::future<> init(const entity_name_t& name,
                             const std::string& lname,
                             const uint64_t nonce) {
        return ceph::net::Messenger::create(name, lname, nonce, 0)
          .then([this](ceph::net::Messenger *messenger) {
            return container().invoke_on_all([messenger](auto& client) {
                client.msgr = messenger->get_local_shard();
                client.msgr->set_default_policy(ceph::net::SocketPolicy::lossy_client(0));
                client.msgr->set_auth_client(&client.dummy_auth);
                client.msgr->set_auth_server(&client.dummy_auth);
              }).then([this, messenger] {
                return messenger->start(this);
              });
          });
      }

      Dispatcher* get_local_shard() override {
        return &(container().local());
      }
      seastar::future<> stop() {
        return seastar::make_ready_future<>();
      }
    };
  };

  logger().info("test_concurrent_dispatch(v2={}):", v2);
  return seastar::when_all_succeed(
      ceph::net::create_sharded<test_state::Server>(),
      ceph::net::create_sharded<test_state::Client>())
    .then([v2](test_state::Server *server,
             test_state::Client *client) {
      entity_addr_t addr;
      addr.parse("127.0.0.1:9010", nullptr);
      if (v2) {
        addr.set_type(entity_addr_t::TYPE_MSGR2);
      } else {
        addr.set_type(entity_addr_t::TYPE_LEGACY);
      }
      addr.set_family(AF_INET);
      return seastar::when_all_succeed(
          server->init(entity_name_t::OSD(4), "server3", 5, addr),
          client->init(entity_name_t::OSD(5), "client3", 6))
        .then([server, client] {
          return client->msgr->connect(server->msgr->get_myaddr(),
                                      entity_name_t::TYPE_OSD);
        }).then([](ceph::net::ConnectionXRef conn) {
          // send two messages
          (*conn)->send(make_message<MPing>());
          (*conn)->send(make_message<MPing>());
        }).then([server] {
          return server->wait();
        }).finally([client] {
          logger().info("client shutdown...");
          return client->msgr->shutdown();
        }).finally([server] {
          logger().info("server shutdown...");
          return server->msgr->shutdown();
        }).finally([] {
          logger().info("test_concurrent_dispatch() done!\n");
        });
    });
}

seastar::future<> test_preemptive_shutdown(bool v2) {
  struct test_state {
    class Server final
      : public ceph::net::Dispatcher,
        public seastar::peering_sharded_service<Server> {
      ceph::net::Messenger *msgr = nullptr;
      ceph::auth::DummyAuthClientServer dummy_auth;

      seastar::future<> ms_dispatch(ceph::net::Connection* c,
                                    MessageRef m) override {
        return c->send(make_message<MPing>());
      }

     public:
      seastar::future<> init(const entity_name_t& name,
                             const std::string& lname,
                             const uint64_t nonce,
                             const entity_addr_t& addr) {
        return ceph::net::Messenger::create(name, lname, nonce, seastar::engine().cpu_id()
        ).then([this, addr](ceph::net::Messenger *messenger) {
          return container().invoke_on_all([messenger](auto& server) {
            server.msgr = messenger->get_local_shard();
            server.msgr->set_default_policy(ceph::net::SocketPolicy::stateless_server(0));
            server.msgr->set_auth_client(&server.dummy_auth);
            server.msgr->set_auth_server(&server.dummy_auth);
          }).then([messenger, addr] {
            return messenger->bind(entity_addrvec_t{addr});
          }).then([this, messenger] {
            return messenger->start(this);
          });
        });
      }
      entity_addr_t get_addr() const {
        return msgr->get_myaddr();
      }
      seastar::future<> shutdown() {
        return msgr->shutdown();
      }
      Dispatcher* get_local_shard() override {
        return &(container().local());
      }
      seastar::future<> stop() {
        return seastar::now();
      }
    };

    class Client final
      : public ceph::net::Dispatcher,
        public seastar::peering_sharded_service<Client> {
      ceph::net::Messenger *msgr = nullptr;
      ceph::auth::DummyAuthClientServer dummy_auth;

      bool stop_send = false;
      seastar::promise<> stopped_send_promise;

      seastar::future<> ms_dispatch(ceph::net::Connection* c,
                                    MessageRef m) override {
        return seastar::now();
      }

     public:
      seastar::future<> init(const entity_name_t& name,
                             const std::string& lname,
                             const uint64_t nonce) {
        return ceph::net::Messenger::create(name, lname, nonce, seastar::engine().cpu_id()
        ).then([this](ceph::net::Messenger *messenger) {
          return container().invoke_on_all([messenger](auto& client) {
            client.msgr = messenger->get_local_shard();
            client.msgr->set_default_policy(ceph::net::SocketPolicy::lossy_client(0));
            client.msgr->set_auth_client(&client.dummy_auth);
            client.msgr->set_auth_server(&client.dummy_auth);
          }).then([this, messenger] {
            return messenger->start(this);
          });
        });
      }
      seastar::future<> send_pings(const entity_addr_t& addr) {
        return msgr->connect(addr, entity_name_t::TYPE_OSD
        ).then([this](ceph::net::ConnectionXRef conn) {
          seastar::do_until(
            [this] { return stop_send; },
            [this, conn = &**conn] {
              return conn->send(make_message<MPing>()).then([] {
                return seastar::sleep(0ms);
              });
            }
          ).then_wrapped([this, conn] (auto fut) {
            fut.forward_to(std::move(stopped_send_promise));
          });
        });
      }
      seastar::future<> shutdown() {
        return msgr->shutdown().then([this] {
          stop_send = true;
          return stopped_send_promise.get_future();
        });
      }
      Dispatcher* get_local_shard() override {
        return &(container().local());
      }
      seastar::future<> stop() {
        return seastar::now();
      }
    };
  };

  logger().info("test_preemptive_shutdown(v2={}):", v2);
  return seastar::when_all_succeed(
    ceph::net::create_sharded<test_state::Server>(),
    ceph::net::create_sharded<test_state::Client>()
  ).then([v2](test_state::Server *server,
             test_state::Client *client) {
    entity_addr_t addr;
    addr.parse("127.0.0.1:9010", nullptr);
    if (v2) {
      addr.set_type(entity_addr_t::TYPE_MSGR2);
    } else {
      addr.set_type(entity_addr_t::TYPE_LEGACY);
    }
    addr.set_family(AF_INET);
    return seastar::when_all_succeed(
      server->init(entity_name_t::OSD(6), "server4", 7, addr),
      client->init(entity_name_t::OSD(7), "client4", 8)
    ).then([server, client] {
      return client->send_pings(server->get_addr());
    }).then([] {
      return seastar::sleep(100ms);
    }).then([client] {
      logger().info("client shutdown...");
      return client->shutdown();
    }).finally([server] {
      logger().info("server shutdown...");
      return server->shutdown();
    }).finally([] {
      logger().info("test_preemptive_shutdown() done!\n");
    });
  });
}

using ceph::msgr::v2::Tag;
using ceph::net::Breakpoint;
using ceph::net::Connection;
using ceph::net::ConnectionRef;
using ceph::net::custom_bp_t;
using ceph::net::Dispatcher;
using ceph::net::Interceptor;
using ceph::net::Messenger;
using ceph::net::SocketPolicy;
using ceph::net::tag_bp_t;

struct counter_t { unsigned counter = 0; };

enum class conn_state_t {
  unknown = 0,
  established,
  closed,
  replaced,
};

std::ostream& operator<<(std::ostream& out, const conn_state_t& state) {
  switch(state) {
   case conn_state_t::unknown:
    return out << "unknown";
   case conn_state_t::established:
    return out << "established";
   case conn_state_t::closed:
    return out << "closed";
   case conn_state_t::replaced:
    return out << "replaced";
   default:
    ceph_abort();
  }
}

template <typename T>
void _assert_eq(ConnectionRef conn,
                unsigned index,
                const char* expr_actual, T actual,
                const char* expr_expected, T expected) {
  if (actual != expected) {
    throw std::runtime_error(fmt::format(
          "[{}] {} '{}' is actually {}, not the expected '{}' {}",
          index, *conn, expr_actual, actual, expr_expected, expected));
  }
}
#define ASSERT_EQUAL(conn, index, actual, expected) \
  _assert_eq(conn, index, #actual, actual, #expected, expected)

struct ConnResult {
  ConnectionRef conn;
  unsigned index;
  conn_state_t state = conn_state_t::unknown;

  unsigned connect_attempts = 0;
  unsigned client_connect_attempts = 0;
  unsigned client_reconnect_attempts = 0;
  unsigned cnt_connect_dispatched = 0;

  unsigned accept_attempts = 0;
  unsigned server_connect_attempts = 0;
  unsigned server_reconnect_attempts = 0;
  unsigned cnt_accept_dispatched = 0;

  unsigned cnt_reset_dispatched = 0;
  unsigned cnt_remote_reset_dispatched = 0;

  ConnResult(Connection& conn, unsigned index)
    : conn(conn.shared_from_this()), index(index) {}

  void assert_state_at(conn_state_t expected) const {
    ASSERT_EQUAL(conn, index, state, expected);
  }

  void assert_connect(unsigned attempts,
                      unsigned connects,
                      unsigned reconnects,
                      unsigned dispatched) const {
    ASSERT_EQUAL(conn, index, connect_attempts, attempts);
    ASSERT_EQUAL(conn, index, client_connect_attempts, connects);
    ASSERT_EQUAL(conn, index, client_reconnect_attempts, reconnects);
    ASSERT_EQUAL(conn, index, cnt_connect_dispatched, dispatched);
  }

  void assert_accept(unsigned attempts,
                     unsigned accepts,
                     unsigned reaccepts,
                     unsigned dispatched) const {
    ASSERT_EQUAL(conn, index, accept_attempts, attempts);
    ASSERT_EQUAL(conn, index, server_connect_attempts, accepts);
    ASSERT_EQUAL(conn, index, server_reconnect_attempts, reaccepts);
    ASSERT_EQUAL(conn, index, cnt_accept_dispatched, dispatched);
  }

  void assert_accept(unsigned attempts,
                     unsigned total_accepts,
                     unsigned dispatched) const {
    ASSERT_EQUAL(conn, index, accept_attempts, attempts);
    ASSERT_EQUAL(conn, index, server_connect_attempts + server_reconnect_attempts, total_accepts);
    ASSERT_EQUAL(conn, index, cnt_accept_dispatched, dispatched);
  }

  void assert_reset(unsigned local, unsigned remote) const {
    ASSERT_EQUAL(conn, index, cnt_reset_dispatched, local);
    ASSERT_EQUAL(conn, index, cnt_remote_reset_dispatched, remote);
  }

  void dump() const {
    logger().info("\nResult({}):\n"
                  "  conn: [{}] {}:\n"
                  "  state: {}\n"
                  "  connect_attempts: {}\n"
                  "  client_connect_attempts: {}\n"
                  "  client_reconnect_attempts: {}\n"
                  "  cnt_connect_dispatched: {}\n"
                  "  accept_attempts: {}\n"
                  "  server_connect_attempts: {}\n"
                  "  server_reconnect_attempts: {}\n"
                  "  cnt_accept_dispatched: {}\n"
                  "  cnt_reset_dispatched: {}\n"
                  "  cnt_remote_reset_dispatched: {}\n",
                  this,
                  index, *conn,
                  state,
                  connect_attempts,
                  client_connect_attempts,
                  client_reconnect_attempts,
                  cnt_connect_dispatched,
                  accept_attempts,
                  server_connect_attempts,
                  server_reconnect_attempts,
                  cnt_accept_dispatched,
                  cnt_reset_dispatched,
                  cnt_remote_reset_dispatched);
  }
};
using ConnResults = std::vector<ConnResult>;

struct TestInterceptor : public Interceptor {
  std::map<Breakpoint, std::set<unsigned>> breakpoints;
  std::map<Breakpoint, counter_t> breakpoints_counter;
  std::map<ConnectionRef, unsigned> conns;
  ConnResults results;
  std::optional<seastar::promise<>> signal;

  TestInterceptor() = default;
  // only used for copy breakpoint configurations
  TestInterceptor(const TestInterceptor& other) {
    assert(other.breakpoints_counter.empty());
    assert(other.conns.empty());
    assert(other.results.empty());
    breakpoints = other.breakpoints;
    assert(!other.signal);
  }

  void make_fault(Breakpoint bp, unsigned round = 1) {
    assert(round >= 1);
    breakpoints[bp].insert(round);
  }

  ConnResult* find_result(ConnectionRef conn) {
    auto it = conns.find(conn);
    if (it == conns.end()) {
      return nullptr;
    } else {
      return &results[it->second];
    }
  }

  seastar::future<> wait() {
    assert(!signal);
    signal = seastar::promise<>();
    return signal->get_future();
  }

  void notify() {
    if (signal) {
      signal->set_value();
      signal = std::nullopt;
    }
  }

 private:
  void register_conn(Connection& conn) override {
    unsigned index = results.size();
    results.emplace_back(conn, index);
    conns[conn.shared_from_this()] = index;
    notify();
    logger().info("[{}] {} new connection registered", index, conn);
  }

  void register_conn_closed(Connection& conn) override {
    auto result = find_result(conn.shared_from_this());
    if (result == nullptr) {
      logger().error("Untracked closed connection: {}", conn);
      ceph_abort();
    }

    if (result->state != conn_state_t::replaced) {
      result->state = conn_state_t::closed;
    }
    notify();
    logger().info("[{}] {} closed({})", result->index, conn, result->state);
  }

  void register_conn_ready(Connection& conn) override {
    auto result = find_result(conn.shared_from_this());
    if (result == nullptr) {
      logger().error("Untracked ready connection: {}", conn);
      ceph_abort();
    }

    ceph_assert(conn.is_connected());
    notify();
    logger().info("[{}] {} ready", result->index, conn);
  }

  void register_conn_replaced(Connection& conn) override {
    auto result = find_result(conn.shared_from_this());
    if (result == nullptr) {
      logger().error("Untracked replaced connection: {}", conn);
      ceph_abort();
    }

    result->state = conn_state_t::replaced;
    logger().info("[{}] {} {}", result->index, conn, result->state);
  }

  bool intercept(Connection& conn, Breakpoint bp) override {
    ++breakpoints_counter[bp].counter;

    auto result = find_result(conn.shared_from_this());
    if (result == nullptr) {
      logger().error("Untracked intercepted connection: {}, at breakpoint {}",
                     conn, bp);
      ceph_abort();
    }
    logger().info("[{}] {} intercepted {}", result->index, conn, bp);

    if (bp == custom_bp_t::SOCKET_CONNECTING) {
      ++result->connect_attempts;
    } else if (bp == tag_bp_t{Tag::CLIENT_IDENT, true}) {
      ++result->client_connect_attempts;
    } else if (bp == tag_bp_t{Tag::SESSION_RECONNECT, true}) {
      ++result->client_reconnect_attempts;
    } else if (bp == custom_bp_t::SOCKET_ACCEPTED) {
      ++result->accept_attempts;
    } else if (bp == tag_bp_t{Tag::CLIENT_IDENT, false}) {
      ++result->server_connect_attempts;
    } else if (bp == tag_bp_t{Tag::SESSION_RECONNECT, false}) {
      ++result->server_reconnect_attempts;
    }

    auto it_bp = breakpoints.find(bp);
    if (it_bp != breakpoints.end()) {
      auto it_cnt = it_bp->second.find(breakpoints_counter[bp].counter);
      if (it_cnt != it_bp->second.end()) {
        return true;
      }
    }
    return false;
  }
};

enum class cmd_t : char {
  none = '\0',
  shutdown,
  suite_start,
  suite_stop,
  suite_connect_me,
  suite_send_me,
  suite_recv_op
};

enum class policy_t : char {
  none = '\0',
  stateful_server,
  stateless_server,
  lossless_peer,
  lossless_peer_reuse,
  lossy_client,
  lossless_client
};

SocketPolicy to_socket_policy(policy_t policy) {
  switch (policy) {
   case policy_t::stateful_server:
    return SocketPolicy::stateful_server(0);
   case policy_t::stateless_server:
    return SocketPolicy::stateless_server(0);
   case policy_t::lossless_peer:
    return SocketPolicy::lossless_peer(0);
   case policy_t::lossless_peer_reuse:
    return SocketPolicy::lossless_peer_reuse(0);
   case policy_t::lossy_client:
    return SocketPolicy::lossy_client(0);
   case policy_t::lossless_client:
    return SocketPolicy::lossless_client(0);
   default:
    logger().error("unexpected policy type");
    ceph_abort();
  }
}

class FailoverSuite : public Dispatcher {
  ceph::auth::DummyAuthClientServer dummy_auth;
  Messenger& test_msgr;
  const entity_addr_t test_peer_addr;
  TestInterceptor interceptor;

  unsigned tracked_index = 0;
  ConnectionRef tracked_conn;
  unsigned pending_send = 0;
  unsigned pending_peer_receive = 0;
  unsigned pending_receive = 0;

  seastar::future<> ms_dispatch(Connection* c, MessageRef m) override {
    auto result = interceptor.find_result(c->shared_from_this());
    if (result == nullptr) {
      logger().error("Untracked ms dispatched connection: {}", *c);
      ceph_abort();
    }

    if (tracked_conn != c->shared_from_this()) {
      logger().error("[{}] {} got op, but doesn't match tracked_conn [{}] {}",
                     result->index, *c, tracked_index, *tracked_conn);
      ceph_abort();
    }
    ceph_assert(result->index == tracked_index);

    ceph_assert(m->get_type() == CEPH_MSG_OSD_OP);
    ceph_assert(pending_receive > 0);
    --pending_receive;
    if (pending_receive == 0) {
      interceptor.notify();
    }
    logger().info("[{}] {} got op, pending {} ops", result->index, *c, pending_receive);
    return seastar::now();
  }

  seastar::future<> ms_handle_accept(ConnectionRef conn) override {
    auto result = interceptor.find_result(conn);
    if (result == nullptr) {
      logger().error("Untracked accepted connection: {}", *conn);
      ceph_abort();
    }

    if (tracked_conn) {
      logger().error("[{}] {} got accepted, but there's already traced_conn [{}] {}",
                     result->index, *conn, tracked_index, *tracked_conn);
      ceph_abort();
    }

    tracked_index = result->index;
    tracked_conn = conn;
    ++result->cnt_accept_dispatched;
    logger().info("[{}] {} got accepted and tracked, start to send {} ops",
                  result->index, *conn, pending_send);
    return flush_pending_send();
  }

  seastar::future<> ms_handle_connect(ConnectionRef conn) override {
    auto result = interceptor.find_result(conn);
    if (result == nullptr) {
      logger().error("Untracked connected connection: {}", *conn);
      ceph_abort();
    }

    if (tracked_conn != conn) {
      logger().error("[{}] {} got connected, but doesn't match tracked_conn [{}] {}",
                     result->index, *conn, tracked_index, *tracked_conn);
      ceph_abort();
    }
    ceph_assert(result->index == tracked_index);

    ++result->cnt_connect_dispatched;
    logger().info("[{}] {} got connected", result->index, *conn);
    return seastar::now();
  }

  seastar::future<> ms_handle_reset(ConnectionRef conn) override {
    auto result = interceptor.find_result(conn);
    if (result == nullptr) {
      logger().error("Untracked reset connection: {}", *conn);
      ceph_abort();
    }

    if (tracked_conn != conn) {
      logger().error("[{}] {} got reset, but doesn't match tracked_conn [{}] {}",
                     result->index, *conn, tracked_index, *tracked_conn);
      ceph_abort();
    }
    ceph_assert(result->index == tracked_index);

    tracked_index = 0;
    tracked_conn = nullptr;
    ++result->cnt_reset_dispatched;
    logger().info("[{}] {} got reset and untracked", result->index, *conn);
    return seastar::now();
  }

  seastar::future<> ms_handle_remote_reset(ConnectionRef conn) override {
    auto result = interceptor.find_result(conn);
    if (result == nullptr) {
      logger().error("Untracked remotely reset connection: {}", *conn);
      ceph_abort();
    }

    if (tracked_conn != conn) {
      logger().error("[{}] {} got remotely reset, but doesn't match tracked_conn [{}] {}",
                     result->index, *conn, tracked_index, *tracked_conn);
      ceph_abort();
    }
    ceph_assert(result->index == tracked_index);

    logger().info("[{}] {} got remotely reset", result->index, *conn);
    ++result->cnt_remote_reset_dispatched;
    return seastar::now();
  }

 private:
  seastar::future<> init(entity_addr_t addr, SocketPolicy policy) {
    test_msgr.set_default_policy(policy);
    test_msgr.set_auth_client(&dummy_auth);
    test_msgr.set_auth_server(&dummy_auth);
    test_msgr.interceptor = &interceptor;
    return test_msgr.bind(entity_addrvec_t{addr}).then([this] {
      return test_msgr.start(this);
    });
  }

  seastar::future<> send_op() {
    ceph_assert(tracked_conn);
    ++pending_peer_receive;
    pg_t pgid;
    object_locator_t oloc;
    hobject_t hobj(object_t(), oloc.key, CEPH_NOSNAP, pgid.ps(),
                   pgid.pool(), oloc.nspace);
    spg_t spgid(pgid);
    return tracked_conn->send(make_message<MOSDOp>(0, 0, hobj, spgid, 0, 0, 0));
  }

  seastar::future<> flush_pending_send() {
    ceph_assert(tracked_conn);
    return seastar::do_until(
        [this] { return pending_send == 0; },
        [this] {
      --pending_send;
      return send_op();
    });
  }

  seastar::future<> wait_ready(unsigned num_conns) {
    assert(num_conns > 0);
    if (interceptor.results.size() > num_conns) {
      throw std::runtime_error(fmt::format(
            "{} connections, more than expected: {}",
            interceptor.results.size(), num_conns));
    }

    for (auto& result : interceptor.results) {
      if (result.conn->is_closed()) {
        continue;
      }

      if (result.conn->is_connected()) {
        if (tracked_conn != result.conn || tracked_index != result.index) {
          throw std::runtime_error(fmt::format(
                "The connected connection [{}] {} doesn't"
                " match the tracked connection [{}] {}",
                result.index, *result.conn, tracked_index, tracked_conn));
        }

        if (pending_send || pending_peer_receive || pending_receive) {
          logger().info("Waiting for pending_send={} pending_peer_receive={}"
                        " pending_receive={} from [{}] {}",
                        pending_send, pending_peer_receive, pending_receive,
                        result.index, *result.conn);
          return interceptor.wait().then([this, num_conns] {
            return wait_ready(num_conns);
          });
        } else {
          result.state = conn_state_t::established;
        }
      } else {
        logger().info("Waiting for connection [{}] {} connected/closed",
                      result.index, *result.conn);
        return interceptor.wait().then([this, num_conns] {
          return wait_ready(num_conns);
        });
      }
    }

    if (interceptor.results.size() < num_conns) {
      logger().info("Waiting for incoming connection, currently {}, expected {}",
                    interceptor.results.size(), num_conns);
      return interceptor.wait().then([this, num_conns] {
        return wait_ready(num_conns);
      });
    }

    logger().debug("Wait done!");
    return seastar::now();
  }

 // called by FailoverTest
 public:
  FailoverSuite(Messenger& test_msgr,
                entity_addr_t test_peer_addr,
                const TestInterceptor& interceptor)
    : test_msgr(test_msgr),
      test_peer_addr(test_peer_addr),
      interceptor(interceptor) { }

  seastar::future<> shutdown() {
    return test_msgr.shutdown();
  }

  void needs_receive() {
    ++pending_receive;
  }

  void notify_peer_reply() {
    ceph_assert(pending_peer_receive > 0);
    --pending_peer_receive;
    logger().info("TestPeer received op, pending {} peer receive ops",
                  pending_peer_receive);
    if (pending_peer_receive == 0) {
      interceptor.notify();
    }
  }

  void post_check() const {
    // make sure all breakpoints were hit
    for (auto& kv : interceptor.breakpoints) {
      auto it = interceptor.breakpoints_counter.find(kv.first);
      if (it == interceptor.breakpoints_counter.end()) {
        throw std::runtime_error(fmt::format("{} was missed", kv.first));
      }
      auto expected = *std::max_element(kv.second.begin(), kv.second.end());
      if (expected > it->second.counter) {
        throw std::runtime_error(fmt::format(
              "{} only triggered {} times, not the expected {}",
              kv.first, it->second.counter, expected));
      }
    }
  }

  static seastar::future<std::unique_ptr<FailoverSuite>>
  create(entity_addr_t test_addr,
         SocketPolicy test_policy,
         entity_addr_t test_peer_addr,
         const TestInterceptor& interceptor) {
    return Messenger::create(entity_name_t::OSD(2), "Test", 2, 0
    ).then([test_addr,
            test_policy,
            test_peer_addr,
            interceptor] (Messenger* test_msgr) {
      auto suite = std::make_unique<FailoverSuite>(
          *test_msgr, test_peer_addr, interceptor);
      return suite->init(test_addr, test_policy
      ).then([suite = std::move(suite)] () mutable {
        return std::move(suite);
      });
    });
  }

 // called by tests
 public:
  seastar::future<> connect_peer() {
    ceph_assert(!tracked_conn);
    return test_msgr.connect(test_peer_addr, entity_name_t::TYPE_OSD
    ).then([this] (auto xconn) {
      ceph_assert(!tracked_conn);

      auto conn = xconn->release();
      auto result = interceptor.find_result(conn);
      ceph_assert(result != nullptr);

      tracked_index = result->index;
      tracked_conn = conn;
      return flush_pending_send();
    });
  }

  seastar::future<> send_peer() {
    if (tracked_conn) {
      ceph_assert(!pending_send);
      return send_op();
    } else {
      ++pending_send;
      return seastar::now();
    }
  }

  seastar::future<std::reference_wrapper<ConnResults>>
  wait_results(unsigned num_conns) {
    return wait_ready(num_conns).then([this] {
      return std::reference_wrapper<ConnResults>(interceptor.results);
    });
  }
};

class FailoverTest : public Dispatcher {
  ceph::auth::DummyAuthClientServer dummy_auth;
  Messenger& cmd_msgr;
  ConnectionRef cmd_conn;
  const entity_addr_t test_addr;
  const entity_addr_t test_peer_addr;

  std::optional<seastar::promise<>> recv_pong;
  std::optional<seastar::promise<>> recv_cmdreply;

  std::unique_ptr<FailoverSuite> test_suite;

  seastar::future<> ms_dispatch(Connection* c, MessageRef m) override {
    switch (m->get_type()) {
     case CEPH_MSG_PING:
      ceph_assert(recv_pong);
      recv_pong->set_value();
      recv_pong = std::nullopt;
      return seastar::now();
     case MSG_COMMAND_REPLY:
      ceph_assert(recv_cmdreply);
      recv_cmdreply->set_value();
      recv_cmdreply = std::nullopt;
      return seastar::now();
     case MSG_COMMAND: {
      auto m_cmd = boost::static_pointer_cast<MCommand>(m);
      ceph_assert(static_cast<cmd_t>(m_cmd->cmd[0][0]) == cmd_t::suite_recv_op);
      ceph_assert(test_suite);
      test_suite->notify_peer_reply();
      return seastar::now();
     }
     default:
      logger().error("{} got unexpected msg from cmd server: {}", *c, *m);
      ceph_abort();
    }
  }

 private:
  seastar::future<> prepare_cmd(
      cmd_t cmd,
      std::function<void(ceph::ref_t<MCommand>)>
        f_prepare = [] (auto m) { return; }) {
    assert(!recv_cmdreply);
    recv_cmdreply  = seastar::promise<>();
    auto fut = recv_cmdreply->get_future();
    auto m = make_message<MCommand>();
    m->cmd.emplace_back(1, static_cast<char>(cmd));
    f_prepare(m);
    return cmd_conn->send(m).then([fut = std::move(fut)] () mutable {
      return std::move(fut);
    });
  }

  seastar::future<> start_peer(policy_t peer_policy) {
    return prepare_cmd(cmd_t::suite_start,
        [peer_policy] (auto m) {
      m->cmd.emplace_back(1, static_cast<char>(peer_policy));
    });
  }

  seastar::future<> stop_peer() {
    return prepare_cmd(cmd_t::suite_stop);
  }

  seastar::future<> pingpong() {
    assert(!recv_pong);
    recv_pong = seastar::promise<>();
    auto fut = recv_pong->get_future();
    return cmd_conn->send(make_message<MPing>()
    ).then([this, fut = std::move(fut)] () mutable {
      return std::move(fut);
    });
  }

  seastar::future<> init(entity_addr_t cmd_peer_addr) {
    cmd_msgr.set_default_policy(SocketPolicy::lossy_client(0));
    cmd_msgr.set_auth_client(&dummy_auth);
    cmd_msgr.set_auth_server(&dummy_auth);
    return cmd_msgr.start(this).then([this, cmd_peer_addr] {
      return cmd_msgr.connect(cmd_peer_addr, entity_name_t::TYPE_OSD);
    }).then([this] (auto conn) {
      cmd_conn = conn->release();
      return pingpong();
    });
  }

 public:
  FailoverTest(Messenger& cmd_msgr,
               entity_addr_t test_addr,
               entity_addr_t test_peer_addr)
    : cmd_msgr(cmd_msgr),
      test_addr(test_addr),
      test_peer_addr(test_peer_addr) { }

  seastar::future<> shutdown() {
    logger().info("CmdCli shutdown...");
    assert(!recv_cmdreply);
    auto m = make_message<MCommand>();
    m->cmd.emplace_back(1, static_cast<char>(cmd_t::shutdown));
    return cmd_conn->send(m).then([this] {
      return seastar::sleep(200ms);
    }).finally([this] {
      return cmd_msgr.shutdown();
    });
  }

  static seastar::future<seastar::lw_shared_ptr<FailoverTest>>
  create(entity_addr_t cmd_peer_addr, entity_addr_t test_addr) {
    assert(cmd_peer_addr.is_msgr2());
    return Messenger::create(entity_name_t::OSD(1), "CmdCli", 1, 0
    ).then([cmd_peer_addr, test_addr] (Messenger* cmd_msgr) {
      entity_addr_t test_peer_addr = cmd_peer_addr;
      test_peer_addr.set_port(cmd_peer_addr.get_port() + 1);
      test_peer_addr.set_nonce(4);
      auto test = seastar::make_lw_shared<FailoverTest>(
          *cmd_msgr, test_addr, test_peer_addr);
      return test->init(cmd_peer_addr).then([test] {
        logger().info("CmdCli ready");
        return test;
      });
    });
  }

 // called by tests
 public:
  seastar::future<> run_suite(
      std::string name,
      const TestInterceptor& interceptor,
      policy_t test_policy,
      policy_t peer_policy,
      std::function<seastar::future<>(FailoverSuite&)>&& f) {
    logger().info("\n\n[{}]", name);
    ceph_assert(!test_suite);
    SocketPolicy test_policy_ = to_socket_policy(test_policy);
    return FailoverSuite::create(
        test_addr, test_policy_, test_peer_addr, interceptor
    ).then([this, peer_policy, f = std::move(f)] (auto suite) mutable {
      test_suite.swap(suite);
      return start_peer(peer_policy).then([this, f = std::move(f)] {
        return f(*test_suite);
      }).then([this] {
        test_suite->post_check();
        logger().info("\n[SUCCESS]");
      }).handle_exception([] (auto eptr) {
        logger().info("\n[FAIL: {}]", eptr);
        throw;
      }).finally([this] {
        return stop_peer();
      }).finally([this] {
        return test_suite->shutdown().then([this] {
          test_suite.reset();
        });
      });
    });
  }

  seastar::future<> peer_connect_me() {
    return prepare_cmd(cmd_t::suite_connect_me,
        [this] (auto m) {
      m->cmd.emplace_back(fmt::format("{}", test_addr));
    });
  }

  seastar::future<> peer_send_me() {
    ceph_assert(test_suite);
    test_suite->needs_receive();
    return prepare_cmd(cmd_t::suite_send_me);
  }

  seastar::future<> send_bidirectional() {
    ceph_assert(test_suite);
    return test_suite->send_peer().then([this] {
      return peer_send_me();
    });
  }
};

class FailoverSuitePeer : public Dispatcher {
  using cb_t = std::function<seastar::future<>()>;
  ceph::auth::DummyAuthClientServer dummy_auth;
  Messenger& peer_msgr;
  cb_t op_callback;

  ConnectionRef tracked_conn;
  unsigned pending_send = 0;

  seastar::future<> ms_dispatch(Connection* c, MessageRef m) override {
    ceph_assert(m->get_type() == CEPH_MSG_OSD_OP);
    ceph_assert(tracked_conn == c->shared_from_this());
    return op_callback();
  }

  seastar::future<> ms_handle_accept(ConnectionRef conn) override {
    ceph_assert(!tracked_conn);
    tracked_conn = conn;
    return flush_pending_send();
  }

  seastar::future<> ms_handle_reset(ConnectionRef conn) override {
    ceph_assert(tracked_conn == conn);
    tracked_conn = nullptr;
    return seastar::now();
  }

 private:
  seastar::future<> init(entity_addr_t addr, SocketPolicy policy) {
    peer_msgr.set_default_policy(policy);
    peer_msgr.set_auth_client(&dummy_auth);
    peer_msgr.set_auth_server(&dummy_auth);
    return peer_msgr.bind(entity_addrvec_t{addr}).then([this] {
      return peer_msgr.start(this);
    });
  }

  seastar::future<> send_op() {
    ceph_assert(tracked_conn);
    pg_t pgid;
    object_locator_t oloc;
    hobject_t hobj(object_t(), oloc.key, CEPH_NOSNAP, pgid.ps(),
                   pgid.pool(), oloc.nspace);
    spg_t spgid(pgid);
    return tracked_conn->send(make_message<MOSDOp>(0, 0, hobj, spgid, 0, 0, 0));
  }

  seastar::future<> flush_pending_send() {
    ceph_assert(tracked_conn);
    return seastar::do_until(
        [this] { return pending_send == 0; },
        [this] {
      --pending_send;
      return send_op();
    });
  }

 public:
  FailoverSuitePeer(Messenger& peer_msgr, cb_t op_callback)
    : peer_msgr(peer_msgr), op_callback(op_callback) { }

  seastar::future<> shutdown() {
    return peer_msgr.shutdown();
  }

  seastar::future<> connect(entity_addr_t addr) {
    ceph_assert(!tracked_conn);
    return peer_msgr.connect(addr, entity_name_t::TYPE_OSD
    ).then([this] (auto xconn) {
      ceph_assert(!tracked_conn);
      tracked_conn = xconn->release();
      return flush_pending_send();
    });
  }

  seastar::future<> send_peer() {
    if (tracked_conn) {
      return send_op();
    } else {
      ++pending_send;
      return seastar::now();
    }
  }

  static seastar::future<std::unique_ptr<FailoverSuitePeer>>
  create(entity_addr_t addr, const SocketPolicy& policy, cb_t op_callback) {
    return Messenger::create(entity_name_t::OSD(4), "TestPeer", 4, 0
    ).then([addr, policy, op_callback] (Messenger* peer_msgr) {
      auto suite = std::make_unique<FailoverSuitePeer>(*peer_msgr, op_callback);
      return suite->init(addr, policy
      ).then([suite = std::move(suite)] () mutable {
        return std::move(suite);
      });
    });
  }
};

class FailoverTestPeer : public Dispatcher {
  ceph::auth::DummyAuthClientServer dummy_auth;
  Messenger& cmd_msgr;
  ConnectionRef cmd_conn;
  std::unique_ptr<FailoverSuitePeer> test_suite;

  seastar::future<> ms_dispatch(Connection* c, MessageRef m) override {
    ceph_assert(cmd_conn == c->shared_from_this());
    switch (m->get_type()) {
     case CEPH_MSG_PING:
      return c->send(make_message<MPing>());
     case MSG_COMMAND: {
      auto m_cmd = boost::static_pointer_cast<MCommand>(m);
      auto cmd = static_cast<cmd_t>(m_cmd->cmd[0][0]);
      if (cmd == cmd_t::shutdown) {
        logger().info("CmdSrv shutdown...");
        cmd_msgr.shutdown();
        return seastar::now();
      }
      return handle_cmd(cmd, m_cmd).then([c] {
        return c->send(make_message<MCommandReply>());
      });
     }
     default:
      logger().error("{} got unexpected msg from cmd client: {}", *c, m);
      ceph_abort();
    }
  }

  seastar::future<> ms_handle_accept(ConnectionRef conn) override {
    cmd_conn = conn;
    return seastar::now();
  }

 private:
  seastar::future<> notify_recv_op() {
    ceph_assert(cmd_conn);
    auto m = make_message<MCommand>();
    m->cmd.emplace_back(1, static_cast<char>(cmd_t::suite_recv_op));
    return cmd_conn->send(m);
  }

  seastar::future<> handle_cmd(cmd_t cmd, MRef<MCommand> m_cmd) {
    switch (cmd) {
     case cmd_t::suite_start: {
      ceph_assert(!test_suite);
      // suite bind to cmd_addr, with port + 1
      auto test_peer_addr = get_addr();
      test_peer_addr.set_port(get_addr().get_port() + 1);
      auto policy = to_socket_policy(static_cast<policy_t>(m_cmd->cmd[1][0]));
      return FailoverSuitePeer::create(test_peer_addr, policy,
                                       [this] { return notify_recv_op(); }
      ).then([this] (auto suite) {
        test_suite.swap(suite);
      });
     }
     case cmd_t::suite_stop:
      ceph_assert(test_suite);
      return test_suite->shutdown().then([this] {
        test_suite.reset();
      });
     case cmd_t::suite_connect_me: {
      ceph_assert(test_suite);
      entity_addr_t test_addr = entity_addr_t();
      test_addr.parse(m_cmd->cmd[1].c_str(), nullptr);
      return test_suite->connect(test_addr);
     }
     case cmd_t::suite_send_me:
      ceph_assert(test_suite);
      return test_suite->send_peer();
     default:
      logger().error("TestPeer got unexpected command {} from Test", m_cmd);
      ceph_abort();
      return seastar::now();
    }
  }

  seastar::future<> init(entity_addr_t cmd_addr) {
    cmd_msgr.set_default_policy(SocketPolicy::stateless_server(0));
    cmd_msgr.set_auth_client(&dummy_auth);
    cmd_msgr.set_auth_server(&dummy_auth);
    return cmd_msgr.bind(entity_addrvec_t{cmd_addr}).then([this] {
      return cmd_msgr.start(this);
    });
  }

 public:
  FailoverTestPeer(Messenger& cmd_msgr)
    : cmd_msgr(cmd_msgr) { }

  entity_addr_t get_addr() const {
    return cmd_msgr.get_myaddr();
  }

  seastar::future<> wait() {
    return cmd_msgr.wait();
  }

  static seastar::future<std::unique_ptr<FailoverTestPeer>> create() {
    return Messenger::create(entity_name_t::OSD(3), "CmdSrv", 3, 0
    ).then([] (Messenger* cmd_msgr) {
      entity_addr_t cmd_addr;
      cmd_addr.parse("v2:127.0.0.1:9011", nullptr);
      auto test_peer = std::make_unique<FailoverTestPeer>(*cmd_msgr);
      return test_peer->init(cmd_addr
      ).then([test_peer = std::move(test_peer)] () mutable {
        logger().info("CmdSrv ready");
        return std::move(test_peer);
      });
    });
  }
};

seastar::future<>
test_v2_lossy_early_connect_fault(FailoverTest& test) {
  return seastar::do_with(std::vector<Breakpoint>{
      {custom_bp_t::BANNER_WRITE},
      {custom_bp_t::BANNER_READ},
      {custom_bp_t::BANNER_PAYLOAD_READ},
      {custom_bp_t::SOCKET_CONNECTING},
      {Tag::HELLO, true},
      {Tag::HELLO, false},
      {Tag::AUTH_REQUEST, true},
      {Tag::AUTH_DONE, false},
      {Tag::AUTH_SIGNATURE, true},
      {Tag::AUTH_SIGNATURE, false},
  }, [&test] (auto& failure_cases) {
    return seastar::do_for_each(failure_cases, [&test] (auto bp) {
      TestInterceptor interceptor;
      interceptor.make_fault(bp);
      return test.run_suite(
          fmt::format("test_v2_lossy_early_connect_fault -- {}", bp),
          interceptor,
          policy_t::lossy_client,
          policy_t::stateless_server,
          [&test] (FailoverSuite& suite) {
        return seastar::futurize_apply([&test] {
          return test.send_bidirectional();
        }).then([&suite] {
          return suite.connect_peer();
        }).then([&suite] {
          return suite.wait_results(1);
        }).then([] (ConnResults& results) {
          results[0].assert_state_at(conn_state_t::established);
          results[0].assert_connect(2, 1, 0, 1);
          results[0].assert_accept(0, 0, 0, 0);
          results[0].assert_reset(0, 0);
        });
      });
    });
  });
}

seastar::future<>
test_v2_lossy_connect_fault(FailoverTest& test) {
  return seastar::do_with(std::vector<Breakpoint>{
      {Tag::CLIENT_IDENT, true},
      {Tag::SERVER_IDENT, false},
  }, [&test] (auto& failure_cases) {
    return seastar::do_for_each(failure_cases, [&test] (auto bp) {
      TestInterceptor interceptor;
      interceptor.make_fault(bp);
      return test.run_suite(
          fmt::format("test_v2_lossy_connect_fault -- {}", bp),
          interceptor,
          policy_t::lossy_client,
          policy_t::stateless_server,
          [&test] (FailoverSuite& suite) {
        return seastar::futurize_apply([&suite] {
          return suite.send_peer();
        }).then([&suite] {
          return suite.connect_peer();
        }).then([&suite] {
          return suite.wait_results(1);
        }).then([] (ConnResults& results) {
          results[0].assert_state_at(conn_state_t::established);
          results[0].assert_connect(2, 2, 0, 1);
          results[0].assert_accept(0, 0, 0, 0);
          results[0].assert_reset(0, 0);
        });
      });
    });
  });
}

seastar::future<>
test_v2_lossy_connected_fault(FailoverTest& test) {
  return seastar::do_with(std::vector<Breakpoint>{
      {Tag::MESSAGE, true},
      {Tag::MESSAGE, false},
  }, [&test] (auto& failure_cases) {
    return seastar::do_for_each(failure_cases, [&test] (auto bp) {
      TestInterceptor interceptor;
      interceptor.make_fault(bp);
      return test.run_suite(
          fmt::format("test_v2_lossy_connected_fault -- {}", bp),
          interceptor,
          policy_t::lossy_client,
          policy_t::stateless_server,
          [&test] (FailoverSuite& suite) {
        return seastar::futurize_apply([&test] {
          return test.send_bidirectional();
        }).then([&suite] {
          return suite.connect_peer();
        }).then([&suite] {
          return suite.wait_results(1);
        }).then([] (ConnResults& results) {
          results[0].assert_state_at(conn_state_t::closed);
          results[0].assert_connect(1, 1, 0, 1);
          results[0].assert_accept(0, 0, 0, 0);
          results[0].assert_reset(1, 0);
        });
      });
    });
  });
}

seastar::future<>
test_v2_lossy_early_accept_fault(FailoverTest& test) {
  return seastar::do_with(std::vector<Breakpoint>{
      {custom_bp_t::BANNER_WRITE},
      {custom_bp_t::BANNER_READ},
      {custom_bp_t::BANNER_PAYLOAD_READ},
      {Tag::HELLO, true},
      {Tag::HELLO, false},
      {Tag::AUTH_REQUEST, false},
      {Tag::AUTH_DONE, true},
      {Tag::AUTH_SIGNATURE, true},
      {Tag::AUTH_SIGNATURE, false},
  }, [&test] (auto& failure_cases) {
    return seastar::do_for_each(failure_cases, [&test] (auto bp) {
      TestInterceptor interceptor;
      interceptor.make_fault(bp);
      return test.run_suite(
          fmt::format("test_v2_lossy_early_accept_fault -- {}", bp),
          interceptor,
          policy_t::stateless_server,
          policy_t::lossy_client,
          [&test] (FailoverSuite& suite) {
        return seastar::futurize_apply([&test] {
          return test.send_bidirectional();
        }).then([&test] {
          return test.peer_connect_me();
        }).then([&suite] {
          return suite.wait_results(2);
        }).then([] (ConnResults& results) {
          results[0].assert_state_at(conn_state_t::closed);
          results[0].assert_connect(0, 0, 0, 0);
          results[0].assert_accept(1, 0, 0, 0);
          results[0].assert_reset(0, 0);
          results[1].assert_state_at(conn_state_t::established);
          results[1].assert_connect(0, 0, 0, 0);
          results[1].assert_accept(1, 1, 0, 1);
          results[1].assert_reset(0, 0);
        });
      });
    });
  });
}

seastar::future<>
test_v2_lossy_accept_fault(FailoverTest& test) {
  return seastar::do_with(std::vector<Breakpoint>{
      {Tag::CLIENT_IDENT, false},
      {Tag::SERVER_IDENT, true},
  }, [&test] (auto& failure_cases) {
    return seastar::do_for_each(failure_cases, [&test] (auto bp) {
      TestInterceptor interceptor;
      interceptor.make_fault(bp);
      return test.run_suite(
          fmt::format("test_v2_lossy_accept_fault -- {}", bp),
          interceptor,
          policy_t::stateless_server,
          policy_t::lossy_client,
          [&test] (FailoverSuite& suite) {
        return seastar::futurize_apply([&test] {
          return test.send_bidirectional();
        }).then([&test] {
          return test.peer_connect_me();
        }).then([&suite] {
          return suite.wait_results(2);
        }).then([] (ConnResults& results) {
          results[0].assert_state_at(conn_state_t::closed);
          results[0].assert_connect(0, 0, 0, 0);
          results[0].assert_accept(1, 1, 0, 0);
          results[0].assert_reset(0, 0);
          results[1].assert_state_at(conn_state_t::established);
          results[1].assert_connect(0, 0, 0, 0);
          results[1].assert_accept(1, 1, 0, 1);
          results[1].assert_reset(0, 0);
        });
      });
    });
  });
}

seastar::future<>
test_v2_lossy_accepted_fault(FailoverTest& test) {
  return seastar::do_with(std::vector<Breakpoint>{
      {Tag::MESSAGE, true},
      {Tag::MESSAGE, false},
  }, [&test] (auto& failure_cases) {
    return seastar::do_for_each(failure_cases, [&test] (auto bp) {
      TestInterceptor interceptor;
      interceptor.make_fault(bp);
      return test.run_suite(
          fmt::format("test_v2_lossy_accepted_fault -- {}", bp),
          interceptor,
          policy_t::stateless_server,
          policy_t::lossy_client,
          [&test] (FailoverSuite& suite) {
        return seastar::futurize_apply([&test] {
          return test.send_bidirectional();
        }).then([&test] {
          return test.peer_connect_me();
        }).then([&suite] {
          return suite.wait_results(1);
        }).then([] (ConnResults& results) {
          results[0].assert_state_at(conn_state_t::closed);
          results[0].assert_connect(0, 0, 0, 0);
          results[0].assert_accept(1, 1, 0, 1);
          results[0].assert_reset(1, 0);
        });
      });
    });
  });
}

seastar::future<>
test_v2_lossless_connect_fault(FailoverTest& test) {
  return seastar::do_with(std::vector<Breakpoint>{
      {Tag::CLIENT_IDENT, true},
      {Tag::SERVER_IDENT, false},
  }, [&test] (auto& failure_cases) {
    return seastar::do_for_each(failure_cases, [&test] (auto bp) {
      TestInterceptor interceptor;
      interceptor.make_fault(bp);
      return test.run_suite(
          fmt::format("test_v2_lossless_connect_fault -- {}", bp),
          interceptor,
          policy_t::lossless_client,
          policy_t::stateful_server,
          [&test] (FailoverSuite& suite) {
        return seastar::futurize_apply([&test] {
          return test.send_bidirectional();
        }).then([&suite] {
          return suite.connect_peer();
        }).then([&suite] {
          return suite.wait_results(1);
        }).then([] (ConnResults& results) {
          results[0].assert_state_at(conn_state_t::established);
          results[0].assert_connect(2, 2, 0, 1);
          results[0].assert_accept(0, 0, 0, 0);
          results[0].assert_reset(0, 0);
        });
      });
    });
  });
}

seastar::future<>
test_v2_lossless_connected_fault(FailoverTest& test) {
  return seastar::do_with(std::vector<Breakpoint>{
      {Tag::MESSAGE, true},
      {Tag::MESSAGE, false},
  }, [&test] (auto& failure_cases) {
    return seastar::do_for_each(failure_cases, [&test] (auto bp) {
      TestInterceptor interceptor;
      interceptor.make_fault(bp);
      return test.run_suite(
          fmt::format("test_v2_lossless_connected_fault -- {}", bp),
          interceptor,
          policy_t::lossless_client,
          policy_t::stateful_server,
          [&test] (FailoverSuite& suite) {
        return seastar::futurize_apply([&test] {
          return test.send_bidirectional();
        }).then([&suite] {
          return suite.connect_peer();
        }).then([&suite] {
          return suite.wait_results(1);
        }).then([] (ConnResults& results) {
          results[0].assert_state_at(conn_state_t::established);
          results[0].assert_connect(2, 1, 1, 2);
          results[0].assert_accept(0, 0, 0, 0);
          results[0].assert_reset(0, 0);
        });
      });
    });
  });
}

seastar::future<>
test_v2_lossless_reconnect_fault(FailoverTest& test) {
  return seastar::do_with(std::vector<std::pair<Breakpoint, Breakpoint>>{
      {{Tag::MESSAGE, true}, {Tag::SESSION_RECONNECT, true}},
      {{Tag::MESSAGE, true}, {Tag::SESSION_RECONNECT_OK, false}},
  }, [&test] (auto& failure_cases) {
    return seastar::do_for_each(failure_cases, [&test] (auto bp_pair) {
      TestInterceptor interceptor;
      interceptor.make_fault(bp_pair.first);
      interceptor.make_fault(bp_pair.second);
      return test.run_suite(
          fmt::format("test_v2_lossless_reconnect_fault -- {}, {}",
                      bp_pair.first, bp_pair.second),
          interceptor,
          policy_t::lossless_client,
          policy_t::stateful_server,
          [&test] (FailoverSuite& suite) {
        return seastar::futurize_apply([&test] {
          return test.send_bidirectional();
        }).then([&suite] {
          return suite.connect_peer();
        }).then([&suite] {
          return suite.wait_results(1);
        }).then([] (ConnResults& results) {
          results[0].assert_state_at(conn_state_t::established);
          results[0].assert_connect(3, 1, 2, 2);
          results[0].assert_accept(0, 0, 0, 0);
          results[0].assert_reset(0, 0);
        });
      });
    });
  });
}

seastar::future<>
test_v2_lossless_accept_fault(FailoverTest& test) {
  return seastar::do_with(std::vector<Breakpoint>{
      {Tag::CLIENT_IDENT, false},
      {Tag::SERVER_IDENT, true},
  }, [&test] (auto& failure_cases) {
    return seastar::do_for_each(failure_cases, [&test] (auto bp) {
      TestInterceptor interceptor;
      interceptor.make_fault(bp);
      return test.run_suite(
          fmt::format("test_v2_lossless_accept_fault -- {}", bp),
          interceptor,
          policy_t::stateful_server,
          policy_t::lossless_client,
          [&test] (FailoverSuite& suite) {
        return seastar::futurize_apply([&test] {
          return test.send_bidirectional();
        }).then([&test] {
          return test.peer_connect_me();
        }).then([&suite] {
          return suite.wait_results(2);
        }).then([] (ConnResults& results) {
          results[0].assert_state_at(conn_state_t::closed);
          results[0].assert_connect(0, 0, 0, 0);
          results[0].assert_accept(1, 1, 0, 0);
          results[0].assert_reset(0, 0);
          results[1].assert_state_at(conn_state_t::established);
          results[1].assert_connect(0, 0, 0, 0);
          results[1].assert_accept(1, 1, 0, 1);
          results[1].assert_reset(0, 0);
        });
      });
    });
  });
}

seastar::future<>
test_v2_lossless_accepted_fault(FailoverTest& test) {
  return seastar::do_with(std::vector<Breakpoint>{
      {Tag::MESSAGE, true},
      {Tag::MESSAGE, false},
  }, [&test] (auto& failure_cases) {
    return seastar::do_for_each(failure_cases, [&test] (auto bp) {
      TestInterceptor interceptor;
      interceptor.make_fault(bp);
      return test.run_suite(
          fmt::format("test_v2_lossless_accepted_fault -- {}", bp),
          interceptor,
          policy_t::stateful_server,
          policy_t::lossless_client,
          [&test] (FailoverSuite& suite) {
        return seastar::futurize_apply([&test] {
          return test.send_bidirectional();
        }).then([&test] {
          return test.peer_connect_me();
        }).then([&suite] {
          return suite.wait_results(2);
        }).then([] (ConnResults& results) {
          results[0].assert_state_at(conn_state_t::established);
          results[0].assert_connect(0, 0, 0, 0);
          results[0].assert_accept(1, 1, 0, 1);
          results[0].assert_reset(0, 0);
          results[1].assert_state_at(conn_state_t::replaced);
          results[1].assert_connect(0, 0, 0, 0);
          results[1].assert_accept(1, 1, 0);
          results[1].assert_reset(0, 0);
        });
      });
    });
  });
}

seastar::future<>
test_v2_lossless_reaccept_fault(FailoverTest& test) {
  return seastar::do_with(std::vector<std::pair<Breakpoint, Breakpoint>>{
      {{Tag::MESSAGE, false}, {Tag::SESSION_RECONNECT, false}},
      {{Tag::MESSAGE, false}, {Tag::SESSION_RECONNECT_OK, true}},
  }, [&test] (auto& failure_cases) {
    return seastar::do_for_each(failure_cases, [&test] (auto bp_pair) {
      TestInterceptor interceptor;
      interceptor.make_fault(bp_pair.first);
      interceptor.make_fault(bp_pair.second);
      return test.run_suite(
          fmt::format("test_v2_lossless_reaccept_fault -- {}, {}",
                      bp_pair.first, bp_pair.second),
          interceptor,
          policy_t::stateful_server,
          policy_t::lossless_client,
          [&test, bp = bp_pair.second] (FailoverSuite& suite) {
        return seastar::futurize_apply([&test] {
          return test.send_bidirectional();
        }).then([&test] {
          return test.peer_connect_me();
        }).then([&suite] {
          return suite.wait_results(3);
        }).then([bp] (ConnResults& results) {
          results[0].assert_state_at(conn_state_t::established);
          results[0].assert_connect(0, 0, 0, 0);
          results[0].assert_accept(1, 1, 0, 1);
          results[0].assert_reset(0, 0);
          if (bp == Breakpoint{Tag::SESSION_RECONNECT, false}) {
            results[1].assert_state_at(conn_state_t::closed);
          } else {
            results[1].assert_state_at(conn_state_t::replaced);
          }
          results[1].assert_connect(0, 0, 0, 0);
          results[1].assert_accept(1, 0, 1, 0);
          results[1].assert_reset(0, 0);
          results[2].assert_state_at(conn_state_t::replaced);
          results[2].assert_connect(0, 0, 0, 0);
          results[2].assert_accept(1, 0, 1, 0);
          results[2].assert_reset(0, 0);
        });
      });
    });
  });
}

seastar::future<>
test_v2_peer_connect_fault(FailoverTest& test) {
  return seastar::do_with(std::vector<Breakpoint>{
      {Tag::CLIENT_IDENT, true},
      {Tag::SERVER_IDENT, false},
  }, [&test] (auto& failure_cases) {
    return seastar::do_for_each(failure_cases, [&test] (auto bp) {
      TestInterceptor interceptor;
      interceptor.make_fault(bp);
      return test.run_suite(
          fmt::format("test_v2_peer_connect_fault -- {}", bp),
          interceptor,
          policy_t::lossless_peer,
          policy_t::lossless_peer,
          [&test] (FailoverSuite& suite) {
        return seastar::futurize_apply([&suite] {
          return suite.send_peer();
        }).then([&suite] {
          return suite.connect_peer();
        }).then([&suite] {
          return suite.wait_results(1);
        }).then([] (ConnResults& results) {
          results[0].assert_state_at(conn_state_t::established);
          results[0].assert_connect(2, 2, 0, 1);
          results[0].assert_accept(0, 0, 0, 0);
          results[0].assert_reset(0, 0);
        });
      });
    });
  });
}

seastar::future<>
test_v2_peer_accept_fault(FailoverTest& test) {
  return seastar::do_with(std::vector<Breakpoint>{
      {Tag::CLIENT_IDENT, false},
      {Tag::SERVER_IDENT, true},
  }, [&test] (auto& failure_cases) {
    return seastar::do_for_each(failure_cases, [&test] (auto bp) {
      TestInterceptor interceptor;
      interceptor.make_fault(bp);
      return test.run_suite(
          fmt::format("test_v2_peer_accept_fault -- {}", bp),
          interceptor,
          policy_t::lossless_peer,
          policy_t::lossless_peer,
          [&test] (FailoverSuite& suite) {
        return seastar::futurize_apply([&test] {
          return test.peer_send_me();
        }).then([&test] {
          return test.peer_connect_me();
        }).then([&suite] {
          return suite.wait_results(2);
        }).then([] (ConnResults& results) {
          results[0].assert_state_at(conn_state_t::closed);
          results[0].assert_connect(0, 0, 0, 0);
          results[0].assert_accept(1, 1, 0, 0);
          results[0].assert_reset(0, 0);
          results[1].assert_state_at(conn_state_t::established);
          results[1].assert_connect(0, 0, 0, 0);
          results[1].assert_accept(1, 1, 0, 1);
          results[1].assert_reset(0, 0);
        });
      });
    });
  });
}

seastar::future<>
test_v2_peer_connected_fault_reconnect(FailoverTest& test) {
  auto bp = Breakpoint{Tag::MESSAGE, true};
  TestInterceptor interceptor;
  interceptor.make_fault(bp);
  return test.run_suite(
      fmt::format("test_v2_peer_connected_fault_reconnect -- {}", bp),
      interceptor,
      policy_t::lossless_peer,
      policy_t::lossless_peer,
      [&test] (FailoverSuite& suite) {
    return seastar::futurize_apply([&suite] {
      return suite.send_peer();
    }).then([&suite] {
      return suite.connect_peer();
    }).then([&suite] {
      return suite.wait_results(1);
    }).then([] (ConnResults& results) {
      results[0].assert_state_at(conn_state_t::established);
      results[0].assert_connect(2, 1, 1, 2);
      results[0].assert_accept(0, 0, 0, 0);
      results[0].assert_reset(0, 0);
    });
  });
}

seastar::future<>
test_v2_peer_connected_fault_reaccept(FailoverTest& test) {
  auto bp = Breakpoint{Tag::MESSAGE, false};
  TestInterceptor interceptor;
  interceptor.make_fault(bp);
  return test.run_suite(
      fmt::format("test_v2_peer_connected_fault_reaccept -- {}", bp),
      interceptor,
      policy_t::lossless_peer,
      policy_t::lossless_peer,
      [&test] (FailoverSuite& suite) {
    return seastar::futurize_apply([&test] {
      return test.peer_send_me();
    }).then([&suite] {
      return suite.connect_peer();
    }).then([&suite] {
      return suite.wait_results(2);
    }).then([] (ConnResults& results) {
      results[0].assert_state_at(conn_state_t::established);
      results[0].assert_connect(1, 1, 0, 1);
      results[0].assert_accept(0, 0, 0, 0);
      results[0].assert_reset(0, 0);
      results[1].assert_state_at(conn_state_t::replaced);
      results[1].assert_connect(0, 0, 0, 0);
      results[1].assert_accept(1, 0, 1, 0);
      results[1].assert_reset(0, 0);
    });
  });
}

seastar::future<>
test_v2_failover(entity_addr_t test_addr = entity_addr_t(),
                 entity_addr_t cmd_peer_addr = entity_addr_t()) {
  if (test_addr == entity_addr_t() || cmd_peer_addr == entity_addr_t()) {
    // initiate crimson test peer locally
    logger().info("test_v2_failover: start local TestPeer...");
    return FailoverTestPeer::create().then([] (auto peer) {
      entity_addr_t test_addr_;
      test_addr_.parse("v2:127.0.0.1:9010");
      return test_v2_failover(test_addr_, peer->get_addr()
      ).finally([peer = std::move(peer)] () mutable {
        return peer->wait().then([peer = std::move(peer)] {});
      });
    }).handle_exception([] (auto eptr) {
      logger().error("FailoverTestPeer: got exception {}", eptr);
      throw;
    });
  }

  test_addr.set_nonce(2);
  return FailoverTest::create(cmd_peer_addr, test_addr).then([] (auto test) {
    return seastar::futurize_apply([test] {
      return test_v2_lossy_early_connect_fault(*test);
    }).then([test] {
      return test_v2_lossy_connect_fault(*test);
    }).then([test] {
      return test_v2_lossy_connected_fault(*test);
    }).then([test] {
      return test_v2_lossy_early_accept_fault(*test);
    }).then([test] {
      return test_v2_lossy_accept_fault(*test);
    }).then([test] {
      return test_v2_lossy_accepted_fault(*test);
    }).then([test] {
      return test_v2_lossless_connect_fault(*test);
    }).then([test] {
      return test_v2_lossless_connected_fault(*test);
    }).then([test] {
      return test_v2_lossless_reconnect_fault(*test);
    }).then([test] {
      return test_v2_lossless_accept_fault(*test);
    }).then([test] {
      return test_v2_lossless_accepted_fault(*test);
    }).then([test] {
      return test_v2_lossless_reaccept_fault(*test);
    }).then([test] {
      return test_v2_peer_connect_fault(*test);
    }).then([test] {
      return test_v2_peer_accept_fault(*test);
    }).then([test] {
      return test_v2_peer_connected_fault_reconnect(*test);
    }).then([test] {
      return test_v2_peer_connected_fault_reaccept(*test);
    }).finally([test] {
      return test->shutdown().then([test] {});
    });
  }).handle_exception([] (auto eptr) {
    logger().error("FailoverTest: got exception {}", eptr);
    throw;
  });
}

}

int main(int argc, char** argv)
{
  seastar::app_template app;
  app.add_options()
    ("verbose,v", bpo::value<bool>()->default_value(false),
     "chatty if true")
    ("rounds", bpo::value<unsigned>()->default_value(512),
     "number of pingpong rounds")
    ("keepalive-ratio", bpo::value<double>()->default_value(0.1),
     "ratio of keepalive in ping messages");
  return app.run(argc, argv, [&app] {
    auto&& config = app.configuration();
    verbose = config["verbose"].as<bool>();
    auto rounds = config["rounds"].as<unsigned>();
    auto keepalive_ratio = config["keepalive-ratio"].as<double>();
    return test_echo(rounds, keepalive_ratio, false)
    .then([rounds, keepalive_ratio] {
      return test_echo(rounds, keepalive_ratio, true);
    }).then([] {
      return test_concurrent_dispatch(false);
    }).then([] {
      return test_concurrent_dispatch(true);
    }).then([] {
      return test_preemptive_shutdown(false);
    }).then([] {
      return test_preemptive_shutdown(true);
    }).then([] {
      return test_v2_failover();
    }).then([] {
      std::cout << "All tests succeeded" << std::endl;
    }).handle_exception([] (auto eptr) {
      std::cout << "Test failure" << std::endl;
      return seastar::make_exception_future<>(eptr);
    });
  });
}
