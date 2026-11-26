// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include "posix_io_uring.h"

#include "acconfig.h"
#include "common/errno.h"
#include "common/dout.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <deque>
#include <limits>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include <fcntl.h>
#include <linux/openat2.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <unistd.h>
#include <cerrno>

#define dout_subsys ceph_subsys_rgw
#define dout_context g_ceph_context

#if defined(HAVE_LIBURING)
#include <liburing.h>
#include <sys/eventfd.h>
#include <boost/asio/associated_executor.hpp>
#include <boost/asio/error.hpp>
#include <boost/asio/post.hpp>
#include <boost/system/system_error.hpp>
#include "common/async/yield_waiter.h"
#endif

namespace rgw::sal {

namespace {

/* Set by posix_try_use_uring() and posix_uring_ensure_ring(dpp, nsfs):
 * 0=posix, 1=nsfs, -1=unset.  Names the driver whose options size a
 * ring made by the one-argument posix_uring_ensure_ring(). */
std::atomic<int> uring_driver_nsfs{-1};

int uring_err(const DoutPrefixProvider* dpp, int err, const char* what)
{
  ldpp_dout(dpp, 1) << "URING: ERROR: " << what << " : "
                    << cpp_strerror(err < 0 ? -err : err)
                    << " (" << err << ")" << dendl;
  return err;
}

int pwrite_full(int fd, const char* buf, int64_t len, int64_t ofs,
                const DoutPrefixProvider* dpp)
{
  int64_t left = len;
  int64_t off = ofs;
  const char* p = buf;
  while (left > 0) {
    ssize_t n = ::pwrite(fd, p, left, off);
    if (n < 0) {
      int err = errno;
      return uring_err(dpp, -err, "pwrite");
    }
    if (n == 0) {
      return uring_err(dpp, -EIO, "pwrite returned 0");
    }
    p += n;
    off += n;
    left -= n;
  }
  return 0;
}

unsigned round_up_pow2(unsigned n)
{
  constexpr unsigned MAX_POW2 =
      1u << (std::numeric_limits<unsigned>::digits - 1);
  if (n <= 1) {
    return 1;
  }
  if (n > MAX_POW2) {
    return MAX_POW2;
  }
  --n;
  for (unsigned s = 1; s < sizeof(unsigned) * 8; s <<= 1) {
    n |= n >> s;
  }
  return n + 1;
}

/* Truncate only alignment padding we appended past the previous EOF.
 * If old_size already extended into the pad window, those bytes stay. */
int truncate_pad_if_appended(int fd, int64_t logical, int64_t write_end,
                             int64_t old_size, const DoutPrefixProvider* dpp)
{
  if (old_size <= logical && write_end > logical) {
    if (::ftruncate(fd, logical) < 0) {
      return uring_err(dpp, -errno, "ftruncate pad");
    }
  }
  return 0;
}

} // anonymous namespace

int posix_direct_write(int fd, int64_t ofs, bufferlist& bl,
                       const DoutPrefixProvider* dpp)
{
  int64_t len = bl.length();
  if (len == 0) {
    return 0;
  }

  if ((ofs % POSIX_DIRECT_ALIGN) == 0 && (len % POSIX_DIRECT_ALIGN) == 0) {
    bl.rebuild_aligned_size_and_memory(POSIX_DIRECT_ALIGN, POSIX_DIRECT_ALIGN);
    return pwrite_full(fd, bl.c_str(), len, ofs, dpp);
  }

  struct stat st;
  if (::fstat(fd, &st) < 0) {
    return uring_err(dpp, -errno, "posix_direct_write fstat");
  }
  const int64_t old_size = st.st_size;

  if ((ofs % POSIX_DIRECT_ALIGN) == 0) {
    int64_t alen = posix_align_up(len);
    int64_t logical = ofs + len;
    bufferptr bp(buffer::create_small_page_aligned(alen));
    if (old_size > logical) {
      ssize_t n = ::pread(fd, bp.c_str(), alen, ofs);
      if (n < 0) {
        return uring_err(dpp, -errno, "posix_direct_write pread tail");
      }
      if (n < alen) {
        memset(bp.c_str() + n, 0, alen - n);
      }
    } else {
      memset(bp.c_str(), 0, alen);
    }
    memcpy(bp.c_str(), bl.c_str(), len);
    int r = pwrite_full(fd, bp.c_str(), alen, ofs, dpp);
    if (r < 0) {
      return uring_err(dpp, r, "posix_direct_write pwrite_full aligned");
    }
    r = truncate_pad_if_appended(fd, logical, ofs + alen, old_size, dpp);
    if (r < 0) {
      return uring_err(dpp, r, "posix_direct_write truncate_pad aligned");
    }
    return 0;
  }

  /* Unaligned offset: read-modify-write of an aligned window. */
  int64_t aofs = posix_align_down(ofs);
  int64_t front = ofs - aofs;
  int64_t alen = posix_align_up(front + len);
  int64_t logical = ofs + len;
  bufferptr bp(buffer::create_small_page_aligned(alen));
  memset(bp.c_str(), 0, alen);
  ssize_t n = ::pread(fd, bp.c_str(), alen, aofs);
  if (n < 0) {
    return uring_err(dpp, -errno, "posix_direct_write RMW pread");
  }
  memcpy(bp.c_str() + front, bl.c_str(), len);
  int r = pwrite_full(fd, bp.c_str(), alen, aofs, dpp);
  if (r < 0) {
    return uring_err(dpp, r, "posix_direct_write pwrite_full RMW");
  }
  r = truncate_pad_if_appended(fd, logical, aofs + alen, old_size, dpp);
  if (r < 0) {
    return uring_err(dpp, r, "posix_direct_write truncate_pad RMW");
  }
  return 0;
}

bool posix_io_engine_is_uring(CephContext* cct, const char* engine_opt)
{
  return cct->_conf.get_val<std::string>(engine_opt) == "io_uring";
}

bool posix_try_use_uring(const DoutPrefixProvider* dpp, optional_yield y,
                         bool nsfs)
{
  if (!y) {
    return false;
  }
  const char* engine_opt = nsfs ? "rgw_nsfs_io_engine" : "rgw_posix_io_engine";
  if (!posix_io_engine_is_uring(dpp->get_cct(), engine_opt)) {
    return false;
  }
  if (!posix_uring_ensure_ring(dpp, nsfs)) {
    thread_local bool logged_fallback = false;
    if (!logged_fallback) {
      logged_fallback = true;
      ldpp_dout(dpp, 1) << "URING: ERROR: io_uring requested but ring init "
                           "failed; falling back to sync engine on this thread"
                        << dendl;
    }
    return false;
  }
  return true;
}

unsigned posix_sync_clamp_iodepth(const DoutPrefixProvider* dpp, unsigned qd,
                                  const char* iodepth_opt)
{
  if (qd > 2) {
    ldpp_dout(dpp, 1) << "URING: WARNING: clamping " << iodepth_opt << "=" << qd
                      << " to 2 for sync IO engine" << dendl;
    return 2;
  }
  if (qd < 1) {
    return 1;
  }
  return qd;
}

#if defined(HAVE_LIBURING)

namespace {

struct CqeDispatch {
  void (*fn)(io_uring_cqe* cqe, void* arg) = nullptr;
  void* arg = nullptr;
};

/* Init failures must be visible with --debug_rgw=0. */
int uring_init_err(const DoutPrefixProvider* dpp, int err, const char* what,
                   unsigned entries, unsigned flags, unsigned idle_ms)
{
  struct rlimit rl {};
  getrlimit(RLIMIT_MEMLOCK, &rl);
  ldpp_dout(dpp, 1) << "URING: ERROR: " << what << ": "
                    << cpp_strerror(err < 0 ? -err : err)
                    << " (" << err << ")"
                    << " entries=" << entries
                    << " flags=0x" << std::hex << flags << std::dec
                    << " SQPOLL=" << bool(flags & IORING_SETUP_SQPOLL)
                    << " idle_ms=" << idle_ms
                    << " RLIMIT_MEMLOCK=" << rl.rlim_cur
                    << dendl;
  if (err == -ENOSYS) {
    static std::once_flag enosys_once;
    std::call_once(enosys_once, [dpp]() {
      ldpp_dout(dpp, 1) << "URING: ERROR: kernel returned ENOSYS for io_uring; "
                           "liburing is linked but this kernel has no "
                           "io_uring syscall. Engine will fall back to sync."
                        << dendl;
    });
  }
  return err;
}

struct ThreadIoUringState {
  io_uring ring{};
  bool initialized = false;
  int init_error = 0;
  int event_fd = -1;
  std::thread reaper_thread;
  std::atomic<bool> shutdown{false};

  int init(const DoutPrefixProvider* dpp, unsigned queue_depth, unsigned flags,
           unsigned sq_thread_idle_ms) {
    if (initialized) {
      return 0;
    }
    if (init_error < 0) {
      return uring_err(dpp, init_error, "init previous failure");
    }

    /* COOP_TASKRUN / DEFER_TASKRUN require the submitter to io_uring_enter
     * to flush CQEs, which fights the eventfd reaper. Do not set them.
     * SINGLE_ISSUER is opt-in via rgw_{posix,nsfs}_io_uring_single_issuer
     * and is already folded into flags by posix_uring_ensure_ring(). */

    auto queue_init = [&](unsigned f) {
      struct io_uring_params params{};
      params.flags = f;
      if (f & IORING_SETUP_SQPOLL) {
        params.sq_thread_idle = sq_thread_idle_ms;
      }
      return io_uring_queue_init_params(queue_depth, &ring, &params);
    };

    int ret = queue_init(flags);
#ifdef IORING_SETUP_SINGLE_ISSUER
    if (ret == -EINVAL && (flags & IORING_SETUP_SINGLE_ISSUER)) {
      ldpp_dout(dpp, 1) << "URING: WARNING: SINGLE_ISSUER rejected ("
                        << cpp_strerror(-ret)
                        << "), retrying without it" << dendl;
      flags &= ~IORING_SETUP_SINGLE_ISSUER;
      ret = queue_init(flags);
    }
#endif
    if ((flags & IORING_SETUP_SQPOLL) &&
        (ret == -EPERM || ret == -EINVAL || ret == -ENOSYS)) {
      ldpp_dout(dpp, 1) << "URING: WARNING: SQPOLL rejected ("
                        << cpp_strerror(-ret)
                        << "), retrying without it" << dendl;
      flags = 0;
      ret = queue_init(0);
    }
    if (ret < 0) {
      if (ret == -ENOMEM) {
        struct rlimit rl{};
        ::getrlimit(RLIMIT_MEMLOCK, &rl);
        ldpp_dout(dpp, 0) << "ERROR: URING: cannot create this thread's ring:"
          " out of locked memory.  Ring memory is charged against"
          " RLIMIT_MEMLOCK, currently " << rl.rlim_cur << " bytes, and every"
          " serving thread creates one.  Raise the limit, or reduce the"
          " ring depth" << dendl;
      }
      init_error = uring_init_err(dpp, ret, "io_uring_queue_init_params",
                                  queue_depth, flags, sq_thread_idle_ms);
      return ret;
    }

    event_fd = ::eventfd(0, EFD_CLOEXEC);
    if (event_fd < 0) {
      int err = errno;
      io_uring_queue_exit(&ring);
      init_error = uring_init_err(dpp, -err, "eventfd",
                                  queue_depth, flags, sq_thread_idle_ms);
      return -err;
    }

    ret = io_uring_register_eventfd(&ring, event_fd);
    if (ret < 0) {
      ::close(event_fd);
      event_fd = -1;
      io_uring_queue_exit(&ring);
      init_error = uring_init_err(dpp, ret, "io_uring_register_eventfd",
                                  queue_depth, flags, sq_thread_idle_ms);
      return ret;
    }

    shutdown = false;
    reaper_thread = std::thread([this]() { reap_loop(); });
    initialized = true;
    ldpp_dout(dpp, 1) << "URING: ring ready flags=0x" << std::hex << flags
                      << std::dec
                      << " SQPOLL=" << bool(flags & IORING_SETUP_SQPOLL)
#ifdef IORING_SETUP_SINGLE_ISSUER
                      << " SINGLE_ISSUER="
                      << bool(flags & IORING_SETUP_SINGLE_ISSUER)
#endif
                      << " entries=" << queue_depth
                      << " sq_thread_idle_ms=" << sq_thread_idle_ms << dendl;
    return 0;
  }

  void reap_loop() {
    while (true) {
      uint64_t val = 0;
      ssize_t n = ::read(event_fd, &val, sizeof(val));
      if (n < 0) {
        if (errno == EINTR) {
          continue;
        }
        if (shutdown) {
          break;
        }
        ldout(g_ceph_context, 1) << "URING: eventfd read failed: "
                                 << cpp_strerror(errno) << dendl;
        continue;
      }
      struct io_uring_cqe* cqe;
      while (io_uring_peek_cqe(&ring, &cqe) == 0) {
        auto* disp = static_cast<CqeDispatch*>(io_uring_cqe_get_data(cqe));
        if (disp && disp->fn) {
          disp->fn(cqe, disp->arg);
        }
        io_uring_cqe_seen(&ring, cqe);
      }
      if (shutdown) {
        break;
      }
    }
  }

  ~ThreadIoUringState() {
    if (initialized) {
      shutdown = true;
      if (event_fd >= 0) {
        uint64_t val = 1;
        [[maybe_unused]] auto _ = ::write(event_fd, &val, sizeof(val));
      }
      if (reaper_thread.joinable()) {
        reaper_thread.join();
      }
      io_uring_queue_exit(&ring);
      if (event_fd >= 0) {
        ::close(event_fd);
      }
    }
  }
};

/* Per-worker ring + eventfd reaper. Not request-affine: a strand may
 * resume this coroutine on another io_context thread after yield, and
 * that thread has a distinct (initially zeroed) instance. */
thread_local ThreadIoUringState thread_uring_state;

struct IoSlot {
  std::shared_ptr<void> file_owner;
  CqeDispatch disp;
  bufferptr buf;
  bufferlist hold; /* keep PUT payload alive until CQE */
  int64_t user_ofs = 0;
  int64_t user_len = 0;
  int64_t aligned_ofs = 0;
  int64_t aligned_len = 0;
  int result = 0;
  bool done = false;
  bool is_write = false;
  bool submitted = false;
  std::mutex mutex;
  ceph::async::yield_waiter<int> waiter;
  /* For callers with no coroutine to suspend.  An open replaces a
   * blocking openat(2), so blocking here is what it already did;
   * and librgw has no yield context at all, so a yield-only wait
   * would exclude the NFS gateway.  Also where a cancelled
   * wait_slot() blocks until the kernel is done with the slot. */
  std::condition_variable blocking_waiter;

  const char* data_ptr() {
    return buf.length() ? buf.c_str() : hold.c_str();
  }

  static void handle_cqe(io_uring_cqe* cqe, void* arg) {
    auto* slot = static_cast<IoSlot*>(arg);
    /* Everything under the lock, and nothing after it.  The submitter
     * frees the slot as soon as it sees `done` -- an open's slot is a
     * local of the submitting frame -- so once the lock is released
     * the slot may be gone.  Completing under it is safe because this
     * is the reaper, not an executor thread:  complete() posts the
     * handler, which takes the lock again before it resumes. */
    std::lock_guard lock(slot->mutex);
    slot->result = cqe->res;
    slot->done = true;
    if (slot->waiter) {
      slot->waiter.complete(boost::system::error_code{}, slot->result);
    }
    /* harmless when nobody is blocked;  the reaper is a separate
     * thread, so it can complete a blocked submitter */
    slot->blocking_waiter.notify_all();
  }
};

/* `log_result`:  a negative CQE is an error worth logging for a data
 * transfer, but an expected answer for some chain ops (statx, unlinkat
 * of a name that is not there). */
int wait_slot(const DoutPrefixProvider* dpp, IoSlot& slot, optional_yield y,
              bool log_result = true)
{
  std::unique_lock lock(slot.mutex);
  bool cancelled = false;
  if (!slot.done && y) {
    try {
      slot.result = slot.waiter.async_wait(lock, y.get_yield_context());
    } catch (const boost::system::system_error& e) {
      if (e.code() != boost::asio::error::operation_aborted) {
        throw;
      }
      cancelled = true;
    }
  }
  /* Cancellation resumes before the CQE. Block until the kernel is done
   * so the buffer and CqeDispatch stay valid for the reaper. */
  if (!slot.done) {
    slot.blocking_waiter.wait(lock, [&slot] { return slot.done; });
  }
  if (cancelled) {
    return uring_err(dpp, -ECANCELED, "wait_slot cancelled");
  }
  if (log_result && slot.result < 0) {
    return uring_err(dpp, slot.result, "wait_slot cqe");
  }
  return slot.result;
}

/* Wait without a coroutine.  Used by the open path, which replaces
 * a blocking syscall and so blocks exactly as that did. */
int wait_slot_blocking(IoSlot& slot)
{
  std::unique_lock lock(slot.mutex);
  slot.blocking_waiter.wait(lock, [&slot] { return slot.done; });
  return slot.result;
}

int submit_prepared(const DoutPrefixProvider* dpp)
{
  if (!posix_uring_ensure_ring(dpp)) {
    return uring_err(dpp, -EIO, "submit_prepared ring not ready");
  }
  for (int i = 0; i < 16; ++i) {
    int submitted = io_uring_submit(&thread_uring_state.ring);
    if (submitted >= 0) {
      return submitted;
    }
    if (submitted != -EBUSY && submitted != -EAGAIN) {
      return uring_err(dpp, submitted, "io_uring_submit");
    }
  }
  return uring_err(dpp, -EBUSY, "io_uring_submit still busy");
}

io_uring_sqe* get_sqe_retry(const DoutPrefixProvider* dpp)
{
  if (!posix_uring_ensure_ring(dpp)) {
    uring_err(dpp, -EIO, "get_sqe ring not ready");
    return nullptr;
  }
  return io_uring_get_sqe(&thread_uring_state.ring);
}

/* For a single blocking op (open, mkdir, an attribute):  submit what is
 * queued to make room.  The windows do not use this; they track their
 * own unsubmitted SQEs (flush_submit()) and yield for room instead. */
io_uring_sqe* get_sqe_making_room(const DoutPrefixProvider* dpp)
{
  for (int i = 0; i < 8; ++i) {
    io_uring_sqe* sqe = get_sqe_retry(dpp);
    if (sqe) {
      return sqe;
    }
    submit_prepared(dpp);
  }
  return nullptr;
}

void yield_once(optional_yield y)
{
  if (!y) {
    return;
  }
  auto& yield = y.get_yield_context();
  boost::asio::post(boost::asio::get_associated_executor(yield), yield);
}

int prep_rw(const DoutPrefixProvider* dpp, IoSlot& slot, int fd, bool write,
            bool nsfs, uint16_t personality)
{
  /* A window yields between submissions, and its coroutine may resume
   * on another of the frontend's threads.  The ring is per thread, so
   * this thread's is the one to submit on, made here if it has none.
   * -ENXIO when it cannot be made:  the caller transfers synchronously
   * (run_sync()). */
  if (!posix_uring_ensure_ring(dpp, nsfs)) {
    ldpp_dout(dpp, 20) << "URING: this thread has no ring;  transferring "
		       << slot.aligned_len << " bytes synchronously" << dendl;
    return -ENXIO;
  }
  io_uring_sqe* sqe = get_sqe_retry(dpp);
  if (!sqe) {
    return uring_err(dpp, -EAGAIN, "get_sqe");
  }
  /* The whole of impersonation on the data path:  the kernel applies
   * this slot's registered credentials to this one operation.  Zero
   * means the submitting task's own, which is what every caller did
   * before this existed. */
  sqe->personality = personality;
  slot.disp.fn = &IoSlot::handle_cqe;
  slot.disp.arg = &slot;
  if (write) {
    io_uring_prep_write(sqe, fd, slot.data_ptr(), slot.aligned_len,
                        slot.aligned_ofs);
  } else {
    io_uring_prep_read(sqe, fd, slot.buf.c_str(), slot.aligned_len,
                       slot.aligned_ofs);
  }
  io_uring_sqe_set_data(sqe, &slot.disp);
  return 0;
}

/* The transfer prep_rw() would have queued, done on this thread, for a
 * thread with no ring.  The slot is complete before it joins the
 * window, so wait_slot() finds it done and does not suspend, and the
 * window's order is kept.  Runs with the thread's own credentials, as
 * the sync engine's transfers do. */
void run_sync(IoSlot& slot, int fd, bool write)
{
  ssize_t n;
  do {
    n = write ? ::pwrite(fd, slot.data_ptr(), slot.aligned_len,
			 slot.aligned_ofs)
	      : ::pread(fd, slot.buf.c_str(), slot.aligned_len,
			slot.aligned_ofs);
  } while ((n < 0) && (errno == EINTR));
  const int res = (n < 0) ? -errno : static_cast<int>(n);
  std::lock_guard lock(slot.mutex);
  slot.result = res;
  slot.done = true;
}

/* run_sync() as the window's identity, where it has one. */
void run_sync_as(const DoutPrefixProvider* dpp, UringPersonality* who,
		 IoSlot& slot, int fd, bool write, optional_yield y)
{
  if (!who) {
    run_sync(slot, fd, write);
    return;
  }
  const int r = who->run_as(dpp, [&slot, fd, write]() -> int {
    run_sync(slot, fd, write);
    return 0;
  }, y);
  if (r < 0) {
    /* the identity could not be assumed, so nothing ran:  the slot
     * completes with the error rather than being waited on for ever */
    std::lock_guard lock(slot.mutex);
    if (!slot.done) {
      slot.result = r;
      slot.done = true;
    }
  }
}

/* An id acquired for submissions from this thread, released when the
 * scope that submitted them ends -- after the submit, before any
 * yield. */
class HeldPersonality {
  UringPersonality* who;
  uint16_t pid{POSIX_URING_NO_PERSONALITY};
  bool held{false};
public:
  explicit HeldPersonality(UringPersonality* who) : who(who) {}
  ~HeldPersonality() {
    if (held) {
      who->release();
    }
  }
  HeldPersonality(const HeldPersonality&) = delete;
  HeldPersonality& operator=(const HeldPersonality&) = delete;

  int acquire(const DoutPrefixProvider* dpp) {
    if (!who) {
      return 0;
    }
    const int r = who->acquire(dpp, &pid);
    held = (r == 0);
    return r;
  }
  uint16_t id() const { return pid; }
};

/* Between attempts to submit or to find room:  yield where the window
 * may, and otherwise back off on this thread.  A window with a
 * personality may not yield -- the id it holds names a credential only
 * on this thread's ring, and its queued SQEs carry it -- and cannot
 * leave SQEs that point at its slots queued, so it waits here, as
 * posix_uring_chain() does.  False when, after a yield, this thread
 * cannot make a ring. */
bool pause_for_ring(const DoutPrefixProvider* dpp, optional_yield y,
                    bool can_yield, bool nsfs,
                    std::chrono::milliseconds& backoff)
{
  if (can_yield) {
    yield_once(y);
    return posix_uring_ensure_ring(dpp, nsfs);
  }
  std::this_thread::sleep_for(backoff);
  backoff = std::min(backoff * 2, std::chrono::milliseconds(100));
  return true;
}

} // anonymous namespace

int posix_uring_register_personality(const DoutPrefixProvider* dpp)
{
  /* Create the ring if this thread has not needed one yet.
   *
   * Registration happens in prepare(), before any data transfer has
   * asked for a ring -- and it is needed even when the data engine
   * is sync, because the open goes through the ring regardless of
   * how the bytes afterwards travel. */
  if (!posix_uring_ensure_ring(dpp, /*nsfs=*/true)) {
    return -ENXIO;
  }
  const int id = io_uring_register_personality(&thread_uring_state.ring);
  if (id < 0) {
    ldpp_dout(dpp, 0) << "ERROR: URING: io_uring_register_personality: "
                      << cpp_strerror(-id) << dendl;
  }
  return id;
}

void posix_uring_unregister_personality(const DoutPrefixProvider* dpp,
                                        uint16_t id)
{
  if (!thread_uring_state.initialized) {
    return;
  }
  const int r = io_uring_unregister_personality(&thread_uring_state.ring, id);
  if (r < 0) {
    ldpp_dout(dpp, 5) << "URING: io_uring_unregister_personality(" << id
                      << "): " << cpp_strerror(-r) << dendl;
  }
}

bool posix_uring_ensure_ring(const DoutPrefixProvider* dpp, bool nsfs)
{
  /* latched for the one-argument form below;  load first, so the hot
   * path does not write a shared cache line */
  const int want = nsfs ? 1 : 0;
  if (uring_driver_nsfs.load(std::memory_order_relaxed) != want) {
    uring_driver_nsfs.store(want, std::memory_order_release);
  }

  if (thread_uring_state.initialized) {
    return true;
  }
  if (thread_uring_state.init_error < 0) {
    return false;
  }

  CephContext* cct = dpp->get_cct();
  const char* prefix = nsfs ? "rgw_nsfs" : "rgw_posix";

  /* Size the ring to what will actually be submitted on it.
   *
   * A ring is per thread and its memory is accounted against
   * RLIMIT_MEMLOCK, which is 8 MiB by default -- so the depth is
   * multiplied by the frontend's thread count and measured against
   * a small fixed budget.  A 1024-entry ring on each of a couple of
   * hundred threads wants more than that limit allows, and the
   * thread that loses cannot create a ring at all.
   *
   * When the data engine is synchronous the ring still exists,
   * because an impersonated open goes through it to carry the
   * personality (see posix_uring_register_personality) -- but
   * nothing else does.  Those submissions are one at a time and
   * waited on individually, so a handful of entries is the whole
   * requirement and the configured data depth is irrelevant. */
  unsigned entries;
  if (posix_io_engine_is_uring(cct, (std::string(prefix) + "_io_engine").c_str())) {
    entries = cct->_conf.get_val<uint64_t>(
	std::string(prefix) + "_io_uring_queue_depth");
    if (entries < POSIX_URING_MAX_IODEPTH) {
      entries = POSIX_URING_MAX_IODEPTH;
    }
  } else if (nsfs) {
    /* its links, renames and attribute operations as well as its opens
     * (posix_uring_chain()):  sized by an option, for rings made after */
    entries = cct->_conf.get_val<uint64_t>(
	"rgw_nsfs_io_uring_control_entries");
  } else {
    entries = POSIX_URING_CONTROL_ENTRIES;
  }
  entries = round_up_pow2(entries);

  unsigned flags = 0;
  unsigned idle_ms = 0;
  if (cct->_conf.get_val<bool>(std::string(prefix) + "_io_uring_sqpoll")) {
    flags |= IORING_SETUP_SQPOLL;
    idle_ms = cct->_conf.get_val<uint64_t>(
        std::string(prefix) + "_io_uring_sq_thread_idle_ms");
  }
  if (cct->_conf.get_val<bool>(
          std::string(prefix) + "_io_uring_single_issuer")) {
#ifdef IORING_SETUP_SINGLE_ISSUER
    flags |= IORING_SETUP_SINGLE_ISSUER;
#else
    static std::once_flag single_issuer_once;
    std::call_once(single_issuer_once, [dpp, prefix]() {
      ldpp_dout(dpp, 1) << "URING: " << prefix
                        << "_io_uring_single_issuer=true but "
                           "IORING_SETUP_SINGLE_ISSUER is not available in "
                           "this build (need liburing/kernel 6.0+); ignoring"
                        << dendl;
    });
#endif
  }

  int ret = thread_uring_state.init(dpp, entries, flags, idle_ms);
  return ret == 0;
}

bool posix_uring_ensure_ring(const DoutPrefixProvider* dpp)
{
  if (thread_uring_state.initialized) {
    return true;
  }
  int n = uring_driver_nsfs.load(std::memory_order_acquire);
  if (n < 0) {
    ldpp_dout(dpp, 1) << "URING: ERROR: ensure_ring driver not latched "
                         "(posix_try_use_uring did not run first)"
                      << dendl;
    return false;
  }
  return posix_uring_ensure_ring(dpp, n != 0);
}

int posix_uring_openat2(const DoutPrefixProvider* dpp, int dirfd,
                        const char* path, uint64_t flags, uint64_t mode,
                        uint16_t personality, uint64_t resolve)
{
  if (!posix_uring_ensure_ring(dpp, /*nsfs=*/true)) {
    return -ENXIO;
  }

  /* RESOLVE_NO_MAGICLINKS keeps a symlink to /proc out of the
   * resolution, for every caller;  a caller resolving several
   * components at once adds RESOLVE_BENEATH and RESOLVE_NO_SYMLINKS
   * through `resolve`. */
  struct open_how how = {};
  how.flags = flags;
  how.mode = (flags & (O_CREAT | O_TMPFILE)) ? mode : 0;
  how.resolve = RESOLVE_NO_MAGICLINKS | resolve;

  IoSlot slot;
  slot.disp.fn = &IoSlot::handle_cqe;
  slot.disp.arg = &slot;

  io_uring_sqe* sqe = get_sqe_making_room(dpp);
  if (!sqe) {
    ldpp_dout(dpp, 0) << "ERROR: URING: no SQE for openat2" << dendl;
    return -EAGAIN;
  }
  io_uring_prep_openat2(sqe, dirfd, path, &how);
  sqe->personality = personality;
  io_uring_sqe_set_data(sqe, &slot.disp);

  const int sub = submit_prepared(dpp);
  if (sub < 0) {
    return sub;
  }
  return wait_slot_blocking(slot);
}

int posix_uring_fsetxattr(const DoutPrefixProvider* dpp, int fd,
                          const char* name, const void* value, size_t len,
                          int flags, uint16_t personality)
{
  if (!posix_uring_ensure_ring(dpp, /*nsfs=*/true)) {
    return -ENXIO;
  }
  IoSlot slot;
  slot.disp.fn = &IoSlot::handle_cqe;
  slot.disp.arg = &slot;

  io_uring_sqe* sqe = get_sqe_making_room(dpp);
  if (!sqe) {
    return -EAGAIN;
  }
  io_uring_prep_fsetxattr(sqe, fd, name, static_cast<const char*>(value),
                          flags, len);
  sqe->personality = personality;
  io_uring_sqe_set_data(sqe, &slot.disp);

  const int sub = submit_prepared(dpp);
  if (sub < 0) {
    return sub;
  }
  return wait_slot_blocking(slot);
}

int posix_uring_fgetxattr(const DoutPrefixProvider* dpp, int fd,
                          const char* name, void* value, size_t len,
                          uint16_t personality)
{
  if (!posix_uring_ensure_ring(dpp, /*nsfs=*/true)) {
    return -ENXIO;
  }
  IoSlot slot;
  slot.disp.fn = &IoSlot::handle_cqe;
  slot.disp.arg = &slot;

  io_uring_sqe* sqe = get_sqe_making_room(dpp);
  if (!sqe) {
    return -EAGAIN;
  }
  io_uring_prep_fgetxattr(sqe, fd, name, static_cast<char*>(value), len);
  sqe->personality = personality;
  io_uring_sqe_set_data(sqe, &slot.disp);

  const int sub = submit_prepared(dpp);
  if (sub < 0) {
    return sub;
  }
  return wait_slot_blocking(slot);
}

int posix_uring_mkdirat(const DoutPrefixProvider* dpp, int dirfd,
                        const char* path, uint64_t mode,
                        uint16_t personality)
{
  if (!posix_uring_ensure_ring(dpp, /*nsfs=*/true)) {
    return -ENXIO;
  }

  IoSlot slot;
  slot.disp.fn = &IoSlot::handle_cqe;
  slot.disp.arg = &slot;

  io_uring_sqe* sqe = get_sqe_making_room(dpp);
  if (!sqe) {
    ldpp_dout(dpp, 0) << "ERROR: URING: no SQE for mkdirat" << dendl;
    return -EAGAIN;
  }
  io_uring_prep_mkdirat(sqe, dirfd, path, mode);
  sqe->personality = personality;
  io_uring_sqe_set_data(sqe, &slot.disp);

  const int sub = submit_prepared(dpp);
  if (sub < 0) {
    return sub;
  }
  return wait_slot_blocking(slot);
}


bool posix_uring_sqpoll()
{
  return thread_uring_state.initialized &&
    (thread_uring_state.ring.flags & IORING_SETUP_SQPOLL);
}

size_t posix_uring_chain_room(const DoutPrefixProvider* dpp)
{
  if (!posix_uring_ensure_ring(dpp, /*nsfs=*/true)) {
    return URING_CHAIN_MAX;	/* the chain will say there is no ring */
  }
  return std::clamp<size_t>(thread_uring_state.ring.sq.ring_entries / 2, 1,
                            URING_CHAIN_MAX);
}

static void prep_chain_op(io_uring_sqe* sqe, UringOp& op)
{
  switch (op.kind) {
  case UringOp::Kind::linkat:
    io_uring_prep_linkat(sqe, op.fd, op.path, op.fd2, op.path2, op.flags);
    break;
  case UringOp::Kind::renameat:
    io_uring_prep_renameat(sqe, op.fd, op.path, op.fd2, op.path2, op.flags);
    break;
  case UringOp::Kind::unlinkat:
    io_uring_prep_unlinkat(sqe, op.fd, op.path, op.flags);
    break;
  case UringOp::Kind::statx:
    io_uring_prep_statx(sqe, op.fd, op.path, op.flags, op.len,
                        static_cast<struct statx*>(op.buf));
    break;
  case UringOp::Kind::fsetxattr:
    io_uring_prep_fsetxattr(sqe, op.fd, op.path,
                            static_cast<const char*>(op.buf), op.flags,
                            op.len);
    break;
  case UringOp::Kind::fgetxattr:
    io_uring_prep_fgetxattr(sqe, op.fd, op.path, static_cast<char*>(op.buf),
                            op.len);
    break;
  }
}

int posix_uring_chain(const DoutPrefixProvider* dpp, UringOp* ops, size_t n,
                      uint16_t personality,
                      const std::function<void()>& submitted,
                      optional_yield y)
{
  if (n == 0) {
    submitted();
    return 0;
  }
  if (n > URING_CHAIN_MAX) {
    return -EINVAL;
  }
  if (!posix_uring_ensure_ring(dpp, /*nsfs=*/true)) {
    return -ENXIO;
  }
  io_uring* ring = &thread_uring_state.ring;

  /* the whole chain in the queue at once:  get_sqe_making_room() submits
   * to make room, which would send the first ops without the rest */
  if (io_uring_sq_space_left(ring) < n) {
    submit_prepared(dpp);
    if (io_uring_sq_space_left(ring) < n) {
      ldpp_dout(dpp, 0) << "ERROR: URING: no room for a chain of " << n
                        << dendl;
      return -EAGAIN;
    }
  }

  std::array<IoSlot, URING_CHAIN_MAX> slots;
  for (size_t i = 0; i < n; ++i) {
    io_uring_sqe* sqe = io_uring_get_sqe(ring);
    prep_chain_op(sqe, ops[i]);
    sqe->personality = personality;
    if (i + 1 < n) {
      sqe->flags |= IOSQE_IO_LINK;
    }
    slots[i].disp.fn = &IoSlot::handle_cqe;
    slots[i].disp.arg = &slots[i];
    io_uring_sqe_set_data(sqe, &slots[i].disp);
  }

  /* Until the kernel has taken every SQE.  It stops at an SQE it cannot
   * set up, completing that one with the error and leaving the rest
   * queued;  left there, another request's next submit on this thread
   * would send them, pointing at slots in a frame that may be gone.
   * Under SQPOLL the kernel thread takes them, and the wait below
   * returns only once it has. */
  if (posix_uring_sqpoll()) {
    const int r = io_uring_submit(ring);
    if (r < 0) {
      ldpp_dout(dpp, 0) << "ERROR: URING: io_uring_submit failed: "
                        << cpp_strerror(-r) << dendl;
    }
  } else {
    /* Until it does, however long:  a stalled request is safe, and
     * neither alternative is -- returning leaves the SQEs to be sent
     * into this frame once it is gone, and taking them back part way
     * runs half a chain.  A stall says why, every ten seconds. */
    auto backoff = std::chrono::milliseconds(1);
    const auto began = std::chrono::steady_clock::now();
    auto said = began;
    while (io_uring_sq_ready(ring) > 0) {
      const int r = io_uring_submit(ring);
      if (r >= 0) {
        continue;
      }
      const auto now = std::chrono::steady_clock::now();
      if (now - said >= std::chrono::seconds(10)) {
        said = now;
        ldpp_dout(dpp, 0) << "ERROR: URING: a chain of " << n
                          << " has not been submitted in "
                          << std::chrono::duration_cast<std::chrono::seconds>(
                               now - began).count()
                          << "s: " << cpp_strerror(-r) << ";  retrying"
                          << dendl;
      }
      std::this_thread::sleep_for(backoff);
      backoff = std::min(backoff * 2, std::chrono::milliseconds(100));
    }
  }
  submitted();

  const bool suspend = y &&
    !y.get_yield_context().get_cancellation_slot().is_connected();
  for (size_t i = 0; i < n; ++i) {
    ops[i].result = suspend ? wait_slot(dpp, slots[i], y, /*log_result=*/false)
                            : wait_slot_blocking(slots[i]);
  }
  return 0;
}

struct UringReadWindow::Impl {
  const DoutPrefixProvider* dpp;
  optional_yield y;
  int fd;
  unsigned qd;
  int64_t chunk_size;
  bool direct_io;
  bool nsfs;
  UringPersonality* personality;
  ResolveRead resolve;
  std::deque<std::unique_ptr<IoSlot>> inflight;
  unsigned pending_sqes = 0;

  Impl(const DoutPrefixProvider* dpp, optional_yield y, int fd, unsigned qd,
       int64_t chunk_size, bool direct_io, bool nsfs,
       UringPersonality* personality, ResolveRead resolve)
    : dpp(dpp), y(y), fd(fd),
      qd(std::clamp(qd, 1u, POSIX_URING_MAX_IODEPTH)),
      chunk_size(chunk_size > 0 ? chunk_size : 128 * 1024),
      direct_io(direct_io), nsfs(nsfs), personality(personality),
      resolve(std::move(resolve)) {}

  /* see pause_for_ring() */
  bool can_yield() const { return y && !personality; }

  int flush_submit() {
    unsigned spin = 0;
    auto backoff = std::chrono::milliseconds(1);
    while (pending_sqes > 0) {
      int r = submit_prepared(dpp);
      if (r > 0) {
        unsigned got = std::min(static_cast<unsigned>(r), pending_sqes);
        unsigned first = inflight.size() - pending_sqes;
        for (unsigned i = 0; i < got; ++i) {
          inflight[first + i]->submitted = true;
        }
        pending_sqes -= got;
        continue;
      }
      ++spin;
      ldpp_dout(dpp, 20) << "URING: flush_submit spin #" << spin
                        << " submit=" << r << " pending=" << pending_sqes
                        << " inflight=" << inflight.size()
                        << " fd=" << fd << dendl;
      if (!pause_for_ring(dpp, y, can_yield(), nsfs, backoff)) {
        return uring_err(dpp, -EIO, "flush_submit ring not ready");
      }
    }
    return 0;
  }

  /* `ring`:  this thread has one;  `pid`:  held by the caller until
   * it has flushed what this prepares.  With `resolve`, an object
   * offset is first mapped to a file's extent -- a multipart object's
   * part -- and `len` returns how much of it this extent covers. */
  int submit_one(int64_t ofs, int64_t& len, bool ring, uint16_t pid) {
    ReadExtent extent{fd, ofs, len, {}};
    if (resolve) {
      int r = resolve(ofs, len, extent);
      if (r < 0) {
        return r;
      }
      if (extent.len < 0 || extent.len > len || extent.ofs < 0 ||
          (extent.len > 0 && extent.fd < 0)) {
        return -EINVAL;
      }
      len = extent.len;
      if (len == 0) {
        return 0;
      }
    }
    ofs = extent.ofs;
    unsigned spin = 0;
    for (;;) {
      auto slot = std::make_unique<IoSlot>();
      slot->file_owner = extent.owner;
      slot->user_ofs = ofs;
      slot->user_len = len;
      if (direct_io) {
        slot->aligned_ofs = posix_align_down(ofs);
        int64_t front = ofs - slot->aligned_ofs;
        slot->aligned_len = posix_align_up(front + len);
      } else {
        slot->aligned_ofs = ofs;
        slot->aligned_len = len;
      }
      slot->buf = bufferptr(
          buffer::create_small_page_aligned(slot->aligned_len));
      int r = ring ? prep_rw(dpp, *slot, extent.fd, /*write=*/false, nsfs, pid)
		   : -ENXIO;
      if (r == -EAGAIN && (pending_sqes > 0 || can_yield())) {
        const bool had_pending = pending_sqes > 0;
        r = flush_submit();
        if (r < 0) {
          return uring_err(dpp, r, "submit_one flush_submit");
        }
        ++spin;
        ldpp_dout(dpp, 20) << "URING: submit_one spin #" << spin
                          << " get_sqe=-EAGAIN had_pending=" << had_pending
                          << " pending=" << pending_sqes
                          << " inflight=" << inflight.size()
                          << " fd=" << fd << " ofs=" << ofs
                          << " len=" << len << dendl;
        if (!had_pending) {
          yield_once(y);
          if (!posix_uring_ensure_ring(dpp, nsfs)) {
            return uring_err(dpp, -EIO, "submit_one ring not ready");
          }
        }
        continue;
      }
      if (r == -EAGAIN || r == -ENXIO) {
        /* A personality acquired for this ring stays pinned across fill(). */
        run_sync_as(dpp, personality, *slot, extent.fd, /*write=*/false,
                    ring ? null_yield : y);
        inflight.push_back(std::move(slot));
        return 0;
      }
      if (r < 0) {
        return uring_err(dpp, r, "submit_one prep_rw");
      }
      ++pending_sqes;
      inflight.push_back(std::move(slot));
      return 0;
    }
  }

  int wait_oldest(bufferlist& out) {
    int r = flush_submit();
    if (r < 0) {
      return uring_err(dpp, r, "read wait_oldest flush_submit");
    }
    if (inflight.empty()) {
      return 0;
    }
    auto& slot = *inflight.front();
    int res = wait_slot(dpp, slot, y);
    int64_t front = slot.user_ofs - slot.aligned_ofs;
    int64_t got = 0;
    if (res >= 0) {
      got = std::min<int64_t>(
          std::max<int64_t>(res - front, 0), slot.user_len);
      if (got > 0) {
        out.append(slot.buf, front, got);
      }
    }
    inflight.pop_front();
    if (res < 0) {
      return uring_err(dpp, res, "read wait_oldest");
    }
    return static_cast<int>(got);
  }

  int drain() {
    int first_err = 0;
    while (!inflight.empty()) {
      bufferlist discard;
      int r = wait_oldest(discard);
      if (r < 0 && first_err == 0) {
        first_err = r;
      }
    }
    if (first_err < 0) {
      return uring_err(dpp, first_err, "read drain");
    }
    return 0;
  }
};

UringReadWindow::UringReadWindow(const DoutPrefixProvider* dpp,
                                 optional_yield y, int fd, unsigned qd,
                                 int64_t chunk_size, bool direct_io,
                                 bool nsfs, UringPersonality* personality,
                                 ResolveRead resolve)
  : impl(std::make_unique<Impl>(dpp, y, fd, qd, chunk_size, direct_io, nsfs,
                                personality, std::move(resolve)))
{}

UringReadWindow::~UringReadWindow()
{
  if (impl) {
    try {
      impl->drain();
    } catch (...) {}
  }
}

int UringReadWindow::iterate(int64_t ofs, int64_t left,
                             fu2::unique_function<int(bufferlist&, int)> on_chunk)
{
  int64_t submit_ofs = ofs;
  int64_t submit_left = left;
  const DoutPrefixProvider* dpp = impl->dpp;

  /* One run of submissions, on whichever thread this is:  its ring,
   * and the personality acquired on it, held until the flush. */
  auto fill = [&]() -> int {
    const bool ring = posix_uring_ensure_ring(impl->dpp, impl->nsfs);
    HeldPersonality who(ring ? impl->personality : nullptr);
    if (int r = who.acquire(impl->dpp); r < 0) {
      return r;
    }
    while (submit_left > 0 && impl->inflight.size() < impl->qd) {
      int64_t n = std::min(submit_left, impl->chunk_size);
      int r = impl->submit_one(submit_ofs, n, ring, who.id());
      if (r < 0) {
        impl->flush_submit();
        return uring_err(dpp, r, "iterate submit_one");
      }
      if (n == 0) {
        submit_left = 0;
        break;
      }
      submit_ofs += n;
      submit_left -= n;
    }
    int r = impl->flush_submit();
    if (r < 0) {
      return uring_err(dpp, r, "iterate fill flush_submit");
    }
    return 0;
  };

  while (left > 0) {
    int r = fill();
    if (r < 0) {
      impl->drain();
      return uring_err(dpp, r, "iterate fill");
    }
    if (impl->inflight.empty()) {
      break;
    }
    bufferlist bl;
    int len = impl->wait_oldest(bl);
    if (len < 0) {
      impl->drain();
      return uring_err(dpp, len, "iterate wait_oldest");
    }
    if (len == 0) {
      impl->drain();
      break;
    }
    r = on_chunk(bl, len);
    if (r < 0) {
      impl->drain();
      return uring_err(dpp, r, "iterate handle_data");
    }
    left -= len;
  }
  int r = impl->drain();
  if (r < 0) {
    return uring_err(dpp, r, "iterate drain");
  }
  return 0;
}

struct UringWriteWindow::Impl {
  const DoutPrefixProvider* dpp;
  optional_yield y;
  int fd;
  unsigned qd;
  bool direct_io;
  bool nsfs;
  UringPersonality* personality;
  int64_t logical_end = 0;
  bool needs_truncate = false;
  std::deque<std::unique_ptr<IoSlot>> inflight;

  Impl(const DoutPrefixProvider* dpp, optional_yield y, int fd, unsigned qd,
       bool direct_io, bool nsfs, UringPersonality* personality)
    : dpp(dpp), y(y), fd(fd),
      qd(std::clamp(qd, 1u, POSIX_URING_MAX_IODEPTH)),
      direct_io(direct_io), nsfs(nsfs), personality(personality) {}

  /* see pause_for_ring() */
  bool can_yield() const { return y && !personality; }

  int wait_oldest() {
    if (inflight.empty()) {
      return 0;
    }
    auto& slot = *inflight.front();
    unsigned spin = 0;
    auto backoff = std::chrono::milliseconds(1);
    while (!slot.submitted && !slot.done) {
      int s = submit_prepared(dpp);
      if (s > 0) {
        slot.submitted = true;
        break;
      }
      ++spin;
      ldpp_dout(dpp, 20) << "URING: wait_oldest submit spin #" << spin
                        << " submit=" << s << " fd=" << fd
                        << " inflight=" << inflight.size() << dendl;
      if (!pause_for_ring(dpp, y, can_yield(), nsfs, backoff)) {
        return uring_err(dpp, -EIO, "write wait_oldest ring not ready");
      }
    }
    int res = wait_slot(dpp, slot, y);
    const bool is_write = slot.is_write;
    const int64_t aligned_len = slot.aligned_len;
    inflight.pop_front();
    if (res < 0) {
      return uring_err(dpp, res, "write wait_oldest");
    }
    if (is_write && static_cast<int64_t>(res) != aligned_len) {
      return uring_err(dpp, -EIO, "write wait_oldest short write");
    }
    return 0;
  }

  int drain_all() {
    int first_err = 0;
    while (!inflight.empty()) {
      int r = wait_oldest();
      if (r < 0 && first_err == 0) {
        first_err = r;
      }
    }
    if (first_err == 0 && needs_truncate) {
      if (::ftruncate(fd, logical_end) < 0) {
        first_err = uring_err(dpp, -errno, "drain_all ftruncate");
      }
      needs_truncate = false;
    }
    if (first_err < 0) {
      return uring_err(dpp, first_err, "drain_all");
    }
    return 0;
  }

  int submit_write(bufferlist&& data, uint64_t offset) {
    int64_t len = data.length();
    int64_t ofs = static_cast<int64_t>(offset);
    auto slot = std::make_unique<IoSlot>();
    slot->is_write = true;
    slot->user_ofs = ofs;
    slot->user_len = len;
    slot->hold = std::move(data);

    if (direct_io) {
      if ((ofs % POSIX_DIRECT_ALIGN) == 0 &&
          (len % POSIX_DIRECT_ALIGN) == 0) {
        slot->hold.rebuild_aligned_size_and_memory(POSIX_DIRECT_ALIGN,
                                                   POSIX_DIRECT_ALIGN);
        slot->aligned_ofs = ofs;
        slot->aligned_len = len;
      } else if ((ofs % POSIX_DIRECT_ALIGN) == 0) {
        slot->aligned_ofs = ofs;
        slot->aligned_len = posix_align_up(len);
        needs_truncate = true;
      } else {
        /* Unaligned offset: drain then RMW with blocking pwrite. */
        int r = drain_all();
        if (r < 0) {
          return uring_err(dpp, r, "submit_write drain_all");
        }
        int wr = posix_direct_write(fd, ofs, slot->hold, dpp);
        if (wr < 0) {
          return uring_err(dpp, wr, "submit_write posix_direct_write");
        }
        logical_end = std::max(logical_end, ofs + len);
        return 0;
      }
    } else {
      slot->hold.rebuild();
      slot->aligned_ofs = ofs;
      slot->aligned_len = len;
    }

    if (direct_io && slot->aligned_len > len) {
      slot->buf = bufferptr(
          buffer::create_small_page_aligned(slot->aligned_len));
      memcpy(slot->buf.c_str(), slot->hold.c_str(), len);
      memset(slot->buf.c_str() + len, 0, slot->aligned_len - len);
    }

    logical_end = std::max(logical_end, ofs + len);

    /* this thread's ring, and the personality acquired on it, held
     * until the submit returns */
    const bool ring = posix_uring_ensure_ring(dpp, nsfs);
    HeldPersonality who(ring ? personality : nullptr);
    int r = who.acquire(dpp);
    if (r < 0) {
      return r;
    }
    unsigned prep_spin = 0;
    for (;;) {
      r = ring ? prep_rw(dpp, *slot, fd, /*write=*/true, nsfs, who.id())
	       : -ENXIO;
      if (r == -EAGAIN && can_yield()) {
        ++prep_spin;
        ldpp_dout(dpp, 20) << "URING: submit_write prep spin #" << prep_spin
                          << " get_sqe=-EAGAIN fd=" << fd
                          << " ofs=" << ofs << " len=" << len
                          << " inflight=" << inflight.size() << dendl;
        yield_once(y);
        if (!posix_uring_ensure_ring(dpp, nsfs)) {
          return uring_err(dpp, -EIO, "submit_write prep ring not ready");
        }
        continue;
      }
      break;
    }
    if (r == -EAGAIN || r == -ENXIO) {
      /* Keep any held personality on this worker until release. */
      run_sync_as(dpp, personality, *slot, fd, /*write=*/true,
                  ring ? null_yield : y);
      inflight.push_back(std::move(slot));
      return 0;
    }
    if (r < 0) {
      return uring_err(dpp, r, "submit_write prep_rw");
    }
    inflight.push_back(std::move(slot));
    unsigned submit_spin = 0;
    auto backoff = std::chrono::milliseconds(1);
    for (;;) {
      int submitted = submit_prepared(dpp);
      if (submitted > 0) {
        inflight.back()->submitted = true;
        return 0;
      }
      if (submitted < 0 && submitted != -EBUSY && submitted != -EAGAIN) {
        return uring_err(dpp, submitted, "submit_write io_uring_submit");
      }
      ++submit_spin;
      ldpp_dout(dpp, 20) << "URING: submit_write submit spin #" << submit_spin
                        << " submitted=" << submitted
                        << " fd=" << fd << " ofs=" << ofs << " len=" << len
                        << " inflight=" << inflight.size() << dendl;
      if (!pause_for_ring(dpp, y, can_yield(), nsfs, backoff)) {
        return uring_err(dpp, -EIO, "submit_write submit ring not ready");
      }
    }
  }
};

UringWriteWindow::UringWriteWindow(const DoutPrefixProvider* dpp,
                                   optional_yield y, int fd, unsigned qd,
                                   bool direct_io, bool nsfs,
                                   UringPersonality* personality)
  : impl(std::make_unique<Impl>(dpp, y, fd, qd, direct_io, nsfs,
                                personality))
{}

UringWriteWindow::~UringWriteWindow()
{
  if (impl) {
    try {
      impl->drain_all();
    } catch (...) {}
  }
}

int UringWriteWindow::process(bufferlist&& data, uint64_t offset)
{
  if (data.length() == 0) {
    int r = impl->drain_all();
    if (r < 0) {
      return uring_err(impl->dpp, r, "process drain");
    }
    return 0;
  }
  /* no fchmod here:  files are created 0600 (and, impersonated, as
   * their identity), so there is nothing to narrow */
  while (impl->inflight.size() >= impl->qd) {
    int r = impl->wait_oldest();
    if (r < 0) {
      return uring_err(impl->dpp, r, "process wait_oldest");
    }
  }
  int r = impl->submit_write(std::move(data), offset);
  if (r < 0) {
    return uring_err(impl->dpp, r, "process submit_write");
  }
  return 0;
}

int UringWriteWindow::drain()
{
  int r = impl->drain_all();
  if (r < 0) {
    return uring_err(impl->dpp, r, "drain");
  }
  return 0;
}

#else /* !HAVE_LIBURING */

bool posix_uring_ensure_ring(const DoutPrefixProvider* dpp, bool nsfs)
{
  (void)dpp;
  (void)nsfs;
  return false;
}

bool posix_uring_ensure_ring(const DoutPrefixProvider* dpp)
{
  (void)dpp;
  return false;
}

struct UringReadWindow::Impl {};
struct UringWriteWindow::Impl {};

int posix_uring_register_personality(const DoutPrefixProvider* dpp)
{
  (void)dpp;
  return -EOPNOTSUPP;
}

int posix_uring_openat2(const DoutPrefixProvider* dpp, int dirfd,
                        const char* path, uint64_t flags, uint64_t mode,
                        uint16_t personality, uint64_t resolve)
{
  (void)dpp; (void)dirfd; (void)path; (void)flags; (void)mode;
  (void)personality; (void)resolve;
  return -EOPNOTSUPP;
}

int posix_uring_mkdirat(const DoutPrefixProvider* dpp, int dirfd,
                        const char* path, uint64_t mode,
                        uint16_t personality)
{
  (void)dpp; (void)dirfd; (void)path; (void)mode; (void)personality;
  return -EOPNOTSUPP;
}

int posix_uring_fsetxattr(const DoutPrefixProvider* dpp, int fd,
                          const char* name, const void* value, size_t len,
                          int flags, uint16_t personality)
{
  (void)dpp; (void)fd; (void)name; (void)value; (void)len; (void)flags;
  (void)personality;
  return -EOPNOTSUPP;
}

int posix_uring_fgetxattr(const DoutPrefixProvider* dpp, int fd,
                          const char* name, void* value, size_t len,
                          uint16_t personality)
{
  (void)dpp; (void)fd; (void)name; (void)value; (void)len;
  (void)personality;
  return -EOPNOTSUPP;
}


bool posix_uring_sqpoll()
{
  return false;
}

size_t posix_uring_chain_room(const DoutPrefixProvider* dpp)
{
  (void)dpp;
  return URING_CHAIN_MAX;
}

int posix_uring_chain(const DoutPrefixProvider* dpp, UringOp* ops, size_t n,
                      uint16_t personality,
                      const std::function<void()>& submitted,
                      optional_yield y)
{
  (void)dpp; (void)ops; (void)n; (void)personality; (void)submitted;
  (void)y;
  return -ENXIO;
}

void posix_uring_unregister_personality(const DoutPrefixProvider* dpp,
                                        uint16_t id)
{
  (void)dpp; (void)id;
}

UringReadWindow::UringReadWindow(const DoutPrefixProvider* dpp,
                                 optional_yield y, int fd, unsigned qd,
                                 int64_t chunk_size, bool direct_io,
                                 bool nsfs, UringPersonality* personality,
                                 ResolveRead resolve)
  : impl(std::make_unique<Impl>())
{
  (void)dpp; (void)y; (void)fd; (void)qd; (void)chunk_size; (void)direct_io;
  (void)nsfs; (void)personality; (void)resolve;
}

UringReadWindow::~UringReadWindow() = default;

int UringReadWindow::iterate(int64_t ofs, int64_t left,
                             fu2::unique_function<int(bufferlist&, int)> on_chunk)
{
  (void)ofs; (void)left; (void)on_chunk;
  return -EOPNOTSUPP;
}

UringWriteWindow::UringWriteWindow(const DoutPrefixProvider* dpp,
                                   optional_yield y, int fd, unsigned qd,
                                   bool direct_io, bool nsfs,
                                   UringPersonality* personality)
  : impl(std::make_unique<Impl>())
{
  (void)dpp; (void)y; (void)fd; (void)qd; (void)direct_io;
  (void)nsfs; (void)personality;
}

UringWriteWindow::~UringWriteWindow() = default;

int UringWriteWindow::process(bufferlist&& data, uint64_t offset)
{
  (void)data; (void)offset;
  return -EOPNOTSUPP;
}

int UringWriteWindow::drain()
{
  return -EOPNOTSUPP;
}

#endif /* HAVE_LIBURING */

} // namespace rgw::sal
