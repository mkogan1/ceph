// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RBD_MIRROR_MIRROR_STATUS_UPDATER_H
#define CEPH_RBD_MIRROR_MIRROR_STATUS_UPDATER_H

#include "include/rados/librados.hpp"
#include "common/ceph_mutex.h"
#include "cls/rbd/cls_rbd_types.h"
#include <list>
#include <map>
#include <set>
#include <string>

struct Context;
namespace librbd { class ImageCtx; }

namespace rbd {
namespace mirror {

template <typename> struct Threads;

template <typename ImageCtxT = librbd::ImageCtx>
class MirrorStatusUpdater {
public:

  static MirrorStatusUpdater* create(librados::IoCtx& io_ctx,
                                     Threads<ImageCtxT> *threads) {
    return new MirrorStatusUpdater(io_ctx, threads);
  }

  MirrorStatusUpdater(librados::IoCtx& io_ctx, Threads<ImageCtxT> *threads);
  ~MirrorStatusUpdater();

  void init(Context* on_finish);
  void shut_down(Context* on_finish);

  bool exists(const std::string& global_image_id);
  void set_mirror_image_status(
      const std::string& global_image_id,
      const cls::rbd::MirrorImageSiteStatus& mirror_image_site_status,
      bool immediate_update);
  void remove_mirror_image_status(const std::string& global_image_id,
                                  Context* on_finish);

private:
  typedef std::list<Context*> Contexts;
  typedef std::set<std::string> GlobalImageIds;
  typedef std::map<std::string, cls::rbd::MirrorImageSiteStatus>
      GlobalImageStatus;

  librados::IoCtx m_io_ctx;
  Threads<ImageCtxT>* m_threads;

  Context* m_timer_task = nullptr;

  ceph::mutex m_lock;

  bool m_initialized = false;

  GlobalImageIds m_update_global_image_ids;
  GlobalImageStatus m_global_image_status;

  bool m_update_in_progress = false;
  bool m_update_in_flight = false;
  bool m_update_requested = false;
  Contexts m_update_on_finish_ctxs;
  GlobalImageIds m_updating_global_image_ids;

  bool try_remove_mirror_image_status(const std::string& global_image_id,
                                      Context* on_finish);

  void schedule_timer_task();
  void handle_timer_task(int r);

  void queue_update_task(std::unique_lock<ceph::mutex>&& locker);
  void update_task(int r);
  void handle_update_task(int r);

};

} // namespace mirror
} // namespace rbd

extern template class rbd::mirror::MirrorStatusUpdater<librbd::ImageCtx>;

#endif // CEPH_RBD_MIRROR_MIRROR_STATUS_UPDATER_H
