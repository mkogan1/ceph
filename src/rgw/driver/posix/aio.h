// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#pragma once

#include <boost/asio/random_access_file.hpp>
#include "rgw_aio.h"

namespace rgw {

Aio::OpFunc file_read_op(boost::asio::random_access_file& file,
                         uint64_t offset, uint64_t len);

// shared_ptr overload: handler holds the file open until IO completes
Aio::OpFunc file_read_op(std::shared_ptr<boost::asio::random_access_file> file,
                         uint64_t offset, uint64_t len);

Aio::OpFunc file_write_op(boost::asio::random_access_file& file,
                          uint64_t offset, bufferlist bl);

// shared_ptr overload with post-completion callback: handler holds the file
// open until IO completes, then calls on_complete before signaling the throttle.
// on_complete receives the error_code so it can skip work on failure.
using WriteCompleteFunc = fu2::unique_function<void(boost::system::error_code ec)>;
Aio::OpFunc file_write_op(std::shared_ptr<boost::asio::random_access_file> file,
                          uint64_t offset, bufferlist bl,
                          WriteCompleteFunc on_complete);

} // namespace rgw
