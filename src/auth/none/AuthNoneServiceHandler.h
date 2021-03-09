// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2009 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#ifndef CEPH_AUTHNONESERVICEHANDLER_H
#define CEPH_AUTHNONESERVICEHANDLER_H

#include "auth/AuthServiceHandler.h"
#include "auth/Auth.h"

class CephContext;

class AuthNoneServiceHandler  : public AuthServiceHandler {
public:
  explicit AuthNoneServiceHandler(CephContext *cct_)
    : AuthServiceHandler(cct_) {}
  ~AuthNoneServiceHandler() override {}
  
  int handle_request(bufferlist::iterator& indata, bufferlist& result_bl, uint64_t& global_id, AuthCapsInfo& caps, uint64_t *auid = NULL) override {
    return 0;
  }

private:
  int do_start_session(bool is_new_global_id,
                       bufferlist& result_bl,
                       AuthCapsInfo& caps) override {
    caps.allow_all = true;
    return CEPH_AUTH_NONE;
  }
};

#endif
