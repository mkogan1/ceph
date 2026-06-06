// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "rgw_mime.h"
#include "driver/rados/rgw_tools.h"

int rgw_tools_init(const DoutPrefixProvider *dpp, CephContext *cct)
{
  rgw_mime_init(dpp, cct);
  return 0;
}

void rgw_tools_cleanup()
{
  rgw_mime_cleanup();
}
