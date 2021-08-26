// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include <errno.h>

#include "common/errno.h"

#include "rgw_tools.h"

#define dout_subsys ceph_subsys_rgw
#define dout_context g_ceph_context

#define READ_CHUNK_LEN (512 * 1024)

using namespace std;

static std::map<std::string, std::string>* ext_mime_map;

void parse_mime_map_line(const char *start, const char *end)
{
  char line[end - start + 1];
  strncpy(line, start, end - start);
  line[end - start] = '\0';
  char *l = line;
#define DELIMS " \t\n\r"

  while (isspace(*l))
    l++;

  char *mime = strsep(&l, DELIMS);
  if (!mime)
    return;

  char *ext;
  do {
    ext = strsep(&l, DELIMS);
    if (ext && *ext) {
      (*ext_mime_map)[ext] = mime;
    }
  } while (ext);
}


void parse_mime_map(const char *buf)
{
  const char *start = buf, *end = buf;
  while (*end) {
    while (*end && *end != '\n') {
      end++;
    }
    parse_mime_map_line(start, end);
    end++;
    start = end;
  }
}

static int ext_mime_map_init(const DoutPrefixProvider *dpp, CephContext *cct, const char *ext_map)
{
  int fd = open(ext_map, O_RDONLY);
  char *buf = NULL;
  int ret;
  if (fd < 0) {
    ret = -errno;
    ldpp_dout(dpp, 0) << __func__ << " failed to open file=" << ext_map
                  << " : " << cpp_strerror(-ret) << dendl;
    return ret;
  }

  struct stat st;
  ret = fstat(fd, &st);
  if (ret < 0) {
    ret = -errno;
    ldpp_dout(dpp, 0) << __func__ << " failed to stat file=" << ext_map
                  << " : " << cpp_strerror(-ret) << dendl;
    goto done;
  }

  buf = (char *)malloc(st.st_size + 1);
  if (!buf) {
    ret = -ENOMEM;
    ldpp_dout(dpp, 0) << __func__ << " failed to allocate buf" << dendl;
    goto done;
  }

  ret = safe_read(fd, buf, st.st_size + 1);
  if (ret != st.st_size) {
    // huh? file size has changed?
    ldpp_dout(dpp, 0) << __func__ << " raced! will retry.." << dendl;
    free(buf);
    close(fd);
    return ext_mime_map_init(dpp, cct, ext_map);
  }
  buf[st.st_size] = '\0';

  parse_mime_map(buf);
  ret = 0;
done:
  free(buf);
  close(fd);
  return ret;
}

const char *rgw_find_mime_by_ext(string& ext)
{
  map<string, string>::iterator iter = ext_mime_map->find(ext);
  if (iter == ext_mime_map->end())
    return NULL;

  return iter->second.c_str();
}

void rgw_fix_etag(CephContext *cct, map<std::string, bufferlist> *attrset)
{
  if (!attrset)
    return;
  map<string, bufferlist>::iterator iter;
  iter = attrset->find(RGW_ATTR_ETAG);
  if (iter == attrset->end())
    return;
  rgw_fix_etag(cct, iter->second);
}

int rgw_tools_init(const DoutPrefixProvider *dpp, CephContext *cct)
{
  ext_mime_map = new std::map<std::string, std::string>;
  ext_mime_map_init(dpp, cct, cct->_conf->rgw_mime_types_file.c_str());
  // ignore errors; missing mime.types is not fatal
  return 0;
}

void rgw_tools_cleanup()
{
  delete ext_mime_map;
  ext_mime_map = nullptr;
}

void rgw_fix_etag(CephContext *cct, bufferlist& etagbl)
{
  if (etagbl.length() <= CEPH_CRYPTO_MD5_DIGESTSIZE*2) {
    return;
  }
  if (etagbl[CEPH_CRYPTO_MD5_DIGESTSIZE*2] == '-' &&
	std::isdigit(etagbl[CEPH_CRYPTO_MD5_DIGESTSIZE*2+1])) {
    return;
  }
  std::string etag = etagbl.to_str();
  if (etagbl[CEPH_CRYPTO_MD5_DIGESTSIZE*2]) {
    ldout(cct, 2) << "trimming junk from etag <" << etag << ">" << dendl;
  }
  etagbl.clear();
  etagbl.append(etag.c_str(), CEPH_CRYPTO_MD5_DIGESTSIZE*2);
  return;
}

void rgw_fix_etag(CephContext *cct, string& etag)
{
  if (etag.length() <= CEPH_CRYPTO_MD5_DIGESTSIZE*2) {
    return;
  }
  if (etag[CEPH_CRYPTO_MD5_DIGESTSIZE*2] == '-' &&
	std::isdigit(etag[CEPH_CRYPTO_MD5_DIGESTSIZE*2+1])) {
    return;
  }
  if (etag[CEPH_CRYPTO_MD5_DIGESTSIZE*2]) {
    ldout(cct, 2) << "trimming junk from etag <" << etag << ">" << dendl;
  }
  etag.resize(CEPH_CRYPTO_MD5_DIGESTSIZE*2);
  return;
}
