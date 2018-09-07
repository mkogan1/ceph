/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2018 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#ifndef CEPH_SOCK_COMPAT_H
#define CEPH_SOCK_COMPAT_H

#include "include/compat.h"

#include <errno.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <unistd.h>

/*
 * This optimization may not be available on all platforms (e.g. OSX).
 * Apparently a similar approach based on TCP_CORK can be used.
 */
#ifndef MSG_MORE
# define MSG_MORE 0
#endif

/*
 * On BSD SO_NOSIGPIPE can be set via setsockopt to block SIGPIPE.
 */
#ifndef MSG_NOSIGNAL
# define MSG_NOSIGNAL 0
# ifdef SO_NOSIGPIPE
#  define CEPH_USE_SO_NOSIGPIPE
# else
#  define CEPH_USE_SIGPIPE_BLOCKER
#  warning "Using SIGPIPE blocking instead of suppression; this is not well-tested upstream!"
# endif
#endif

inline int socket_cloexec(int domain, int type, int protocol)
{
#ifdef SOCK_CLOEXEC
  return socket(domain, type|SOCK_CLOEXEC, protocol);
#else
  int fd = socket(domain, type, protocol);
  if (fd == -1)
    return -1;

  if (fcntl(fd, F_SETFD, FD_CLOEXEC) < 0)
    goto fail;

  return fd;
fail:
  int save_errno = errno;
  VOID_TEMP_FAILURE_RETRY(close(fd));
  return (errno = save_errno, -1);
#endif
}

inline int socketpair_cloexec(int domain, int type, int protocol, int sv[2])
{
#ifdef SOCK_CLOEXEC
  return socketpair(domain, type|SOCK_CLOEXEC, protocol, sv);
#else
  int rc = socketpair(domain, type, protocol, sv);
  if (rc == -1)
    return -1;

  if (fcntl(sv[0], F_SETFD, FD_CLOEXEC) < 0)
    goto fail;

  if (fcntl(sv[1], F_SETFD, FD_CLOEXEC) < 0)
    goto fail;

  return 0;
fail:
  int save_errno = errno;
  VOID_TEMP_FAILURE_RETRY(close(sv[0]));
  VOID_TEMP_FAILURE_RETRY(close(sv[1]));
  return (errno = save_errno, -1);
#endif
}

inline int accept_cloexec(int sockfd, struct sockaddr* addr, socklen_t* addrlen)
{
#ifdef HAVE_ACCEPT4
  return accept4(sockfd, addr, addrlen, SOCK_CLOEXEC);
#else
  int fd = accept(sockfd, addr, addrlen);
  if (fd == -1)
    return -1;

  if (fcntl(fd, F_SETFD, FD_CLOEXEC) < 0)
    goto fail;

  return fd;
fail:
  int save_errno = errno;
  VOID_TEMP_FAILURE_RETRY(close(fd));
  return (errno = save_errno, -1);
#endif
}


#endif
