/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

#include "io_util.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <fmt/format.h>
#include <ifaddrs.h>
#include <netdb.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/types.h>

#include "fmt/ostream.h"
#include "server/tls_util.h"

#ifdef __linux__
#include <sys/sendfile.h>
#endif

#ifdef ENABLE_OPENSSL
#include <openssl/ssl.h>

#include "event2/bufferevent_ssl.h"
#endif

#include "event_util.h"
#include "scope_exit.h"
#include "unique_fd.h"

#ifndef POLLIN
#define POLLIN 0x0001   /* There is data to read */
#define POLLPRI 0x0002  /* There is urgent data to read */
#define POLLOUT 0x0004  /* Writing now will not block */
#define POLLERR 0x0008  /* Error condition */
#define POLLHUP 0x0010  /* Hung up */
#define POLLNVAL 0x0020 /* Invalid request: fd not open */
#endif

#define AE_READABLE 1  // NOLINT
#define AE_WRITABLE 2  // NOLINT
#define AE_ERROR 4     // NOLINT
#define AE_HUP 8       // NOLINT

namespace util {

Status SockSetTcpNoDelay(int fd, int val) {
  if (setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &val, sizeof(val)) == -1) {
    return Status::FromErrno();
  }
  return Status::OK();
}

Status SockSetTcpKeepalive(int fd, int interval) {
  int val = 1;
  if (setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, &val, sizeof(val)) == -1) {
    return Status::FromErrno();
  }

#ifdef __linux__
  // Default settings are more or less garbage, with the keepalive time
  // set to 7200 by default on Linux. Modify settings to make the feature
  // actually useful.

  // Send first probe after interval.
  val = interval;
  if (setsockopt(fd, IPPROTO_TCP, TCP_KEEPIDLE, &val, sizeof(val)) < 0) {
    return Status::FromErrno("setsockopt TCP_KEEPIDLE");
  }

  // Send next probes after the specified interval. Note that we set the
  // delay as interval / 3, as we send three probes before detecting
  // an error (see the next setsockopt call).
  val = interval / 3;
  if (val == 0) val = 1;
  if (setsockopt(fd, IPPROTO_TCP, TCP_KEEPINTVL, &val, sizeof(val)) < 0) {
    return Status::FromErrno("setsockopt TCP_KEEPINTVL");
  }

  // Consider the socket in error state after three we send three ACK
  // probes without getting a reply.
  val = 3;
  if (setsockopt(fd, IPPROTO_TCP, TCP_KEEPCNT, &val, sizeof(val)) < 0) {
    return Status::FromErrno("setsockopt TCP_KEEPCNT");
  }
#else
  ((void)interval);  // Avoid unused var warning for non Linux systems.
#endif

  return Status::OK();
}

// Lookup IP addresses by hostname
StatusOr<std::vector<std::string>> LookupHostByName(const std::string &host) {
  addrinfo hints = {}, *servinfo = nullptr;

  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;

  if (int rv = getaddrinfo(host.c_str(), nullptr, &hints, &servinfo); rv != 0) {
    return {Status::NotOK, gai_strerror(rv)};
  }

  auto exit = MakeScopeExit([servinfo] { freeaddrinfo(servinfo); });

  std::vector<std::string> ips;
  for (auto p = servinfo; p != nullptr; p = p->ai_next) {
    char ip[INET6_ADDRSTRLEN] = {};
    if (p->ai_family == AF_INET) {
      inet_ntop(p->ai_family, &((sockaddr_in *)p->ai_addr)->sin_addr, ip, sizeof(ip));
    } else {
      inet_ntop(p->ai_family, &((sockaddr_in6 *)p->ai_addr)->sin6_addr, ip, sizeof(ip));
    }
    ips.emplace_back(ip);
  }

  return ips;
}

StatusOr<int> SockConnect(const std::string &host, uint32_t port, int conn_timeout, int timeout) {
  addrinfo hints = {}, *servinfo = nullptr;

  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;

  if (int rv = getaddrinfo(host.c_str(), std::to_string(port).c_str(), &hints, &servinfo); rv != 0) {
    return {Status::NotOK, gai_strerror(rv)};
  }

  auto exit = MakeScopeExit([servinfo] { freeaddrinfo(servinfo); });

  for (auto p = servinfo; p != nullptr; p = p->ai_next) {
    auto cfd = UniqueFD(socket(p->ai_family, p->ai_socktype, p->ai_protocol));
    if (!cfd) continue;

    if (conn_timeout == 0) {
      if (connect(*cfd, p->ai_addr, p->ai_addrlen) == -1) {
        continue;
      }
    } else {
      fcntl(*cfd, F_SETFL, O_NONBLOCK);
      int ret = connect(*cfd, p->ai_addr, p->ai_addrlen);
      if (ret != 0 && errno != EINPROGRESS) {
        continue;
      }

      auto retmask = util::AeWait(*cfd, AE_WRITABLE, conn_timeout);
      if ((retmask & AE_WRITABLE) == 0 || (retmask & AE_ERROR) != 0 || (retmask & AE_HUP) != 0) {
        return Status::FromErrno();
      }

      // restore to the block mode
      int socket_arg = 0;
      if (socket_arg = fcntl(*cfd, F_GETFL, NULL); socket_arg < 0) {
        return Status::FromErrno();
      }

      socket_arg &= (~O_NONBLOCK);
      if (fcntl(*cfd, F_SETFL, socket_arg) < 0) {
        return Status::FromErrno();
      }
    }

    Status s = SockSetTcpKeepalive(*cfd, 120);
    if (s.IsOK()) {
      s = SockSetTcpNoDelay(*cfd, 1);
    }
    if (!s.IsOK()) {
      continue;
    }

    if (timeout > 0) {
      timeval tv;
      tv.tv_sec = timeout / 1000;
      tv.tv_usec = (timeout % 1000) * 1000;
      if (setsockopt(*cfd, SOL_SOCKET, SO_RCVTIMEO, reinterpret_cast<char *>(&tv), sizeof(tv)) < 0) {
        return Status::FromErrno("setsockopt failed");
      }
    }
    return cfd.Release();
  }
  return Status::FromErrno();
}

// NOTE: fd should be blocking here
Status SockSend(int fd, const std::string &data) { return Write(fd, data); }

// Implements SockSendFileImpl to transfer data between file descriptors and
// avoid transferring data to and from user space.
//
// The function prototype is just like sendfile(2) on Linux. in_fd is a file
// descriptor opened for reading and out_fd is a descriptor opened for writing.
// offset specifies where to start reading data from in_fd. count is the number
// of bytes to copy between the file descriptors.
//
// The return value is the number of bytes written to out_fd, if the transfer
// was successful. On error, -1 is returned, and errno is set appropriately.
ssize_t SendFileImpl(int out_fd, int in_fd, off_t offset, size_t count) {
#if defined(__linux__)
  return sendfile(out_fd, in_fd, &offset, count);

#elif defined(__APPLE__)
  off_t len = count;
  if (sendfile(in_fd, out_fd, offset, &len, NULL, 0) == -1)
    return -1;
  else
    return (ssize_t)len;

#else
  errno = ENOSYS;
  return -1;

#endif
}

#ifdef ENABLE_OPENSSL
ssize_t SendFileSSLImpl(ssl_st *ssl, int in_fd, off_t offset, size_t count) {
  constexpr size_t BUFFER_SIZE = 16 * 1024;
  char buf[BUFFER_SIZE];
  if (off_t ret = lseek(in_fd, offset, SEEK_SET); ret == -1) {
    return -1;
  }
  count = count <= BUFFER_SIZE ? count : BUFFER_SIZE;
  if (ssize_t ret = read(in_fd, buf, count); ret == -1) {
    return -1;
  } else {
    count = ret;
  }
  return SSL_write(ssl, buf, (int)count);
}
#endif

template <auto F, typename FD, typename... Args>
Status SockSendFileImpl(FD out_fd, int in_fd, size_t size, Args... args) {
  constexpr size_t BUFFER_SIZE = 16 * 1024;
  off_t offset = 0;
  while (size != 0) {
    size_t n = size <= BUFFER_SIZE ? size : BUFFER_SIZE;
    ssize_t nwritten = F(out_fd, in_fd, offset, n, args...);
    if (nwritten == -1) {
      if (errno == EINTR)
        continue;
      else
        return Status::FromErrno();
    }
    size -= nwritten;
    offset += nwritten;
  }
  return Status::OK();
}

// Send file by sendfile actually according to different operation systems,
// please note that, the out socket fd should be in blocking mode.
Status SockSendFile(int out_fd, int in_fd, size_t size) { return SockSendFileImpl<SendFileImpl>(out_fd, in_fd, size); }

Status SockSendFile(int out_fd, int in_fd, size_t size, [[maybe_unused]] ssl_st *ssl) {
#ifdef ENABLE_OPENSSL
  if (ssl) {
    // NOTE: SockSendFileImpl<SSL_sendfile> will cause errors, refer to #2756
    return SockSendFileImpl<SendFileSSLImpl>(ssl, in_fd, size);
  }
#endif
  return SockSendFile(out_fd, in_fd, size);
}

Status SockSendFile(int out_fd, int in_fd, size_t size, [[maybe_unused]] bufferevent *bev) {
#ifdef ENABLE_OPENSSL
  return SockSendFile(out_fd, in_fd, size, bufferevent_openssl_get_ssl(bev));
#else
  return SockSendFile(out_fd, in_fd, size);
#endif
}

Status SockSetBlocking(int fd, int blocking) {
  int flags = 0;
  // Old flags
  if (flags = fcntl(fd, F_GETFL); flags == -1) {
    return Status::FromErrno("fcntl(F_GETFL)");
  }

  // New flags
  if (blocking)
    flags &= ~O_NONBLOCK;
  else
    flags |= O_NONBLOCK;

  if (fcntl(fd, F_SETFL, flags) == -1) {
    return Status::FromErrno("fcntl(F_SETFL,O_BLOCK)");
  }
  return Status::OK();
}

StatusOr<std::string> SockReadLine(int fd) {
  UniqueEvbuf evbuf;
  if (evbuffer_read(evbuf.get(), fd, -1) <= 0) {
    return Status::FromErrno("read response err");
  }

  UniqueEvbufReadln line(evbuf.get(), EVBUFFER_EOL_CRLF_STRICT);
  if (!line) {
    return Status::FromErrno("read response err(empty)");
  }

  return std::string(line.get(), line.length);
}

StatusOr<std::tuple<std::string, uint32_t>> GetPeerAddr(int fd) {
  sockaddr_storage sa{};
  socklen_t sa_len = sizeof(sa);
  if (getpeername(fd, reinterpret_cast<sockaddr *>(&sa), &sa_len) < 0) {
    return Status::FromErrno("Failed to get peer name");
  }

  if (sa.ss_family == AF_INET6) {
    char buf[INET6_ADDRSTRLEN];
    auto sa6 = reinterpret_cast<sockaddr_in6 *>(&sa);
    inet_ntop(AF_INET6, reinterpret_cast<void *>(&sa6->sin6_addr), buf, INET_ADDRSTRLEN);
    return {buf, ntohs(sa6->sin6_port)};
  } else if (sa.ss_family == AF_INET) {
    auto sa4 = reinterpret_cast<sockaddr_in *>(&sa);
    char buf[INET_ADDRSTRLEN];
    inet_ntop(AF_INET, reinterpret_cast<void *>(&sa4->sin_addr), buf, INET_ADDRSTRLEN);
    return {buf, ntohs(sa4->sin_port)};
  }

  return {Status::NotOK, "Failed to get peer name due to invalid family type"};
}

int GetLocalPort(int fd) {
  sockaddr_in6 address;
  socklen_t len = sizeof(address);
  if (getsockname(fd, (sockaddr *)&address, &len) == -1) {
    return 0;
  }

  if (address.sin6_family == AF_INET) {
    return ntohs(reinterpret_cast<sockaddr_in *>(&address)->sin_port);
  } else if (address.sin6_family == AF_INET6) {
    return ntohs(address.sin6_port);
  }

  return 0;
}

bool IsPortInUse(uint32_t port) {
  auto s = SockConnect("0.0.0.0", port);
  if (s) close(*s);
  return s.IsOK();
}

/* Wait for milliseconds until the given file descriptor becomes
 * writable/readable/exception */
int AeWait(int fd, int mask, int timeout) {
  pollfd pfd;
  int retmask = 0;

  memset(&pfd, 0, sizeof(pfd));
  pfd.fd = fd;
  if (mask & AE_READABLE) pfd.events |= POLLIN;
  if (mask & AE_WRITABLE) pfd.events |= POLLOUT;

  if (int retval = poll(&pfd, 1, timeout); retval == 1) {
    if (pfd.revents & POLLIN) retmask |= AE_READABLE;
    if (pfd.revents & POLLOUT) retmask |= AE_WRITABLE;
    if (pfd.revents & POLLERR) retmask |= AE_ERROR;
    if (pfd.revents & POLLHUP) retmask |= AE_HUP;
    return retmask;
  } else {
    return retval;
  }
}

bool MatchListeningIP(std::vector<std::string> &binds, const std::string &ip) {
  if (std::find(binds.begin(), binds.end(), ip) != binds.end()) {
    return true;
  }

  // If binds contains 0.0.0.0, we should resolve ip addresses and check it
  if (std::find(binds.begin(), binds.end(), "0.0.0.0") != binds.end() ||
      std::find(binds.begin(), binds.end(), "::") != binds.end()) {
    auto local_ip_addresses = GetLocalIPAddresses();
    return std::find(local_ip_addresses.begin(), local_ip_addresses.end(), ip) != local_ip_addresses.end();
  }
  return false;
}

std::vector<std::string> GetLocalIPAddresses() {
  std::vector<std::string> ip_addresses;
  ifaddrs *if_addr_struct = nullptr;
  std::unique_ptr<ifaddrs, decltype(&freeifaddrs)> ifaddrs_ptr(nullptr, &freeifaddrs);
  if (getifaddrs(&if_addr_struct) == -1) {
    return ip_addresses;
  }
  ifaddrs_ptr.reset(if_addr_struct);

  for (ifaddrs *ifa = if_addr_struct; ifa; ifa = ifa->ifa_next) {
    if (!ifa->ifa_addr) {
      continue;
    }
    void *tmp_addr_ptr = nullptr;
    if (ifa->ifa_addr->sa_family == AF_INET) {
      // check it is IPv4
      tmp_addr_ptr = &((sockaddr_in *)ifa->ifa_addr)->sin_addr;
      char address_buffer[INET_ADDRSTRLEN];
      inet_ntop(AF_INET, tmp_addr_ptr, address_buffer, INET_ADDRSTRLEN);
      ip_addresses.emplace_back(address_buffer);
    } else if (ifa->ifa_addr->sa_family == AF_INET6) {
      // check it is IPv6
      tmp_addr_ptr = &((sockaddr_in6 *)ifa->ifa_addr)->sin6_addr;
      char address_buffer[INET6_ADDRSTRLEN];
      inet_ntop(AF_INET6, tmp_addr_ptr, address_buffer, INET6_ADDRSTRLEN);
      ip_addresses.emplace_back(address_buffer);
    }
  }

  return ip_addresses;
}

template <auto syscall, typename FD, typename... Args>
Status WriteImpl(FD fd, std::string_view data, Args &&...args) {
  ssize_t n = 0;
  while (n < static_cast<ssize_t>(data.size())) {
    ssize_t nwritten = syscall(fd, data.data() + n, data.size() - n, std::forward<Args>(args)...);
    if (nwritten == -1) {
      return Status::FromErrno();
    }
    n += nwritten;
  }
  return Status::OK();
}

Status Write(int fd, const std::string &data) { return WriteImpl<write>(fd, data); }

Status Pwrite(int fd, const std::string &data, off_t offset) { return WriteImpl<pwrite>(fd, data, offset); }

Status SockSend(int fd, const std::string &data, [[maybe_unused]] ssl_st *ssl) {
#ifdef ENABLE_OPENSSL
  if (ssl) {
    return WriteImpl<SSL_write>(ssl, data);
  }
#endif
  return SockSend(fd, data);
}

Status SockSend(int fd, const std::string &data, [[maybe_unused]] bufferevent *bev) {
#ifdef ENABLE_OPENSSL
  return SockSend(fd, data, bufferevent_openssl_get_ssl(bev));
#else
  return SockSend(fd, data);
#endif
}

StatusOr<int> SockConnect(const std::string &host, uint32_t port, [[maybe_unused]] ssl_st *ssl, int conn_timeout,
                          int timeout) {
#ifdef ENABLE_OPENSSL
  if (ssl) {
    auto fd = GET_OR_RET(SockConnect(host, port, conn_timeout, timeout));
    SSL_set_fd(ssl, fd);

    auto bio = BIO_new_socket(fd, BIO_NOCLOSE);
    SSL_set_bio(ssl, bio, bio);

    if (int err = SSL_connect(ssl); err != 1) {
      BIO_free(bio);
      return {Status::NotOK, fmt::format("socket failed to do SSL handshake: {}", fmt::streamed(SSLError(err)))};
    }

    return fd;
  }
#endif
  return SockConnect(host, port, conn_timeout, timeout);
}

StatusOr<int> EvbufferRead(evbuffer *buf, evutil_socket_t fd, int howmuch, [[maybe_unused]] ssl_st *ssl) {
#ifdef ENABLE_OPENSSL
  if (ssl) {
    constexpr int BUFFER_SIZE = 4096;
    char tmp[BUFFER_SIZE];

    if (howmuch <= 0 || howmuch > BUFFER_SIZE) {
      howmuch = BUFFER_SIZE;
    }
    if (howmuch = SSL_read(ssl, tmp, howmuch); howmuch <= 0) {
      int err = SSL_get_error(ssl, howmuch);
      if (err == SSL_ERROR_ZERO_RETURN) {
        return {Status::EndOfFile, "EOF encountered while reading from SSL connection"};
      }
      return {(err == SSL_ERROR_WANT_READ) ? Status::TryAgain : Status::NotOK,
              fmt::format("failed to read from SSL connection: {}", fmt::streamed(SSLError(howmuch)))};
    }

    if (int ret = evbuffer_add(buf, tmp, howmuch); ret == -1) {
      return {Status::NotOK, fmt::format("failed to add buffer: {}", strerror(errno))};
    }

    return howmuch;
  }
#endif
  if (int ret = evbuffer_read(buf, fd, howmuch); ret > 0) {
    return ret;
  } else if (ret == 0) {
    return {Status::EndOfFile, "EOF encountered while reading from socket"};
  } else {
    return {(errno == EWOULDBLOCK || errno == EAGAIN) ? Status::TryAgain : Status::NotOK,
            fmt::format("failed to read from socket: {}", strerror(errno))};
  }
}

}  // namespace util
