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

#ifdef __linux__
#define _XOPEN_SOURCE 700  // NOLINT
#else
#define _XOPEN_SOURCE
#endif

#include <event2/thread.h>
#include <glog/logging.h>

#include <iomanip>
#include <ostream>

#include "daemon_util.h"
#include "glog/log_severity.h"
#include "io_util.h"
#include "pid_util.h"
#include "scope_exit.h"
#include "server/server.h"
#include "signal_util.h"
#include "storage/storage.h"
#include "string_util.h"
#include "time_util.h"
#include "vendor/crc64.h"
#include "version_util.h"

Server *srv = nullptr;

extern "C" void SignalHandler(int sig) {
  if (srv && !srv->IsStopped()) {
    LOG(INFO) << "Signal " << strsignal(sig) << " (" << sig << ") received, stopping the server";
    srv->Stop();
  }
}

struct NewOpt {
  friend auto &operator<<(std::ostream &os, NewOpt) { return os << std::string(4, ' ') << std::setw(32); }
} new_opt;

static void PrintUsage(const char *program) {
  std::cout << program << " implements the Redis protocol based on rocksdb" << std::endl
            << "Usage:" << std::endl
            << std::left << new_opt << "-c, --config <filename>"
            << "set config file to <filename>, or `-` for stdin" << std::endl
            << new_opt << "-v, --version"
            << "print version information" << std::endl
            << new_opt << "-h, --help"
            << "print this help message" << std::endl
            << new_opt << "--<config-key> <config-value>"
            << "overwrite specific config option <config-key> to <config-value>" << std::endl;
}

static CLIOptions ParseCommandLineOptions(int argc, char **argv) {
  using namespace std::string_view_literals;
  CLIOptions opts;

  for (int i = 1; i < argc; ++i) {
    if ((argv[i] == "-c"sv || argv[i] == "--config"sv) && i + 1 < argc) {
      opts.conf_file = argv[++i];
    } else if (argv[i] == "-v"sv || argv[i] == "--version"sv) {
      std::cout << "kvrocks " << PrintVersion << std::endl;
      std::exit(0);
    } else if (argv[i] == "-h"sv || argv[i] == "--help"sv) {
      PrintUsage(*argv);
      std::exit(0);
    } else if (std::string_view(argv[i], 2) == "--" && std::string_view(argv[i]).size() > 2 && i + 1 < argc) {
      auto key = std::string_view(argv[i] + 2);
      opts.cli_options.emplace_back(key, argv[++i]);
    } else {
      PrintUsage(*argv);
      std::exit(1);
    }
  }

  return opts;
}

static void InitGoogleLog(const Config *config) {
  FLAGS_minloglevel = config->log_level;
  FLAGS_max_log_size = 100;
  FLAGS_logbufsecs = 0;

  if (util::EqualICase(config->log_dir, "stdout")) {
    for (int level = google::INFO; level <= google::FATAL; level++) {
      google::SetLogDestination(static_cast<google::LogSeverity>(level), "");
    }
    FLAGS_stderrthreshold = google::ERROR;
    FLAGS_logtostdout = true;
    std::setbuf(stdout, nullptr);
  } else {
    FLAGS_log_dir = config->log_dir + "/";
    if (config->log_retention_days != -1) {
      google::EnableLogCleaner(std::chrono::hours(24) * config->log_retention_days);
    }
  }
}

int main(int argc, char *argv[]) {
  srand(static_cast<unsigned>(util::GetTimeStamp()));
  crc64_init();

  evthread_use_pthreads();
  auto event_exit = MakeScopeExit(libevent_global_shutdown);

  signal(SIGPIPE, SIG_IGN);
  SetupSigSegvAction(SignalHandler);

  auto opts = ParseCommandLineOptions(argc, argv);

  Config config;
  Status s = config.Load(opts);
  if (!s.IsOK()) {
    std::cout << "Failed to load config. Error: " << s.Msg() << std::endl;
    return 1;
  }
  const auto socket_fd_exit = MakeScopeExit([&config] {
    if (config.socket_fd != -1) {
      close(config.socket_fd);
    }
  });

  InitGoogleLog(&config);
  google::InitGoogleLogging("kvrocks");
  auto glog_exit = MakeScopeExit(google::ShutdownGoogleLogging);
  LOG(INFO) << "kvrocks " << PrintVersion;
  // Tricky: We don't expect that different instances running on the same port,
  // but the server use REUSE_PORT to support the multi listeners. So we connect
  // the listen port to check if the port has already listened or not.
  if (config.socket_fd == -1 && !config.binds.empty()) {
    uint32_t ports[] = {config.port, config.tls_port, 0};
    for (uint32_t *port = ports; *port; ++port) {
      if (util::IsPortInUse(*port)) {
        LOG(ERROR) << "Could not create server TCP since the specified port[" << *port << "] is already in use";
        return 1;
      }
    }
  }
  bool is_supervised = IsSupervisedMode(config.supervised_mode);
  if (config.daemonize && !is_supervised) Daemonize();
  s = CreatePidFile(config.pidfile);
  if (!s.IsOK()) {
    LOG(ERROR) << "Failed to create pidfile: " << s.Msg();
    return 1;
  }
  auto pidfile_exit = MakeScopeExit([&config] { RemovePidFile(config.pidfile); });

#ifdef ENABLE_OPENSSL
  // initialize OpenSSL
  if (config.tls_port || config.tls_replication) {
    InitSSL();
  }
#endif

  engine::Storage storage(&config);
  s = storage.Open();
  if (!s.IsOK()) {
    LOG(ERROR) << "Failed to open: " << s.Msg();
    return 1;
  }
  Server server(&storage, &config);
  srv = &server;
  s = srv->Start();
  if (!s.IsOK()) {
    LOG(ERROR) << "Failed to start server: " << s.Msg();
    return 1;
  }
  srv->Join();

  return 0;
}
