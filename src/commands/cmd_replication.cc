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

#include "commander.h"
#include "error_constants.h"
#include "io_util.h"
#include "scope_exit.h"
#include "server/redis_reply.h"
#include "server/server.h"
#include "thread_util.h"
#include "time_util.h"
#include "unique_fd.h"

namespace redis {

class CommandPSync : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    size_t seq_arg = 1;
    if (args.size() == 3) {
      seq_arg = 2;
      new_psync_ = true;
    }

    auto parse_result = ParseInt<uint64_t>(args[seq_arg], 10);
    if (!parse_result) {
      return {Status::RedisParseErr, "value is not an unsigned long long or out of range"};
    }

    next_repl_seq_ = static_cast<rocksdb::SequenceNumber>(*parse_result);
    if (new_psync_) {
      assert(args.size() == 3);
      replica_replid_ = args[1];
      if (replica_replid_.size() != kReplIdLength) {
        return {Status::RedisParseErr, "Wrong replication id length"};
      }
    }

    return Commander::Parse(args);
  }

  Status Execute([[maybe_unused]] engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    LOG(INFO) << "Slave " << conn->GetAddr() << ", listening port: " << conn->GetListeningPort()
              << ", announce ip: " << conn->GetAnnounceIP() << " asks for synchronization"
              << " with next sequence: " << next_repl_seq_
              << " replication id: " << (replica_replid_.length() ? replica_replid_ : "not supported")
              << ", and local sequence: " << srv->storage->LatestSeqNumber();

    bool need_full_sync = false;

    // Check replication id of the last sequence log
    if (new_psync_ && srv->GetConfig()->use_rsid_psync) {
      std::string replid_in_wal = srv->storage->GetReplIdFromWalBySeq(next_repl_seq_ - 1);
      LOG(INFO) << "Replication id in WAL: " << replid_in_wal;

      // We check replication id only when WAL has this sequence, since there may be no WAL,
      // Or WAL may have nothing when starting from db of old version kvrocks.
      if (replid_in_wal.length() == kReplIdLength && replid_in_wal != replica_replid_) {
        *output = "wrong replication id of the last log";
        need_full_sync = true;
      }
    }

    // Check Log sequence
    if (!need_full_sync && !checkWALBoundary(srv->storage, next_repl_seq_).IsOK()) {
      *output = "sequence out of range, please use fullsync";
      need_full_sync = true;
    }

    if (need_full_sync) {
      srv->stats.IncrPSyncErrCount();
      return {Status::RedisExecErr, *output};
    }

    // Server would spawn a new thread to sync the batch, and connection would
    // be taken over, so should never trigger any event in worker thread.
    conn->Detach();
    conn->EnableFlag(redis::Connection::kSlave);
    auto s = util::SockSetBlocking(conn->GetFD(), 1);
    if (!s.IsOK()) {
      conn->EnableFlag(redis::Connection::kCloseAsync);
      return s.Prefixed("failed to set blocking mode on socket");
    }

    srv->stats.IncrPSyncOKCount();
    s = srv->AddSlave(conn, next_repl_seq_);
    if (!s.IsOK()) {
      std::string err = redis::Error(s);
      s = util::SockSend(conn->GetFD(), err, conn->GetBufferEvent());
      if (!s.IsOK()) {
        LOG(WARNING) << "failed to send error message to the replica: " << s.Msg();
      }
      conn->EnableFlag(redis::Connection::kCloseAsync);
      LOG(WARNING) << "Failed to add replica: " << conn->GetAddr() << " to start incremental syncing";
    } else {
      LOG(INFO) << "New replica: " << conn->GetAddr() << " was added, start incremental syncing";
    }
    return s;
  }

 private:
  rocksdb::SequenceNumber next_repl_seq_ = 0;
  bool new_psync_ = false;
  std::string replica_replid_;

  // Return OK if the seq is in the range of the current WAL
  static Status checkWALBoundary(engine::Storage *storage, rocksdb::SequenceNumber seq) {
    if (seq == storage->LatestSeqNumber() + 1) {
      return Status::OK();
    }

    // Upper bound
    if (seq > storage->LatestSeqNumber() + 1) {
      return {Status::NotOK};
    }

    // Lower bound
    std::unique_ptr<rocksdb::TransactionLogIterator> iter;
    auto s = storage->GetWALIter(seq, &iter);
    if (s.IsOK() && iter->Valid()) {
      auto batch = iter->GetBatch();
      if (seq != batch.sequence) {
        if (seq > batch.sequence) {
          LOG(ERROR) << "checkWALBoundary with sequence: " << seq
                     << ", but GetWALIter return older sequence: " << batch.sequence;
        }
        return {Status::NotOK};
      }
      return Status::OK();
    }
    return {Status::NotOK};
  }
};

class CommandReplConf : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    if (args.size() % 2 == 0) {
      return {Status::RedisParseErr, errWrongNumOfArguments};
    }

    for (size_t i = 1; i < args.size(); i += 2) {
      Status s = ParseParam(util::ToLower(args[i]), args[i + 1]);
      if (!s.IsOK()) {
        return s;
      }
    }

    return Commander::Parse(args);
  }

  Status ParseParam(const std::string &option, const std::string &value) {
    if (option == "listening-port") {
      auto parse_result = ParseInt<int>(value, NumericRange<int>{1, PORT_LIMIT - 1}, 10);
      if (!parse_result) {
        return {Status::RedisParseErr, "listening-port should be number or out of range"};
      }

      port_ = *parse_result;
    } else if (option == "ip-address") {
      if (value == "") {
        return {Status::RedisParseErr, "ip-address should not be empty"};
      }
      ip_address_ = value;
    } else {
      return {Status::RedisParseErr, errUnknownOption};
    }

    return Status::OK();
  }

  Status Execute([[maybe_unused]] engine::Context &ctx, [[maybe_unused]] Server *srv, Connection *conn,
                 std::string *output) override {
    if (port_ != 0) {
      conn->SetListeningPort(port_);
    }
    if (!ip_address_.empty()) {
      conn->SetAnnounceIP(ip_address_);
    }
    *output = redis::RESP_OK;
    return Status::OK();
  }

 private:
  int port_ = 0;
  std::string ip_address_;
};

class CommandFetchMeta : public Commander {
 public:
  Status Parse([[maybe_unused]] const std::vector<std::string> &args) override { return Status::OK(); }

  Status Execute([[maybe_unused]] engine::Context &ctx, Server *srv, Connection *conn,
                 [[maybe_unused]] std::string *output) override {
    int repl_fd = conn->GetFD();
    std::string ip = conn->GetAnnounceIP();

    auto s = util::SockSetBlocking(repl_fd, 1);
    if (!s.IsOK()) {
      return s.Prefixed("failed to set blocking mode on socket");
    }

    conn->NeedNotFreeBufferEvent();
    conn->EnableFlag(redis::Connection::kCloseAsync);
    srv->stats.IncrFullSyncCount();

    // Feed-replica-meta thread
    auto t = GET_OR_RET(util::CreateThread("feed-repl-info", [srv, repl_fd, ip, bev = conn->GetBufferEvent()] {
      srv->IncrFetchFileThread();
      auto exit = MakeScopeExit([srv, bev] {
        bufferevent_free(bev);
        srv->DecrFetchFileThread();
      });

      std::string files;
      auto s = engine::Storage::ReplDataManager::GetFullReplDataInfo(srv->storage, &files);
      if (!s.IsOK()) {
        LOG(WARNING) << "[replication] Failed to get full data file info: " << s.Msg();
        s = util::SockSend(repl_fd, redis::Error({Status::RedisErrorNoPrefix, "can't create db checkpoint"}), bev);
        if (!s.IsOK()) {
          LOG(WARNING) << "[replication] Failed to send error response: " << s.Msg();
        }
        return;
      }
      // Send full data file info
      if (auto s = util::SockSend(repl_fd, files + CRLF, bev)) {
        LOG(INFO) << "[replication] Succeed sending full data file info to " << ip;
      } else {
        LOG(WARNING) << "[replication] Fail to send full data file info " << ip << ", error: " << s.Msg();
      }
      auto now_secs = static_cast<time_t>(util::GetTimeStamp());
      srv->storage->SetCheckpointAccessTimeSecs(now_secs);
    }));

    if (auto s = util::ThreadDetach(t); !s) {
      return s;
    }

    return Status::OK();
  }
};

class CommandFetchFile : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    files_str_ = args[1];
    return Status::OK();
  }

  Status Execute([[maybe_unused]] engine::Context &ctx, Server *srv, Connection *conn,
                 [[maybe_unused]] std::string *output) override {
    std::vector<std::string> files = util::Split(files_str_, ",");

    int repl_fd = conn->GetFD();
    std::string ip = conn->GetAnnounceIP();

    auto s = util::SockSetBlocking(repl_fd, 1);
    if (!s.IsOK()) {
      return s.Prefixed("failed to set blocking mode on socket");
    }

    conn->NeedNotFreeBufferEvent();  // Feed-replica-file thread will close the replica bufferevent
    conn->EnableFlag(redis::Connection::kCloseAsync);

    auto t = GET_OR_RET(util::CreateThread("feed-repl-file", [srv, repl_fd, ip, files, bev = conn->GetBufferEvent()]() {
      auto exit = MakeScopeExit([bev] { bufferevent_free(bev); });
      srv->IncrFetchFileThread();

      for (const auto &file : files) {
        if (srv->IsStopped()) break;

        uint64_t file_size = 0, max_replication_bytes = 0;
        if (srv->GetConfig()->max_replication_mb > 0 && srv->GetFetchFileThreadNum() != 0) {
          max_replication_bytes = (srv->GetConfig()->max_replication_mb * MiB) / srv->GetFetchFileThreadNum();
        }
        auto start = std::chrono::high_resolution_clock::now();
        auto fd = UniqueFD(engine::Storage::ReplDataManager::OpenDataFile(srv->storage, file, &file_size));
        if (!fd) break;

        // Send file size and content
        auto s = util::SockSend(repl_fd, std::to_string(file_size) + CRLF, bev);
        if (s) {
          s = util::SockSendFile(repl_fd, *fd, file_size, bev);
        }
        if (s) {
          LOG(INFO) << "[replication] Succeed sending file " << file << " to " << ip;
        } else {
          LOG(WARNING) << "[replication] Fail to send file " << file << " to " << ip << ", error: " << s.Msg();
          break;
        }
        fd.Close();

        // Sleep if the speed of sending file is more than replication speed limit
        auto end = std::chrono::high_resolution_clock::now();
        uint64_t duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
        if (max_replication_bytes > 0) {
          auto shortest = static_cast<uint64_t>(static_cast<double>(file_size) /
                                                static_cast<double>(max_replication_bytes) * (1000 * 1000));
          if (duration < shortest) {
            LOG(INFO) << "[replication] Need to sleep " << (shortest - duration) / 1000
                      << " ms since of sending files too quickly";
            usleep(shortest - duration);
          }
        }
      }
      auto now_secs = util::GetTimeStamp<std::chrono::seconds>();
      srv->storage->SetCheckpointAccessTimeSecs(now_secs);
      srv->DecrFetchFileThread();
    }));

    if (auto s = util::ThreadDetach(t); !s) {
      return s;
    }

    return Status::OK();
  }

 private:
  std::string files_str_;
};

class CommandDBName : public Commander {
 public:
  Status Parse([[maybe_unused]] const std::vector<std::string> &args) override { return Status::OK(); }

  Status Execute([[maybe_unused]] engine::Context &ctx, Server *srv, Connection *conn,
                 [[maybe_unused]] std::string *output) override {
    conn->Reply(srv->storage->GetName() + CRLF);
    return Status::OK();
  }
};

REDIS_REGISTER_COMMANDS(Replication, MakeCmdAttr<CommandReplConf>("replconf", -3, "read-only no-script", NO_KEY),
                        MakeCmdAttr<CommandPSync>("psync", -2, "read-only no-multi no-script", NO_KEY),
                        MakeCmdAttr<CommandFetchMeta>("_fetch_meta", 1, "read-only no-multi no-script", NO_KEY),
                        MakeCmdAttr<CommandFetchFile>("_fetch_file", 2, "read-only no-multi no-script", NO_KEY),
                        MakeCmdAttr<CommandDBName>("_db_name", 1, "read-only no-multi", NO_KEY), )

}  // namespace redis
