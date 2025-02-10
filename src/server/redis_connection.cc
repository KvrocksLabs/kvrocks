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

#include <glog/logging.h>
#include <rocksdb/iostats_context.h>
#include <rocksdb/perf_context.h>

#include <mutex>
#include <shared_mutex>

#include "commands/commander.h"
#include "commands/error_constants.h"
#include "fmt/format.h"
#include "nonstd/span.hpp"
#include "search/indexer.h"
#include "server/redis_reply.h"
#include "string_util.h"
#ifdef ENABLE_OPENSSL
#include <event2/bufferevent_ssl.h>
#endif

#include "commands/blocking_commander.h"
#include "redis_connection.h"
#include "scope_exit.h"
#include "server.h"
#include "time_util.h"
#include "tls_util.h"
#include "worker.h"

namespace redis {

Connection::Connection(bufferevent *bev, Worker *owner)
    : need_free_bev_(true), bev_(bev), req_(owner->srv), owner_(owner), srv_(owner->srv) {
  int64_t now = util::GetTimeStamp();
  create_time_ = now;
  last_interaction_ = now;
}

Connection::~Connection() {
  if (bev_) {
    if (need_free_bev_) {
      bufferevent_free(bev_);
    } else {
      // cleanup event callbacks here to prevent using Connection's resource
      bufferevent_setcb(bev_, nullptr, nullptr, nullptr, nullptr);
    }
  }
  // unsubscribe all channels and patterns if exists
  UnsubscribeAll();
  PUnsubscribeAll();
}

std::string Connection::ToString() {
  return fmt::format("id={} addr={} fd={} name={} age={} idle={} flags={} namespace={} qbuf={} obuf={} cmd={}\n", id_,
                     addr_, bufferevent_getfd(bev_), name_, GetAge(), GetIdleTime(), GetFlags(), ns_,
                     evbuffer_get_length(Input()), evbuffer_get_length(Output()), last_cmd_);
}

void Connection::Close() {
  if (close_cb) close_cb(GetFD());
  owner_->FreeConnection(this);
}

void Connection::Detach() { owner_->DetachConnection(this); }

void Connection::OnRead([[maybe_unused]] struct bufferevent *bev) {
  is_running_ = true;
  MakeScopeExit([this] { is_running_ = false; });

  SetLastInteraction();
  auto s = req_.Tokenize(Input());
  if (!s.IsOK()) {
    EnableFlag(redis::Connection::kCloseAfterReply);
    Reply(redis::Error(s));
    LOG(INFO) << "[connection] Failed to tokenize the request. Error: " << s.Msg();
    return;
  }

  ExecuteCommands(req_.GetCommands());
  if (IsFlagEnabled(kCloseAsync)) {
    Close();
  }
}

void Connection::OnWrite([[maybe_unused]] bufferevent *bev) {
  if (IsFlagEnabled(kCloseAfterReply) || IsFlagEnabled(kCloseAsync)) {
    Close();
  }
}

void Connection::OnEvent(bufferevent *bev, int16_t events) {
  if (events & BEV_EVENT_ERROR) {
    LOG(ERROR) << "[connection] Going to remove the client: " << GetAddr()
               << ", while encounter error: " << evutil_socket_error_to_string(EVUTIL_SOCKET_ERROR())
#ifdef ENABLE_OPENSSL
               << ", SSL Error: " << SSLError(bufferevent_get_openssl_error(bev))  // NOLINT
#endif
        ;  // NOLINT
    Close();
    return;
  }

  if (events & BEV_EVENT_EOF) {
    DLOG(INFO) << "[connection] Going to remove the client: " << GetAddr() << ", while closed by client";
    Close();
    return;
  }

  if (events & BEV_EVENT_TIMEOUT) {
    DLOG(INFO) << "[connection] The client: " << GetAddr() << "] reached timeout";
    bufferevent_enable(bev, EV_READ | EV_WRITE);
  }
}

void Connection::Reply(const std::string &msg) {
  owner_->srv->stats.IncrOutboundBytes(msg.size());
  redis::Reply(bufferevent_get_output(bev_), msg);
}

void Connection::SendFile(int fd) {
  // NOTE: we don't need to close the fd, the libevent will do that
  auto output = bufferevent_get_output(bev_);
  evbuffer_add_file(output, fd, 0, -1);
}

void Connection::SetAddr(std::string ip, uint32_t port) {
  ip_ = std::move(ip);
  port_ = port;
  addr_ = ip_ + ":" + std::to_string(port_);
}

uint64_t Connection::GetAge() const { return static_cast<uint64_t>(util::GetTimeStamp() - create_time_); }

void Connection::SetLastInteraction() { last_interaction_ = util::GetTimeStamp(); }

uint64_t Connection::GetIdleTime() const { return static_cast<uint64_t>(util::GetTimeStamp() - last_interaction_); }

// Currently, master connection is not handled in connection
// but in replication thread.
//
// The function will return one of the following:
//  kTypeSlave  -> Slave
//  kTypeNormal -> Normal client
//  kTypePubsub -> Client subscribed to Pub/Sub channels
uint64_t Connection::GetClientType() const {
  if (IsFlagEnabled(kSlave)) return kTypeSlave;

  if (!subscribe_channels_.empty() || !subscribe_patterns_.empty()) return kTypePubsub;

  return kTypeNormal;
}

std::string Connection::GetFlags() const {
  std::string flags;
  if (IsFlagEnabled(kSlave)) flags.append("S");
  if (IsFlagEnabled(kCloseAfterReply)) flags.append("c");
  if (IsFlagEnabled(kMonitor)) flags.append("M");
  if (IsFlagEnabled(kAsking)) flags.append("A");
  if (!subscribe_channels_.empty() || !subscribe_patterns_.empty()) flags.append("P");
  if (flags.empty()) flags = "N";
  return flags;
}

void Connection::EnableFlag(Flag flag) { flags_ |= flag; }

void Connection::DisableFlag(Flag flag) { flags_ &= (~flag); }

bool Connection::IsFlagEnabled(Flag flag) const { return (flags_ & flag) > 0; }

bool Connection::CanMigrate() const {
  return !is_running_                                                    // reading or writing
         && !IsFlagEnabled(redis::Connection::kCloseAfterReply)          // close after reply
         && saved_current_command_ == nullptr                            // not executing blocking command like BLPOP
         && subscribe_channels_.empty() && subscribe_patterns_.empty();  // not subscribing any channel
}

void Connection::SubscribeChannel(const std::string &channel) {
  for (const auto &chan : subscribe_channels_) {
    if (channel == chan) return;
  }

  subscribe_channels_.emplace_back(channel);
  owner_->srv->SubscribeChannel(channel, this);
}

void Connection::UnsubscribeChannel(const std::string &channel) {
  for (auto iter = subscribe_channels_.begin(); iter != subscribe_channels_.end(); iter++) {
    if (*iter == channel) {
      subscribe_channels_.erase(iter);
      owner_->srv->UnsubscribeChannel(channel, this);
      return;
    }
  }
}

void Connection::UnsubscribeAll(const UnsubscribeCallback &reply) {
  if (subscribe_channels_.empty()) {
    if (reply) reply("", static_cast<int>(subscribe_patterns_.size()));
    return;
  }

  int removed = 0;
  for (const auto &chan : subscribe_channels_) {
    owner_->srv->UnsubscribeChannel(chan, this);
    removed++;
    if (reply) {
      reply(chan, static_cast<int>(subscribe_channels_.size() - removed + subscribe_patterns_.size()));
    }
  }
  subscribe_channels_.clear();
}

int Connection::SubscriptionsCount() { return static_cast<int>(subscribe_channels_.size()); }

void Connection::PSubscribeChannel(const std::string &pattern) {
  for (const auto &p : subscribe_patterns_) {
    if (pattern == p) return;
  }
  subscribe_patterns_.emplace_back(pattern);
  owner_->srv->PSubscribeChannel(pattern, this);
}

void Connection::PUnsubscribeChannel(const std::string &pattern) {
  for (auto iter = subscribe_patterns_.begin(); iter != subscribe_patterns_.end(); iter++) {
    if (*iter == pattern) {
      subscribe_patterns_.erase(iter);
      owner_->srv->PUnsubscribeChannel(pattern, this);
      return;
    }
  }
}

void Connection::PUnsubscribeAll(const UnsubscribeCallback &reply) {
  if (subscribe_patterns_.empty()) {
    if (reply) reply("", static_cast<int>(subscribe_channels_.size()));
    return;
  }

  int removed = 0;
  for (const auto &pattern : subscribe_patterns_) {
    owner_->srv->PUnsubscribeChannel(pattern, this);
    removed++;
    if (reply) {
      reply(pattern, static_cast<int>(subscribe_patterns_.size() - removed + subscribe_channels_.size()));
    }
  }
  subscribe_patterns_.clear();
}

int Connection::PSubscriptionsCount() { return static_cast<int>(subscribe_patterns_.size()); }

void Connection::SSubscribeChannel(const std::string &channel, uint16_t slot) {
  for (const auto &chan : subscribe_shard_channels_) {
    if (channel == chan) return;
  }

  subscribe_shard_channels_.emplace_back(channel);
  owner_->srv->SSubscribeChannel(channel, this, slot);
}

void Connection::SUnsubscribeChannel(const std::string &channel, uint16_t slot) {
  for (auto iter = subscribe_shard_channels_.begin(); iter != subscribe_shard_channels_.end(); iter++) {
    if (*iter == channel) {
      subscribe_shard_channels_.erase(iter);
      owner_->srv->SUnsubscribeChannel(channel, this, slot);
      return;
    }
  }
}

void Connection::SUnsubscribeAll(const UnsubscribeCallback &reply) {
  if (subscribe_shard_channels_.empty()) {
    if (reply) reply("", 0);
    return;
  }

  int removed = 0;
  for (const auto &chan : subscribe_shard_channels_) {
    owner_->srv->SUnsubscribeChannel(chan, this,
                                     owner_->srv->GetConfig()->cluster_enabled ? GetSlotIdFromKey(chan) : 0);
    removed++;
    if (reply) {
      reply(chan, static_cast<int>(subscribe_shard_channels_.size() - removed));
    }
  }
  subscribe_shard_channels_.clear();
}

int Connection::SSubscriptionsCount() { return static_cast<int>(subscribe_shard_channels_.size()); }

bool Connection::IsProfilingEnabled(const std::string &cmd) {
  auto config = srv_->GetConfig();
  if (config->profiling_sample_ratio == 0) return false;

  if (!config->profiling_sample_all_commands &&
      config->profiling_sample_commands.find(cmd) == config->profiling_sample_commands.end()) {
    return false;
  }

  if (config->profiling_sample_ratio == 100 || std::rand() % 100 <= config->profiling_sample_ratio) {
    rocksdb::SetPerfLevel(rocksdb::PerfLevel::kEnableTimeExceptForMutex);
    rocksdb::get_perf_context()->Reset();
    rocksdb::get_iostats_context()->Reset();
    return true;
  }

  return false;
}

void Connection::RecordProfilingSampleIfNeed(const std::string &cmd, uint64_t duration) {
  int threshold = srv_->GetConfig()->profiling_sample_record_threshold_ms;
  if (threshold > 0 && static_cast<int>(duration / 1000) < threshold) {
    rocksdb::SetPerfLevel(rocksdb::PerfLevel::kDisable);
    return;
  }

  std::string perf_context = rocksdb::get_perf_context()->ToString(true);
  std::string iostats_context = rocksdb::get_iostats_context()->ToString(true);
  rocksdb::SetPerfLevel(rocksdb::PerfLevel::kDisable);
  if (perf_context.empty()) return;  // request without db operation

  auto entry = std::make_unique<PerfEntry>();
  entry->cmd_name = cmd;
  entry->duration = duration;
  entry->iostats_context = std::move(iostats_context);
  entry->perf_context = std::move(perf_context);
  srv_->GetPerfLog()->PushEntry(std::move(entry));
}

Status Connection::ExecuteCommand(engine::Context &ctx, const std::string &cmd_name,
                                  const std::vector<std::string> &cmd_tokens, Commander *current_cmd,
                                  std::string *reply) {
  srv_->stats.IncrCalls(cmd_name);

  auto start = std::chrono::high_resolution_clock::now();
  bool is_profiling = IsProfilingEnabled(cmd_name);
  auto s = current_cmd->Execute(ctx, srv_, this, reply);
  auto end = std::chrono::high_resolution_clock::now();
  uint64_t duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
  if (is_profiling) RecordProfilingSampleIfNeed(cmd_name, duration);

  srv_->SlowlogPushEntryIfNeeded(&cmd_tokens, duration, this);
  srv_->stats.IncrLatency(static_cast<uint64_t>(duration), cmd_name);
  return s;
}

static bool IsCmdForIndexing(uint64_t cmd_flags, CommandCategory cmd_cat) {
  return (cmd_flags & redis::kCmdWrite) &&
         (cmd_cat == CommandCategory::Hash || cmd_cat == CommandCategory::JSON || cmd_cat == CommandCategory::Key);
}

static bool IsCmdAllowedInStaleData(const std::string &cmd_name) {
  return cmd_name == "info" || cmd_name == "slaveof" || cmd_name == "config";
}

void Connection::ExecuteCommands(std::deque<CommandTokens> *to_process_cmds) {
  const Config *config = srv_->GetConfig();
  std::string reply;
  std::string password = config->requirepass;

  while (!to_process_cmds->empty()) {
    CommandTokens cmd_tokens = std::move(to_process_cmds->front());
    to_process_cmds->pop_front();
    if (cmd_tokens.empty()) continue;

    bool is_multi_exec = IsFlagEnabled(Connection::kMultiExec);
    if (IsFlagEnabled(redis::Connection::kCloseAfterReply) && !is_multi_exec) break;

    auto cmd_s = Server::LookupAndCreateCommand(cmd_tokens.front());
    if (!cmd_s.IsOK()) {
      auto cmd_name = cmd_tokens.front();
      if (util::EqualICase(cmd_name, "host:") || util::EqualICase(cmd_name, "post")) {
        LOG(WARNING) << "A likely HTTP request is detected in the RESP connection, indicating a potential "
                        "Cross-Protocol Scripting attack. Connection aborted.";
        EnableFlag(kCloseAsync);
        return;
      }
      if (is_multi_exec) multi_error_ = true;
      Reply(redis::Error(
          {Status::NotOK,
           fmt::format("unknown command `{}`, with args beginning with: {}", cmd_name,
                       util::StringJoin(nonstd::span(cmd_tokens.begin() + 1, cmd_tokens.end()),
                                        [](const auto &v) -> decltype(auto) { return fmt::format("`{}`", v); }))}));
      continue;
    }
    auto current_cmd = std::move(*cmd_s);

    const auto &attributes = current_cmd->GetAttributes();
    auto cmd_name = attributes->name;

    int tokens = static_cast<int>(cmd_tokens.size());
    if (!attributes->CheckArity(tokens)) {
      if (is_multi_exec) multi_error_ = true;
      Reply(redis::Error({Status::NotOK, "wrong number of arguments"}));
      continue;
    }

    auto cmd_flags = attributes->GenerateFlags(cmd_tokens);
    if (GetNamespace().empty()) {
      if (!password.empty()) {
        if (!(cmd_flags & kCmdAuth)) {
          Reply(redis::Error({Status::RedisNoAuth, "Authentication required."}));
          continue;
        }
      } else {
        BecomeAdmin();
        SetNamespace(kDefaultNamespace);
      }
    }

    std::shared_lock<std::shared_mutex> concurrency;  // Allow concurrency
    std::unique_lock<std::shared_mutex> exclusivity;  // Need exclusivity
    // If the command needs to process exclusively, we need to get 'ExclusivityGuard'
    // that can guarantee other threads can't come into critical zone, such as DEBUG,
    // CLUSTER subcommand, CONFIG SET, MULTI, LUA (in the immediate future).
    // Otherwise, we just use 'ConcurrencyGuard' to allow all workers to execute commands at the same time.
    if (is_multi_exec && !(cmd_flags & kCmdBypassMulti)) {
      // No lock guard, because 'exec' command has acquired 'WorkExclusivityGuard'
    } else if (cmd_flags & kCmdExclusive) {
      exclusivity = srv_->WorkExclusivityGuard();
    } else {
      concurrency = srv_->WorkConcurrencyGuard();
    }

    if (srv_->IsLoading() && !(cmd_flags & kCmdLoading)) {
      Reply(redis::Error({Status::RedisLoading, errRestoringBackup}));
      if (is_multi_exec) multi_error_ = true;
      continue;
    }

    current_cmd->SetArgs(cmd_tokens);
    auto s = current_cmd->Parse();
    if (!s.IsOK()) {
      if (is_multi_exec) multi_error_ = true;
      Reply(redis::Error(s));
      continue;
    }

    if (is_multi_exec && (cmd_flags & kCmdNoMulti)) {
      Reply(redis::Error({Status::NotOK, fmt::format("{} inside MULTI is not allowed", util::ToUpper(cmd_name))}));
      multi_error_ = true;
      continue;
    }

    if ((cmd_flags & kCmdAdmin) && !IsAdmin()) {
      Reply(redis::Error({Status::RedisExecErr, errAdminPermissionRequired}));
      continue;
    }

    if (config->cluster_enabled) {
      s = srv_->cluster->CanExecByMySelf(attributes, cmd_tokens, this);
      if (!s.IsOK()) {
        if (is_multi_exec) multi_error_ = true;
        Reply(redis::Error(s));
        continue;
      }
    }

    // reset the ASKING flag after executing the next query
    if (IsFlagEnabled(kAsking)) {
      DisableFlag(kAsking);
    }

    // We don't execute commands, but queue them, and then execute in EXEC command
    if (is_multi_exec && !in_exec_ && !(cmd_flags & kCmdBypassMulti)) {
      multi_cmds_.emplace_back(std::move(cmd_tokens));
      Reply(redis::SimpleString("QUEUED"));
      continue;
    }

    if (config->slave_readonly && srv_->IsSlave() && (cmd_flags & kCmdWrite)) {
      Reply(redis::Error({Status::RedisReadOnly, "You can't write against a read only slave."}));
      continue;
    }

    if ((cmd_flags & kCmdWrite) && !(cmd_flags & kCmdNoDBSizeCheck) && srv_->storage->ReachedDBSizeLimit()) {
      Reply(redis::Error({Status::NotOK, "write command not allowed when reached max-db-size."}));
      continue;
    }

    if (!config->slave_serve_stale_data && srv_->IsSlave() && !IsCmdAllowedInStaleData(cmd_name) &&
        srv_->GetReplicationState() != kReplConnected) {
      Reply(redis::Error({Status::RedisMasterDown,
                          "Link with MASTER is down "
                          "and slave-serve-stale-data is set to 'no'."}));
      continue;
    }

    ScopeExit in_script_exit{[this] { in_script_ = false; }, false};
    if (attributes->category == CommandCategory::Script || attributes->category == CommandCategory::Function) {
      in_script_ = true;
      in_script_exit.Enable();
    }

    SetLastCmd(cmd_name);
    {
      std::optional<MultiLockGuard> guard;
      if (cmd_flags & kCmdWrite) {
        std::vector<std::string> lock_keys;
        attributes->ForEachKeyRange(
            [&lock_keys, this](const std::vector<std::string> &args, const CommandKeyRange &key_range) {
              key_range.ForEachKey(
                  [&, this](const std::string &key) {
                    auto ns_key = ComposeNamespaceKey(ns_, key, srv_->storage->IsSlotIdEncoded());
                    lock_keys.emplace_back(std::move(ns_key));
                  },
                  args);
            },
            cmd_tokens);

        guard.emplace(srv_->storage->GetLockManager(), lock_keys);
      }
      engine::Context ctx(srv_->storage);

      std::vector<GlobalIndexer::RecordResult> index_records;
      if (!srv_->index_mgr.index_map.empty() && IsCmdForIndexing(cmd_flags, attributes->category) &&
          !config->cluster_enabled) {
        attributes->ForEachKeyRange(
            [&, this](const std::vector<std::string> &args, const CommandKeyRange &key_range) {
              key_range.ForEachKey(
                  [&, this](const std::string &key) {
                    auto res = srv_->indexer.Record(ctx, key, ns_);
                    if (res.IsOK()) {
                      index_records.push_back(*res);
                    } else if (!res.Is<Status::NoPrefixMatched>() && !res.Is<Status::TypeMismatched>()) {
                      LOG(WARNING) << "index recording failed for key: " << key;
                    }
                  },
                  args);
            },
            cmd_tokens);
      }

      s = ExecuteCommand(ctx, cmd_name, cmd_tokens, current_cmd.get(), &reply);
      for (const auto &record : index_records) {
        auto s = GlobalIndexer::Update(ctx, record);
        if (!s.IsOK() && !s.Is<Status::TypeMismatched>()) {
          LOG(WARNING) << "index updating failed for key: " << record.key;
        }
      }
    }

    srv_->FeedMonitorConns(this, cmd_tokens);

    // Break the execution loop when occurring the blocking command like BLPOP or BRPOP,
    // it will suspend the connection and wait for the wakeup signal.
    if (s.Is<Status::BlockingCmd>()) {
      // For the blocking command, it will use the command while resumed from the suspend state.
      // So we need to save the command for the next execution.
      // Migrate connection would also check the saved_current_command_ to determine whether
      // the connection can be migrated or not.
      saved_current_command_ = std::move(current_cmd);
      break;
    }

    // Reply for MULTI
    if (!s.IsOK()) {
      Reply(redis::Error(s));
      continue;
    }

    srv_->UpdateWatchedKeysFromArgs(cmd_tokens, *attributes);

    if (!reply.empty()) Reply(reply);
    reply.clear();
  }
}

void Connection::ResetMultiExec() {
  in_exec_ = false;
  multi_error_ = false;
  multi_cmds_.clear();
  DisableFlag(Connection::kMultiExec);
}

}  // namespace redis
