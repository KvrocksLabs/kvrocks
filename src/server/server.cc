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

#include "server.h"

#include <glog/logging.h>
#include <rocksdb/convenience.h>
#include <rocksdb/statistics.h>
#include <sys/resource.h>
#include <sys/statvfs.h>
#include <sys/utsname.h>

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <functional>
#include <iomanip>
#include <jsoncons/json.hpp>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <utility>

#include "commands/commander.h"
#include "common/string_util.h"
#include "config/config.h"
#include "fmt/format.h"
#include "redis_connection.h"
#include "rocksdb/version.h"
#include "storage/compaction_checker.h"
#include "storage/redis_db.h"
#include "storage/scripting.h"
#include "storage/storage.h"
#include "thread_util.h"
#include "time_util.h"
#include "version.h"
#include "worker.h"

Server::Server(engine::Storage *storage, Config *config)
    : stats(config->histogram_bucket_boundaries),
      storage(storage),
      indexer(storage),
      index_mgr(&indexer, storage),
      start_time_secs_(util::GetTimeStamp()),
      config_(config),
      namespace_(storage) {
  // init commands stats here to prevent concurrent insert, and cause core
  auto commands = redis::CommandTable::GetOriginal();

  for (const auto &iter : *commands) {
    stats.commands_stats[iter.first].calls = 0;
    stats.commands_stats[iter.first].latency = 0;

    if (stats.bucket_boundaries.size() > 0) {
      // NB: Extra index for the last bucket (Inf)
      for (std::size_t i{0}; i <= stats.bucket_boundaries.size(); ++i) {
        stats.commands_histogram[iter.first].buckets.push_back(std::make_unique<std::atomic<uint64_t>>(0));
      }
      stats.commands_histogram[iter.first].calls = 0;
      stats.commands_histogram[iter.first].sum = 0;
    }
  }

  // init cursor_dict_
  cursor_dict_ = std::make_unique<CursorDictType>();

#ifdef ENABLE_OPENSSL
  // init ssl context
  if (config->tls_port || config->tls_replication) {
    ssl_ctx = CreateSSLContext(config);
    if (!ssl_ctx) {
      exit(1);
    }
  }
#endif

  // Init cluster
  cluster = std::make_unique<Cluster>(this, config_->binds, config_->port);

  // init shard pub/sub channels
  pubsub_shard_channels_.resize(config->cluster_enabled ? HASH_SLOTS_SIZE : 1);

  for (int i = 0; i < config->workers; i++) {
    auto worker = std::make_unique<Worker>(this, config);
    // multiple workers can't listen to the same unix socket, so
    // listen unix socket only from a single worker - the first one
    if (!config->unixsocket.empty() && i == 0) {
      Status s = worker->ListenUnixSocket(config->unixsocket, config->unixsocketperm, config->backlog);
      if (!s.IsOK()) {
        LOG(ERROR) << "[server] Failed to listen on unix socket: " << config->unixsocket << ". Error: " << s.Msg();
        exit(1);
      }
      LOG(INFO) << "[server] Listening on unix socket: " << config->unixsocket;
    }
    worker_threads_.emplace_back(std::make_unique<WorkerThread>(std::move(worker)));
  }

  AdjustOpenFilesLimit();
  slow_log_.SetMaxEntries(config->slowlog_max_len);
  perf_log_.SetMaxEntries(config->profiling_sample_record_max_len);
}

Server::~Server() {
  DisconnectSlaves();
  // Wait for all fetch file threads stop and exit and force destroy the server after 60s.
  int counter = 0;
  while (GetFetchFileThreadNum() != 0) {
    usleep(100000);
    if (++counter == 600) {
      LOG(WARNING) << "[server] Will force destroy the server after waiting 60s, leave " << GetFetchFileThreadNum()
                   << " fetch file threads are still running";
      break;
    }
  }

  for (auto &worker_thread : worker_threads_) {
    worker_thread.reset();
  }
  cleanupExitedWorkerThreads(true /* force */);
  CleanupExitedSlaves();
}

// Kvrocks threads list:
// - Work-thread: process client's connections and requests
// - Task-runner: one thread pool, handle some jobs that may freeze server if run directly
// - Cron-thread: server's crontab, clean backups, resize sst and memtable size
// - Compaction-checker: active compaction according to collected statistics
// - Replication-thread: replicate incremental stream from master if in slave role, there
//   are some dynamic threads to fetch files when full sync.
//     - fetch-file-thread: fetch SST files from master
// - Feed-slave-thread: feed data to slaves if having slaves, but there also are some dynamic
//   threads when full sync, TODO(@shooterit) we should manage this threads uniformly.
//     - feed-replica-data-info: generate checkpoint and send files list when full sync
//     - feed-replica-file: send SST files when slaves ask for full sync
Status Server::Start() {
  auto s = namespace_.LoadAndRewrite();
  if (!s.IsOK()) {
    return s;
  }
  if (!config_->master_host.empty()) {
    s = AddMaster(config_->master_host, static_cast<uint32_t>(config_->master_port), false);
    if (!s.IsOK()) return s;
  } else {
    // Generate new replication id if not a replica
    engine::Context ctx(storage);
    s = storage->ShiftReplId(ctx);
    if (!s.IsOK()) {
      return s.Prefixed("failed to shift replication id");
    }
  }

  if (!config_->cluster_enabled) {
    engine::Context no_txn_ctx = engine::Context::NoTransactionContext(storage);
    GET_OR_RET(index_mgr.Load(no_txn_ctx, kDefaultNamespace));
    for (const auto &[_, ns] : namespace_.List()) {
      GET_OR_RET(index_mgr.Load(no_txn_ctx, ns));
    }
  }

  if (config_->cluster_enabled) {
    if (config_->persist_cluster_nodes_enabled) {
      auto s = cluster->LoadClusterNodes(config_->NodesFilePath());
      if (!s.IsOK()) {
        return s.Prefixed("failed to load cluster nodes info");
      }
    }
    // Create objects used for slot migration
    slot_migrator = std::make_unique<SlotMigrator>(this);
    auto s = slot_migrator->CreateMigrationThread();
    if (!s.IsOK()) {
      return s.Prefixed("failed to create migration thread");
    }

    slot_import = std::make_unique<SlotImport>(this);
  }

  for (const auto &worker : worker_threads_) {
    worker->Start();
  }

  if (auto s = task_runner_.Start(); !s) {
    LOG(WARNING) << "Failed to start task runner: " << s.Msg();
  }
  // setup server cron thread
  cron_thread_ = GET_OR_RET(util::CreateThread("server-cron", [this] { this->cron(); }));

  compaction_checker_thread_ = GET_OR_RET(util::CreateThread("compact-check", [this] {
    uint64_t counter = 0;
    int64_t last_compact_date = 0;
    CompactionChecker compaction_checker{this->storage};

    while (!stop_) {
      // Sleep first
      std::this_thread::sleep_for(std::chrono::milliseconds(100));

      // To guarantee accessing DB safely
      auto guard = storage->ReadLockGuard();
      if (storage->IsClosing()) continue;

      if (!is_loading_ && ++counter % 600 == 0  // check every minute
          && config_->compaction_checker_cron.IsEnabled()) {
        auto t_now = static_cast<time_t>(util::GetTimeStamp());
        std::tm now{};
        localtime_r(&t_now, &now);
        if (config_->compaction_checker_cron.IsTimeMatch(&now)) {
          const auto &column_family_list = engine::ColumnFamilyConfigs::ListAllColumnFamilies();
          for (auto &column_family : column_family_list) {
            compaction_checker.PickCompactionFilesForCf(column_family);
          }
        }
        // compact once per day
        auto now_hours = t_now / 3600;
        if (now_hours != 0 && last_compact_date != now_hours / 24) {
          last_compact_date = now_hours / 24;
          compaction_checker.CompactPropagateAndPubSubFiles();
        }
      }
    }
  }));

  memory_startup_use_.store(Stats::GetMemoryRSS(), std::memory_order_relaxed);
  LOG(INFO) << "[server] Ready to accept connections";

  return Status::OK();
}

void Server::Stop() {
  stop_ = true;

  slaveof_mu_.lock();
  if (replication_thread_) replication_thread_->Stop();
  slaveof_mu_.unlock();

  for (const auto &worker : worker_threads_) {
    worker->Stop(0 /* immediately terminate  */);
  }

  rocksdb::CancelAllBackgroundWork(storage->GetDB(), true);
  task_runner_.Cancel();
}

void Server::Join() {
  if (auto s = util::ThreadJoin(cron_thread_); !s) {
    LOG(WARNING) << "Cron thread operation failed: " << s.Msg();
  }
  if (auto s = util::ThreadJoin(compaction_checker_thread_); !s) {
    LOG(WARNING) << "Compaction checker thread operation failed: " << s.Msg();
  }
  if (auto s = task_runner_.Join(); !s) {
    LOG(WARNING) << s.Msg();
  }
  for (const auto &worker : worker_threads_) {
    worker->Join();
  }
}

Status Server::AddMaster(const std::string &host, uint32_t port, bool force_reconnect) {
  std::lock_guard<std::mutex> guard(slaveof_mu_);

  // Don't check host and port if 'force_reconnect' argument is set to true
  if (!force_reconnect && !master_host_.empty() && master_host_ == host && master_port_ == port) {
    return Status::OK();
  }

  // Master is changed
  if (!master_host_.empty()) {
    if (replication_thread_) replication_thread_->Stop();
    replication_thread_ = nullptr;
  }

  // For master using old version, it uses replication thread to implement
  // replication, and uses 'listen-port + 1' as thread listening port.
  uint32_t master_listen_port = port;
  if (GetConfig()->master_use_repl_port) master_listen_port += 1;

  replication_thread_ = std::make_unique<ReplicationThread>(host, master_listen_port, this);
  auto s = replication_thread_->Start([this]() { return PrepareRestoreDB(); },
                                      [this]() {
                                        this->is_loading_ = false;
                                        if (auto s = task_runner_.Start(); !s) {
                                          LOG(WARNING) << "Failed to start task runner: " << s.Msg();
                                        }
                                      });
  if (s.IsOK()) {
    master_host_ = host;
    master_port_ = port;
    config_->SetMaster(host, port);
  } else {
    replication_thread_ = nullptr;
  }
  return s;
}

Status Server::RemoveMaster() {
  std::lock_guard<std::mutex> guard(slaveof_mu_);

  if (!master_host_.empty()) {
    master_host_.clear();
    master_port_ = 0;
    config_->ClearMaster();
    if (replication_thread_) {
      replication_thread_->Stop();
      replication_thread_ = nullptr;
    }
    engine::Context ctx(storage);
    return storage->ShiftReplId(ctx);
  }
  return Status::OK();
}

Status Server::AddSlave(redis::Connection *conn, rocksdb::SequenceNumber next_repl_seq) {
  auto t = std::make_unique<FeedSlaveThread>(this, conn, next_repl_seq);
  auto s = t->Start();
  if (!s.IsOK()) {
    return s;
  }

  std::lock_guard<std::mutex> lg(slave_threads_mu_);
  slave_threads_.emplace_back(std::move(t));
  return Status::OK();
}

void Server::DisconnectSlaves() {
  std::lock_guard<std::mutex> lg(slave_threads_mu_);

  for (auto &slave_thread : slave_threads_) {
    if (!slave_thread->IsStopped()) slave_thread->Stop();
  }

  while (!slave_threads_.empty()) {
    auto slave_thread = std::move(slave_threads_.front());
    slave_threads_.pop_front();
    slave_thread->Join();
  }
}

void Server::CleanupExitedSlaves() {
  std::lock_guard<std::mutex> lg(slave_threads_mu_);

  for (auto it = slave_threads_.begin(); it != slave_threads_.end();) {
    if ((*it)->IsStopped()) {
      auto thread = std::move(*it);
      it = slave_threads_.erase(it);
      thread->Join();
    } else {
      ++it;
    }
  }
}

void Server::FeedMonitorConns(redis::Connection *conn, const std::vector<std::string> &tokens) {
  if (monitor_clients_ <= 0) return;

  auto now_us = util::GetTimeStampUS();
  std::string output =
      fmt::format("{}.{} [{} {}]", now_us / 1000000, now_us % 1000000, conn->GetNamespace(), conn->GetAddr());
  for (const auto &token : tokens) {
    output += " \"";
    output += util::EscapeString(token);
    output += "\"";
  }

  for (const auto &worker_thread : worker_threads_) {
    auto worker = worker_thread->GetWorker();
    worker->FeedMonitorConns(conn, redis::SimpleString(output));
  }
}

int Server::PublishMessage(const std::string &channel, const std::string &msg) {
  int cnt = 0;
  int index = 0;

  pubsub_channels_mu_.lock();

  std::vector<ConnContext> to_publish_conn_ctxs;
  if (auto iter = pubsub_channels_.find(channel); iter != pubsub_channels_.end()) {
    for (const auto &conn_ctx : iter->second) {
      to_publish_conn_ctxs.emplace_back(conn_ctx);
    }
  }

  // The patterns variable records the pattern of connections
  std::vector<std::string> patterns;
  std::vector<ConnContext> to_publish_patterns_conn_ctxs;
  for (const auto &iter : pubsub_patterns_) {
    if (util::StringMatch(iter.first, channel, false)) {
      for (const auto &conn_ctx : iter.second) {
        to_publish_patterns_conn_ctxs.emplace_back(conn_ctx);
        patterns.emplace_back(iter.first);
      }
    }
  }
  pubsub_channels_mu_.unlock();

  std::string channel_reply;
  channel_reply.append(redis::MultiLen(3));
  channel_reply.append(redis::BulkString("message"));
  channel_reply.append(redis::BulkString(channel));
  channel_reply.append(redis::BulkString(msg));
  for (const auto &conn_ctx : to_publish_conn_ctxs) {
    auto s = conn_ctx.owner->Reply(conn_ctx.fd, channel_reply);
    if (s.IsOK()) {
      cnt++;
    }
  }

  // We should publish corresponding pattern and message for connections
  for (const auto &conn_ctx : to_publish_patterns_conn_ctxs) {
    std::string pattern_reply;
    pattern_reply.append(redis::MultiLen(4));
    pattern_reply.append(redis::BulkString("pmessage"));
    pattern_reply.append(redis::BulkString(patterns[index++]));
    pattern_reply.append(redis::BulkString(channel));
    pattern_reply.append(redis::BulkString(msg));
    auto s = conn_ctx.owner->Reply(conn_ctx.fd, pattern_reply);
    if (s.IsOK()) {
      cnt++;
    }
  }

  return cnt;
}

void Server::SubscribeChannel(const std::string &channel, redis::Connection *conn) {
  std::lock_guard<std::mutex> guard(pubsub_channels_mu_);

  auto conn_ctx = ConnContext(conn->Owner(), conn->GetFD());
  if (auto iter = pubsub_channels_.find(channel); iter == pubsub_channels_.end()) {
    pubsub_channels_.emplace(channel, std::list<ConnContext>{conn_ctx});
  } else {
    iter->second.emplace_back(conn_ctx);
  }
}

void Server::UnsubscribeChannel(const std::string &channel, redis::Connection *conn) {
  std::lock_guard<std::mutex> guard(pubsub_channels_mu_);

  auto iter = pubsub_channels_.find(channel);
  if (iter == pubsub_channels_.end()) {
    return;
  }

  for (const auto &conn_ctx : iter->second) {
    if (conn->GetFD() == conn_ctx.fd && conn->Owner() == conn_ctx.owner) {
      iter->second.remove(conn_ctx);
      if (iter->second.empty()) {
        pubsub_channels_.erase(iter);
      }
      break;
    }
  }
}

void Server::GetChannelsByPattern(const std::string &pattern, std::vector<std::string> *channels) {
  std::lock_guard<std::mutex> guard(pubsub_channels_mu_);

  for (const auto &iter : pubsub_channels_) {
    if (pattern.empty() || util::StringMatch(pattern, iter.first, false)) {
      channels->emplace_back(iter.first);
    }
  }
}

void Server::ListChannelSubscribeNum(const std::vector<std::string> &channels,
                                     std::vector<ChannelSubscribeNum> *channel_subscribe_nums) {
  std::lock_guard<std::mutex> guard(pubsub_channels_mu_);

  for (const auto &chan : channels) {
    if (auto iter = pubsub_channels_.find(chan); iter != pubsub_channels_.end()) {
      channel_subscribe_nums->emplace_back(ChannelSubscribeNum{iter->first, iter->second.size()});
    } else {
      channel_subscribe_nums->emplace_back(ChannelSubscribeNum{chan, 0});
    }
  }
}

void Server::PSubscribeChannel(const std::string &pattern, redis::Connection *conn) {
  std::lock_guard<std::mutex> guard(pubsub_channels_mu_);

  auto conn_ctx = ConnContext(conn->Owner(), conn->GetFD());
  if (auto iter = pubsub_patterns_.find(pattern); iter == pubsub_patterns_.end()) {
    pubsub_patterns_.emplace(pattern, std::list<ConnContext>{conn_ctx});
  } else {
    iter->second.emplace_back(conn_ctx);
  }
}

void Server::PUnsubscribeChannel(const std::string &pattern, redis::Connection *conn) {
  std::lock_guard<std::mutex> guard(pubsub_channels_mu_);

  auto iter = pubsub_patterns_.find(pattern);
  if (iter == pubsub_patterns_.end()) {
    return;
  }

  for (const auto &conn_ctx : iter->second) {
    if (conn->GetFD() == conn_ctx.fd && conn->Owner() == conn_ctx.owner) {
      iter->second.remove(conn_ctx);
      if (iter->second.empty()) {
        pubsub_patterns_.erase(iter);
      }
      break;
    }
  }
}

void Server::SSubscribeChannel(const std::string &channel, redis::Connection *conn, uint16_t slot) {
  assert((config_->cluster_enabled && slot < HASH_SLOTS_SIZE) || slot == 0);
  std::lock_guard<std::mutex> guard(pubsub_shard_channels_mu_);

  auto conn_ctx = ConnContext(conn->Owner(), conn->GetFD());
  if (auto iter = pubsub_shard_channels_[slot].find(channel); iter == pubsub_shard_channels_[slot].end()) {
    pubsub_shard_channels_[slot].emplace(channel, std::list<ConnContext>{conn_ctx});
  } else {
    iter->second.emplace_back(conn_ctx);
  }
}

void Server::SUnsubscribeChannel(const std::string &channel, redis::Connection *conn, uint16_t slot) {
  assert((config_->cluster_enabled && slot < HASH_SLOTS_SIZE) || slot == 0);
  std::lock_guard<std::mutex> guard(pubsub_shard_channels_mu_);

  auto iter = pubsub_shard_channels_[slot].find(channel);
  if (iter == pubsub_shard_channels_[slot].end()) {
    return;
  }

  for (const auto &conn_ctx : iter->second) {
    if (conn->GetFD() == conn_ctx.fd && conn->Owner() == conn_ctx.owner) {
      iter->second.remove(conn_ctx);
      if (iter->second.empty()) {
        pubsub_shard_channels_[slot].erase(iter);
      }
      break;
    }
  }
}

void Server::GetSChannelsByPattern(const std::string &pattern, std::vector<std::string> *channels) {
  std::lock_guard<std::mutex> guard(pubsub_shard_channels_mu_);

  for (const auto &shard_channels : pubsub_shard_channels_) {
    for (const auto &iter : shard_channels) {
      if (pattern.empty() || util::StringMatch(pattern, iter.first, false)) {
        channels->emplace_back(iter.first);
      }
    }
  }
}

void Server::ListSChannelSubscribeNum(const std::vector<std::string> &channels,
                                      std::vector<ChannelSubscribeNum> *channel_subscribe_nums) {
  std::lock_guard<std::mutex> guard(pubsub_shard_channels_mu_);

  for (const auto &chan : channels) {
    uint16_t slot = config_->cluster_enabled ? GetSlotIdFromKey(chan) : 0;
    if (auto iter = pubsub_shard_channels_[slot].find(chan); iter != pubsub_shard_channels_[slot].end()) {
      channel_subscribe_nums->emplace_back(ChannelSubscribeNum{iter->first, iter->second.size()});
    } else {
      channel_subscribe_nums->emplace_back(ChannelSubscribeNum{chan, 0});
    }
  }
}

void Server::BlockOnKey(const std::string &key, redis::Connection *conn) {
  std::lock_guard<std::mutex> guard(blocking_keys_mu_);

  auto conn_ctx = ConnContext(conn->Owner(), conn->GetFD());

  if (auto iter = blocking_keys_.find(key); iter == blocking_keys_.end()) {
    blocking_keys_.emplace(key, std::list<ConnContext>{conn_ctx});
  } else {
    iter->second.emplace_back(conn_ctx);
  }

  IncrBlockedClientNum();
}

void Server::UnblockOnKey(const std::string &key, redis::Connection *conn) {
  std::lock_guard<std::mutex> guard(blocking_keys_mu_);

  auto iter = blocking_keys_.find(key);
  if (iter == blocking_keys_.end()) {
    return;
  }

  for (const auto &conn_ctx : iter->second) {
    if (conn->GetFD() == conn_ctx.fd && conn->Owner() == conn_ctx.owner) {
      iter->second.remove(conn_ctx);
      if (iter->second.empty()) {
        blocking_keys_.erase(iter);
      }
      break;
    }
  }

  DecrBlockedClientNum();
}

void Server::BlockOnStreams(const std::vector<std::string> &keys, const std::vector<redis::StreamEntryID> &entry_ids,
                            redis::Connection *conn) {
  std::lock_guard<std::mutex> guard(blocked_stream_consumers_mu_);

  IncrBlockedClientNum();

  for (size_t i = 0; i < keys.size(); ++i) {
    auto consumer = std::make_shared<StreamConsumer>(conn->Owner(), conn->GetFD(), conn->GetNamespace(), entry_ids[i]);
    if (auto iter = blocked_stream_consumers_.find(keys[i]); iter == blocked_stream_consumers_.end()) {
      std::set<std::shared_ptr<StreamConsumer>> consumers;
      consumers.insert(consumer);
      blocked_stream_consumers_.emplace(keys[i], consumers);
    } else {
      iter->second.insert(consumer);
    }
  }
}

void Server::UnblockOnStreams(const std::vector<std::string> &keys, redis::Connection *conn) {
  std::lock_guard<std::mutex> guard(blocked_stream_consumers_mu_);

  DecrBlockedClientNum();

  for (const auto &key : keys) {
    auto iter = blocked_stream_consumers_.find(key);
    if (iter == blocked_stream_consumers_.end()) {
      continue;
    }

    for (auto it = iter->second.begin(); it != iter->second.end();) {
      const auto &consumer = *it;
      if (conn->GetFD() == consumer->fd && conn->Owner() == consumer->owner) {
        iter->second.erase(it);
        if (iter->second.empty()) {
          blocked_stream_consumers_.erase(iter);
        }
        break;
      }
      ++it;
    }
  }
}

void Server::WakeupBlockingConns(const std::string &key, size_t n_conns) {
  std::lock_guard<std::mutex> guard(blocking_keys_mu_);

  auto iter = blocking_keys_.find(key);
  if (iter == blocking_keys_.end() || iter->second.empty()) {
    return;
  }

  while (n_conns-- && !iter->second.empty()) {
    auto conn_ctx = iter->second.front();
    auto s = conn_ctx.owner->EnableWriteEvent(conn_ctx.fd);
    if (!s.IsOK()) {
      LOG(ERROR) << "[server] Failed to enable write event on blocked client " << conn_ctx.fd << ": " << s.Msg();
    }
    iter->second.pop_front();
  }
}

void Server::OnEntryAddedToStream(const std::string &ns, const std::string &key, const redis::StreamEntryID &entry_id) {
  std::lock_guard<std::mutex> guard(blocked_stream_consumers_mu_);

  auto iter = blocked_stream_consumers_.find(key);
  if (iter == blocked_stream_consumers_.end() || iter->second.empty()) {
    return;
  }

  for (auto it = iter->second.begin(); it != iter->second.end();) {
    auto consumer = *it;
    if (consumer->ns == ns && entry_id > consumer->last_consumed_id) {
      auto s = consumer->owner->EnableWriteEvent(consumer->fd);
      if (!s.IsOK()) {
        LOG(ERROR) << "[server] Failed to enable write event on blocked stream consumer " << consumer->fd << ": "
                   << s.Msg();
      }
      it = iter->second.erase(it);
    } else {
      ++it;
    }
  }
}

void Server::updateCachedTime() { unix_time_secs.store(util::GetTimeStamp()); }

int Server::IncrClientNum() {
  total_clients_.fetch_add(1, std::memory_order_relaxed);
  return connected_clients_.fetch_add(1, std::memory_order_relaxed);
}

int Server::DecrClientNum() { return connected_clients_.fetch_sub(1, std::memory_order_relaxed); }

int Server::IncrMonitorClientNum() { return monitor_clients_.fetch_add(1, std::memory_order_relaxed); }

int Server::DecrMonitorClientNum() { return monitor_clients_.fetch_sub(1, std::memory_order_relaxed); }

int Server::IncrBlockedClientNum() { return blocked_clients_.fetch_add(1, std::memory_order_relaxed); }

int Server::DecrBlockedClientNum() { return blocked_clients_.fetch_sub(1, std::memory_order_relaxed); }

std::shared_lock<std::shared_mutex> Server::WorkConcurrencyGuard() {
  return std::shared_lock(works_concurrency_rw_lock_);
}

std::unique_lock<std::shared_mutex> Server::WorkExclusivityGuard() {
  return std::unique_lock(works_concurrency_rw_lock_);
}

uint64_t Server::GetClientID() { return client_id_.fetch_add(1, std::memory_order_relaxed); }

void Server::recordInstantaneousMetrics() {
  auto rocksdb_stats = storage->GetDB()->GetDBOptions().statistics;
  stats.TrackInstantaneousMetric(STATS_METRIC_COMMAND, stats.total_calls);
  stats.TrackInstantaneousMetric(STATS_METRIC_NET_INPUT, stats.in_bytes);
  stats.TrackInstantaneousMetric(STATS_METRIC_NET_OUTPUT, stats.out_bytes);
  stats.TrackInstantaneousMetric(STATS_METRIC_ROCKSDB_PUT,
                                 rocksdb_stats->getTickerCount(rocksdb::Tickers::NUMBER_KEYS_WRITTEN));
  stats.TrackInstantaneousMetric(STATS_METRIC_ROCKSDB_GET,
                                 rocksdb_stats->getTickerCount(rocksdb::Tickers::NUMBER_KEYS_READ));
  stats.TrackInstantaneousMetric(STATS_METRIC_ROCKSDB_MULTIGET,
                                 rocksdb_stats->getTickerCount(rocksdb::Tickers::NUMBER_MULTIGET_KEYS_READ));
  stats.TrackInstantaneousMetric(STATS_METRIC_ROCKSDB_SEEK,
                                 rocksdb_stats->getTickerCount(rocksdb::Tickers::NUMBER_DB_SEEK));
  stats.TrackInstantaneousMetric(STATS_METRIC_ROCKSDB_NEXT,
                                 rocksdb_stats->getTickerCount(rocksdb::Tickers::NUMBER_DB_NEXT));
  stats.TrackInstantaneousMetric(STATS_METRIC_ROCKSDB_PREV,
                                 rocksdb_stats->getTickerCount(rocksdb::Tickers::NUMBER_DB_PREV));
}

void Server::cron() {
  uint64_t counter = 0;
  while (!stop_) {
    // Sleep first
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // To guarantee accessing DB safely
    auto guard = storage->ReadLockGuard();
    if (storage->IsClosing()) continue;

    updateCachedTime();
    counter++;

    if (is_loading_) {
      // We need to skip the cron operations since `is_loading_` means the db is restoring,
      // and the db pointer will be modified after that. It will panic if we use the db pointer
      // before the new db was reopened.
      continue;
    }

    // check every 20s (use 20s instead of 60s so that cron will execute in critical condition)
    if (counter != 0 && counter % 200 == 0) {
      auto t = static_cast<time_t>(util::GetTimeStamp());
      std::tm now{};
      localtime_r(&t, &now);
      // disable compaction cron when the compaction checker was enabled
      if (!config_->compaction_checker_cron.IsEnabled() && config_->compact_cron.IsEnabled() &&
          config_->compact_cron.IsTimeMatch(&now)) {
        Status s = AsyncCompactDB();
        LOG(INFO) << "[server] Schedule to compact the db, result: " << s.Msg();
      }
      if (config_->bgsave_cron.IsEnabled() && config_->bgsave_cron.IsTimeMatch(&now)) {
        Status s = AsyncBgSaveDB();
        LOG(INFO) << "[server] Schedule to bgsave the db, result: " << s.Msg();
      }
      if (config_->dbsize_scan_cron.IsEnabled() && config_->dbsize_scan_cron.IsTimeMatch(&now)) {
        auto tokens = namespace_.List();
        std::vector<std::string> namespaces;

        // Number of namespaces (custom namespaces + default one)
        namespaces.reserve(tokens.size() + 1);
        for (auto &token : tokens) {
          namespaces.emplace_back(token.second);  // namespace
        }

        // add default namespace as fallback
        namespaces.emplace_back(kDefaultNamespace);

        for (auto &ns : namespaces) {
          Status s = AsyncScanDBSize(ns);
          LOG(INFO) << "[server] Schedule to recalculate the db size on namespace: " << ns << ", result: " << s.Msg();
        }
      }
    }
    // check every 10s
    if (counter != 0 && counter % 100 == 0) {
      Status s = AsyncPurgeOldBackups(config_->max_backup_to_keep, config_->max_backup_keep_hours);

      // Purge backup if needed, it will cost much disk space if we keep backup and full sync
      // checkpoints at the same time
      if (config_->purge_backup_on_fullsync && (storage->ExistCheckpoint() || storage->ExistSyncCheckpoint())) {
        s = AsyncPurgeOldBackups(0, 0);
      }
    }

    // No replica uses this checkpoint, we can remove it.
    if (counter != 0 && counter % 100 == 0) {
      int64_t create_time_secs = storage->GetCheckpointCreateTimeSecs();
      int64_t access_time_secs = storage->GetCheckpointAccessTimeSecs();

      if (storage->ExistCheckpoint()) {
        // TODO(shooterit): support to config the alive time of checkpoint
        int64_t now_secs = util::GetTimeStamp<std::chrono::seconds>();
        if ((GetFetchFileThreadNum() == 0 && now_secs - access_time_secs > 30) ||
            (now_secs - create_time_secs > 24 * 60 * 60)) {
          auto s = rocksdb::DestroyDB(config_->checkpoint_dir, rocksdb::Options());
          if (!s.ok()) {
            LOG(WARNING) << "[server] Fail to clean checkpoint, error: " << s.ToString();
          } else {
            LOG(INFO) << "[server] Clean checkpoint successfully";
          }
        }
      }
    }
    // check if DB need to be resumed every minute
    // Rocksdb has auto resume feature after retryable io error, earlier version(before v6.22.1) had
    // bug when encounter no space error. The current version fixes the no space error issue, but it
    // does not completely resolve, which still exists when encountered disk quota exceeded error.
    // In order to properly handle all possible situations on rocksdb, we manually resume here
    // when encountering no space error and disk quota exceeded error.
    if (counter != 0 && counter % 600 == 0 && storage->IsDBInRetryableIOError()) {
      auto s = storage->GetDB()->Resume();
      if (s.ok()) {
        LOG(WARNING) << "[server] Successfully resumed DB after retryable IO error";
      } else {
        LOG(ERROR) << "[server] Failed to resume DB after retryable IO error: " << s.ToString();
      }
      storage->SetDBInRetryableIOError(false);
    }

    // check if we need to clean up exited worker threads every 5s
    if (counter != 0 && counter % 50 == 0) {
      cleanupExitedWorkerThreads(false);
    }

    CleanupExitedSlaves();
    recordInstantaneousMetrics();
  }
}

Server::InfoEntries Server::GetRocksDBInfo() {
  InfoEntries entries;
  if (is_loading_) return entries;

  rocksdb::DB *db = storage->GetDB();

  uint64_t memtable_sizes = 0, cur_memtable_sizes = 0, num_snapshots = 0, num_running_flushes = 0;
  uint64_t num_immutable_tables = 0, memtable_flush_pending = 0, compaction_pending = 0;
  uint64_t num_running_compaction = 0, num_live_versions = 0, num_super_version = 0, num_background_errors = 0;

  db->GetAggregatedIntProperty("rocksdb.size-all-mem-tables", &memtable_sizes);
  db->GetAggregatedIntProperty("rocksdb.cur-size-all-mem-tables", &cur_memtable_sizes);
  db->GetAggregatedIntProperty("rocksdb.num-immutable-mem-table", &num_immutable_tables);
  db->GetAggregatedIntProperty("rocksdb.mem-table-flush-pending", &memtable_flush_pending);
  db->GetAggregatedIntProperty("rocksdb.current-super-version-number", &num_super_version);
  db->GetAggregatedIntProperty("rocksdb.background-errors", &num_background_errors);
  db->GetAggregatedIntProperty("rocksdb.compaction-pending", &compaction_pending);
  db->GetAggregatedIntProperty("rocksdb.num-live-versions", &num_live_versions);

  {
    // All column families share the same block cache, so it's good to count a single one.
    uint64_t block_cache_usage = 0;
    uint64_t block_cache_pinned_usage = 0;
    auto subkey_cf_handle = storage->GetCFHandle(ColumnFamilyID::PrimarySubkey);
    db->GetIntProperty(subkey_cf_handle, rocksdb::DB::Properties::kBlockCacheUsage, &block_cache_usage);
    entries.emplace_back("block_cache_usage", block_cache_usage);
    db->GetIntProperty(subkey_cf_handle, rocksdb::DB::Properties::kBlockCachePinnedUsage, &block_cache_pinned_usage);
    entries.emplace_back("block_cache_pinned_usage[" + subkey_cf_handle->GetName() + "]", block_cache_pinned_usage);

    // All column faimilies share the same property of the DB, so it's good to count a single one.
    db->GetIntProperty(subkey_cf_handle, rocksdb::DB::Properties::kNumSnapshots, &num_snapshots);
    db->GetIntProperty(subkey_cf_handle, rocksdb::DB::Properties::kNumRunningFlushes, &num_running_flushes);
    db->GetIntProperty(subkey_cf_handle, rocksdb::DB::Properties::kNumRunningCompactions, &num_running_compaction);
  }

  for (const auto &cf_handle : *storage->GetCFHandles()) {
    uint64_t estimate_keys = 0;
    uint64_t index_and_filter_cache_usage = 0;
    std::map<std::string, std::string> cf_stats_map;
    db->GetIntProperty(cf_handle, rocksdb::DB::Properties::kEstimateNumKeys, &estimate_keys);
    entries.emplace_back("estimate_keys[" + cf_handle->GetName() + "]", estimate_keys);
    db->GetIntProperty(cf_handle, rocksdb::DB::Properties::kEstimateTableReadersMem, &index_and_filter_cache_usage);
    entries.emplace_back("index_and_filter_cache_usage[" + cf_handle->GetName() + "]", index_and_filter_cache_usage);
    db->GetMapProperty(cf_handle, rocksdb::DB::Properties::kCFStats, &cf_stats_map);
    entries.emplace_back("level0_file_limit_slowdown[" + cf_handle->GetName() + "]",
                         cf_stats_map["l0-file-count-limit-delays"]);
    entries.emplace_back("level0_file_limit_stop[" + cf_handle->GetName() + "]",
                         cf_stats_map["l0-file-count-limit-stops"]);
    entries.emplace_back("pending_compaction_bytes_slowdown[" + cf_handle->GetName() + "]",
                         cf_stats_map["pending-compaction-bytes-delays"]);
    entries.emplace_back("pending_compaction_bytes_stop[" + cf_handle->GetName() + "]",
                         cf_stats_map["pending-compaction-bytes-stops"]);
    entries.emplace_back("level0_file_limit_stop_with_ongoing_compaction[" + cf_handle->GetName() + "]",
                         cf_stats_map["cf-l0-file-count-limit-stops-with-ongoing-compaction"]);
    entries.emplace_back("level0_file_limit_slowdown_with_ongoing_compaction[" + cf_handle->GetName() + "]",
                         cf_stats_map["cf-l0-file-count-limit-delays-with-ongoing-compaction"]);
    entries.emplace_back("memtable_count_limit_slowdown[" + cf_handle->GetName() + "]",
                         cf_stats_map["memtable-limit-delays"]);
    entries.emplace_back("memtable_count_limit_stop[" + cf_handle->GetName() + "]",
                         cf_stats_map["memtable-limit-stops"]);
  }

  auto rocksdb_stats = storage->GetDB()->GetDBOptions().statistics;
  if (rocksdb_stats) {
    std::map<std::string, uint32_t> block_cache_stats = {
        {"block_cache_hit", rocksdb::Tickers::BLOCK_CACHE_HIT},
        {"block_cache_index_hit", rocksdb::Tickers::BLOCK_CACHE_INDEX_HIT},
        {"block_cache_filter_hit", rocksdb::Tickers::BLOCK_CACHE_FILTER_HIT},
        {"block_cache_data_hit", rocksdb::Tickers::BLOCK_CACHE_DATA_HIT},
        {"block_cache_miss", rocksdb::Tickers::BLOCK_CACHE_MISS},
        {"block_cache_index_miss", rocksdb::Tickers::BLOCK_CACHE_INDEX_MISS},
        {"block_cache_filter_miss", rocksdb::Tickers::BLOCK_CACHE_FILTER_MISS},
        {"block_cache_data_miss", rocksdb::Tickers::BLOCK_CACHE_DATA_MISS},
    };
    for (const auto &iter : block_cache_stats) {
      entries.emplace_back(iter.first, rocksdb_stats->getTickerCount(iter.second));
    }
  }

  entries.emplace_back("all_mem_tables", memtable_sizes);
  entries.emplace_back("cur_mem_tables", cur_memtable_sizes);
  entries.emplace_back("snapshots", num_snapshots);
  entries.emplace_back("num_immutable_tables", num_immutable_tables);
  entries.emplace_back("num_running_flushes", num_running_flushes);
  entries.emplace_back("memtable_flush_pending", memtable_flush_pending);
  entries.emplace_back("compaction_pending", compaction_pending);
  entries.emplace_back("num_running_compactions", num_running_compaction);
  entries.emplace_back("num_live_versions", num_live_versions);
  entries.emplace_back("num_super_version", num_super_version);
  entries.emplace_back("num_background_errors", num_background_errors);
  auto db_stats = storage->GetDBStats();
  entries.emplace_back("flush_count", db_stats->flush_count.load());
  entries.emplace_back("compaction_count", db_stats->compaction_count.load());
  entries.emplace_back("put_per_sec", stats.GetInstantaneousMetric(STATS_METRIC_ROCKSDB_PUT));
  entries.emplace_back("get_per_sec", stats.GetInstantaneousMetric(STATS_METRIC_ROCKSDB_GET) +
                                          stats.GetInstantaneousMetric(STATS_METRIC_ROCKSDB_MULTIGET));
  entries.emplace_back("seek_per_sec", stats.GetInstantaneousMetric(STATS_METRIC_ROCKSDB_SEEK));
  entries.emplace_back("next_per_sec", stats.GetInstantaneousMetric(STATS_METRIC_ROCKSDB_NEXT));
  entries.emplace_back("prev_per_sec", stats.GetInstantaneousMetric(STATS_METRIC_ROCKSDB_PREV));
  db_job_mu_.lock();
  entries.emplace_back("is_bgsaving", (is_bgsave_in_progress_ ? "yes" : "no"));
  entries.emplace_back("is_compacting", (db_compacting_ ? "yes" : "no"));
  db_job_mu_.unlock();

  return entries;
}

Server::InfoEntries Server::GetServerInfo() {
  static int call_uname = 1;
  static utsname name;
  if (call_uname) {
    /* Uname can be slow and is always the same output. Cache it. */
    uname(&name);
    call_uname = 0;
  }

  Server::InfoEntries entries;
  entries.emplace_back("version", VERSION);  // deprecated
  entries.emplace_back("kvrocks_version", VERSION);
  entries.emplace_back("redis_version", REDIS_VERSION);
  entries.emplace_back("git_sha1", GIT_COMMIT);  // deprecated
  entries.emplace_back("kvrocks_git_sha1", GIT_COMMIT);
  entries.emplace_back("redis_mode", (config_->cluster_enabled ? "cluster" : "standalone"));
  entries.emplace_back("kvrocks_mode", (config_->cluster_enabled ? "cluster" : "standalone"));
  entries.emplace_back("os", fmt::format("{} {} {}", name.sysname, name.release, name.machine));
#if defined(__GNUC__) && !defined(__clang__)
  entries.emplace_back("gcc_version", fmt::format("{}.{}.{}", __GNUC__, __GNUC_MINOR__, __GNUC_PATCHLEVEL__));
#endif
#ifdef __clang__
  entries.emplace_back("clang_version",
                       fmt::format("{}.{}.{}", __clang_major__, __clang_minor__, __clang_patchlevel__));
#endif
  entries.emplace_back("rocksdb_version", fmt::format("{}.{}.{}", ROCKSDB_MAJOR, ROCKSDB_MINOR, ROCKSDB_PATCH));
  entries.emplace_back("arch_bits", sizeof(void *) * 8);
  entries.emplace_back("process_id", getpid());
  entries.emplace_back("tcp_port", config_->port);
  entries.emplace_back("server_time_usec", util::GetTimeStampUS());
  int64_t now_secs = util::GetTimeStamp<std::chrono::seconds>();
  entries.emplace_back("uptime_in_seconds", now_secs - start_time_secs_);
  entries.emplace_back("uptime_in_days", (now_secs - start_time_secs_) / 86400);
#ifdef __linux__
  if (auto exec_path = realpath("/proc/self/exe", nullptr)) {
    entries.emplace_back("executable", exec_path);
    free(exec_path);  // NOLINT(cppcoreguidelines-no-malloc)
  }
#endif
  entries.emplace_back("config_file", config_->ConfigFilePath());
  return entries;
}

Server::InfoEntries Server::GetClientsInfo() {
  InfoEntries entries;
  entries.emplace_back("maxclients", config_->maxclients);
  entries.emplace_back("connected_clients", connected_clients_.load());
  entries.emplace_back("monitor_clients", monitor_clients_.load());
  entries.emplace_back("blocked_clients", blocked_clients_.load());
  return entries;
}

Server::InfoEntries Server::GetMemoryInfo() {
  int64_t rss = Stats::GetMemoryRSS();
  int64_t memory_lua = 0;
  for (auto &wt : worker_threads_) {
    memory_lua += wt->GetWorker()->GetLuaMemorySize();
  }
  std::string used_memory_rss_human = util::BytesToHuman(rss);
  std::string used_memory_lua_human = util::BytesToHuman(memory_lua);

  InfoEntries entries;
  entries.emplace_back("used_memory_rss", rss);
  entries.emplace_back("used_memory_rss_human", used_memory_rss_human);
  entries.emplace_back("used_memory_lua", memory_lua);
  entries.emplace_back("used_memory_lua_human", used_memory_lua_human);
  entries.emplace_back("used_memory_startup", memory_startup_use_.load(std::memory_order_relaxed));
  return entries;
}

Server::InfoEntries Server::GetReplicationInfo() {
  InfoEntries entries;
  if (is_loading_) return entries;

  entries.emplace_back("role", (IsSlave() ? "slave" : "master"));
  if (IsSlave()) {
    int64_t now_secs = util::GetTimeStamp<std::chrono::seconds>();
    entries.emplace_back("master_host", master_host_);
    entries.emplace_back("master_port", master_port_);
    ReplState state = GetReplicationState();
    entries.emplace_back("master_link_status", (state == kReplConnected ? "up" : "down"));
    entries.emplace_back("master_sync_unrecoverable_error", (state == kReplError ? "yes" : "no"));
    entries.emplace_back("master_sync_in_progress", (state == kReplFetchMeta || state == kReplFetchSST));
    entries.emplace_back("master_last_io_seconds_ago", now_secs - replication_thread_->LastIOTimeSecs());
    entries.emplace_back("slave_repl_offset", storage->LatestSeqNumber());
    entries.emplace_back("slave_priority", config_->slave_priority);
  }

  int idx = 0;
  rocksdb::SequenceNumber latest_seq = storage->LatestSeqNumber();

  slave_threads_mu_.lock();
  entries.emplace_back("connected_slaves", slave_threads_.size());
  for (const auto &slave : slave_threads_) {
    if (slave->IsStopped()) continue;

    entries.emplace_back("slave" + std::to_string(idx),
                         fmt::format("ip={},port={},offset={},lag={}", slave->GetConn()->GetAnnounceIP(),
                                     slave->GetConn()->GetAnnouncePort(), slave->GetCurrentReplSeq(),
                                     latest_seq - slave->GetCurrentReplSeq()));
    ++idx;
  }
  slave_threads_mu_.unlock();

  entries.emplace_back("master_repl_offset", latest_seq);

  return entries;
}

std::string Server::GetRoleInfo() {
  if (IsSlave()) {
    std::vector<std::string> roles;
    roles.emplace_back("slave");
    roles.emplace_back(master_host_);
    roles.emplace_back(std::to_string(master_port_));

    auto state = GetReplicationState();
    if (state == kReplConnected) {
      roles.emplace_back("connected");
    } else if (state == kReplFetchMeta || state == kReplFetchSST) {
      roles.emplace_back("sync");
    } else {
      roles.emplace_back("connecting");
    }
    roles.emplace_back(std::to_string(storage->LatestSeqNumber()));
    return redis::ArrayOfBulkStrings(roles);
  } else {
    std::vector<std::string> list;

    slave_threads_mu_.lock();
    for (const auto &slave : slave_threads_) {
      if (slave->IsStopped()) continue;

      list.emplace_back(redis::ArrayOfBulkStrings({
          slave->GetConn()->GetAnnounceIP(),
          std::to_string(slave->GetConn()->GetListeningPort()),
          std::to_string(slave->GetCurrentReplSeq()),
      }));
    }
    slave_threads_mu_.unlock();

    auto multi_len = 2;
    if (list.size() > 0) {
      multi_len = 3;
    }
    std::string info;
    info.append(redis::MultiLen(multi_len));
    info.append(redis::BulkString("master"));
    info.append(redis::BulkString(std::to_string(storage->LatestSeqNumber())));
    if (list.size() > 0) {
      info.append(redis::Array(list));
    }
    return info;
  }
}

std::string Server::GetLastRandomKeyCursor() {
  std::string cursor;
  std::lock_guard<std::mutex> guard(last_random_key_cursor_mu_);
  cursor = last_random_key_cursor_;
  return cursor;
}

void Server::SetLastRandomKeyCursor(const std::string &cursor) {
  std::lock_guard<std::mutex> guard(last_random_key_cursor_mu_);
  last_random_key_cursor_ = cursor;
}

int64_t Server::GetCachedUnixTime() {
  if (unix_time_secs.load() == 0) {
    updateCachedTime();
  }
  return unix_time_secs.load();
}

int64_t Server::GetLastBgsaveTime() {
  std::lock_guard<std::mutex> lg(db_job_mu_);
  return last_bgsave_timestamp_secs_ == -1 ? start_time_secs_ : last_bgsave_timestamp_secs_;
}

Server::InfoEntries Server::GetStatsInfo() {
  Server::InfoEntries entries;
  entries.emplace_back("total_connections_received", total_clients_.load());
  entries.emplace_back("total_commands_processed", stats.total_calls.load());
  entries.emplace_back("instantaneous_ops_per_sec", stats.GetInstantaneousMetric(STATS_METRIC_COMMAND));
  entries.emplace_back("total_net_input_bytes", stats.in_bytes.load());
  entries.emplace_back("total_net_output_bytes", stats.out_bytes.load());
  entries.emplace_back("instantaneous_input_kbps",
                       static_cast<float>(stats.GetInstantaneousMetric(STATS_METRIC_NET_INPUT) / 1024));
  entries.emplace_back("instantaneous_output_kbps",
                       static_cast<float>(stats.GetInstantaneousMetric(STATS_METRIC_NET_OUTPUT) / 1024));
  entries.emplace_back("sync_full", stats.fullsync_count.load());
  entries.emplace_back("sync_partial_ok", stats.psync_ok_count.load());
  entries.emplace_back("sync_partial_err", stats.psync_err_count.load());

  auto db_stats = storage->GetDBStats();
  entries.emplace_back("keyspace_hits", db_stats->keyspace_hits.load());
  entries.emplace_back("keyspace_misses", db_stats->keyspace_misses.load());

  {
    std::lock_guard<std::mutex> lg(pubsub_channels_mu_);
    entries.emplace_back("pubsub_channels", pubsub_channels_.size());
    entries.emplace_back("pubsub_patterns", pubsub_patterns_.size());
  }

  return entries;
}

Server::InfoEntries Server::GetCommandsStatsInfo() {
  InfoEntries entries;

  for (const auto &cmd_stat : stats.commands_stats) {
    auto calls = cmd_stat.second.calls.load();
    if (calls == 0) continue;

    auto latency = cmd_stat.second.latency.load();
    entries.emplace_back("cmdstat_" + cmd_stat.first,
                         fmt::format("calls={},usec={},usec_per_call={}", calls, latency,
                                     static_cast<double>(latency) / static_cast<double>(calls)));
  }

  for (const auto &cmd_hist : stats.commands_histogram) {
    auto command_name = cmd_hist.first;
    auto calls = stats.commands_histogram[command_name].calls.load();
    if (calls == 0) continue;

    auto sum = stats.commands_histogram[command_name].sum.load();
    std::string result;
    for (std::size_t i{0}; i < stats.commands_histogram[command_name].buckets.size(); ++i) {
      auto bucket_value = stats.commands_histogram[command_name].buckets[i]->load();
      auto bucket_bound = std::numeric_limits<double>::infinity();
      if (i < stats.bucket_boundaries.size()) {
        bucket_bound = stats.bucket_boundaries[i];
      }

      result.append(fmt::format("{}={},", bucket_bound, bucket_value));
    }
    result.append(fmt::format("sum={},count={}", sum, calls));

    entries.emplace_back("cmdstathist_" + command_name, result);
  }

  return entries;
}

Server::InfoEntries Server::GetClusterInfo() {
  InfoEntries entries;

  entries.emplace_back("cluster_enabled", config_->cluster_enabled);

  return entries;
}

Server::InfoEntries Server::GetPersistenceInfo() {
  InfoEntries entries;

  entries.emplace_back("loading", is_loading_.load());

  std::lock_guard<std::mutex> lg(db_job_mu_);
  entries.emplace_back("bgsave_in_progress", is_bgsave_in_progress_);
  entries.emplace_back("last_bgsave_time",
                       (last_bgsave_timestamp_secs_ == -1 ? start_time_secs_ : last_bgsave_timestamp_secs_));
  entries.emplace_back("last_bgsave_status", last_bgsave_status_);
  entries.emplace_back("last_bgsave_time_sec", last_bgsave_duration_secs_);

  return entries;
}

Server::InfoEntries Server::GetCpuInfo() {  // NOLINT(readability-convert-member-functions-to-static)
  InfoEntries entries;

  rusage self_ru;
  getrusage(RUSAGE_SELF, &self_ru);
  entries.emplace_back("used_cpu_sys", static_cast<float>(self_ru.ru_stime.tv_sec) +
                                           static_cast<float>(self_ru.ru_stime.tv_usec / 1000000));
  entries.emplace_back("used_cpu_user", static_cast<float>(self_ru.ru_utime.tv_sec) +
                                            static_cast<float>(self_ru.ru_utime.tv_usec / 1000000));

  return entries;
}

Server::InfoEntries Server::GetKeyspaceInfo(const std::string &ns) {
  InfoEntries entries;
  if (is_loading_) return entries;

  KeyNumStats stats;
  GetLatestKeyNumStats(ns, &stats);
  auto last_dbsize_scan_timestamp = static_cast<time_t>(GetLastScanTime(ns));

  entries.emplace_back("last_dbsize_scan_timestamp", last_dbsize_scan_timestamp);
  entries.emplace_back("db0", fmt::format("keys={},expires={},avg_ttl={},expired={}", stats.n_key, stats.n_expires,
                                          stats.avg_ttl, stats.n_expired));
  entries.emplace_back("sequence", storage->GetDB()->GetLatestSequenceNumber());
  entries.emplace_back("used_db_size", storage->GetTotalSize(ns));
  entries.emplace_back("max_db_size", config_->max_db_size * GiB);
  double used_percent = config_->max_db_size ? static_cast<double>(storage->GetTotalSize() * 100) /
                                                   static_cast<double>(config_->max_db_size * GiB)
                                             : 0;
  entries.emplace_back("used_percent", fmt::format("{}%", used_percent));

  struct statvfs stat;
  if (statvfs(config_->db_dir.c_str(), &stat) == 0) {
    auto disk_capacity = stat.f_blocks * stat.f_frsize;
    auto used_disk_size = (stat.f_blocks - stat.f_bavail) * stat.f_frsize;
    entries.emplace_back("disk_capacity", disk_capacity);
    entries.emplace_back("used_disk_size", used_disk_size);
    double used_disk_percent = static_cast<double>(used_disk_size * 100) / static_cast<double>(disk_capacity);
    entries.emplace_back("used_disk_percent", fmt::format("{}%", used_disk_percent));
  }

  return entries;
}

// WARNING: we must not access DB(i.e. RocksDB) when server is loading since
// DB is closed and the pointer is invalid. Server may crash if we access DB during loading.
// If you add new fields which access DB into INFO command output, make sure
// this section can't be shown when loading(i.e. !is_loading_).
std::string Server::GetInfo(const std::string &ns, const std::vector<std::string> &sections) {
  std::vector<std::pair<std::string, std::function<InfoEntries(Server *)>>> info_funcs = {
      {"Server", &Server::GetServerInfo},   {"Clients", &Server::GetClientsInfo},
      {"Memory", &Server::GetMemoryInfo},   {"Persistence", &Server::GetPersistenceInfo},
      {"Stats", &Server::GetStatsInfo},     {"Replication", &Server::GetReplicationInfo},
      {"CPU", &Server::GetCpuInfo},         {"CommandStats", &Server::GetCommandsStatsInfo},
      {"Cluster", &Server::GetClusterInfo}, {"Keyspace", [&ns](Server *srv) { return srv->GetKeyspaceInfo(ns); }},
      {"RocksDB", &Server::GetRocksDBInfo},
  };

  std::string info_str;

  bool all = sections.empty() || util::FindICase(sections.begin(), sections.end(), "all") != sections.end();

  bool first = true;
  for (const auto &[sec, fn] : info_funcs) {
    if (all || util::FindICase(sections.begin(), sections.end(), sec) != sections.end()) {
      if (first)
        first = false;
      else
        info_str.append("\r\n");

      info_str.append("# " + sec + "\r\n");

      for (const auto &entry : fn(this)) {
        info_str.append(fmt::format("{}:{}\r\n", entry.name, entry.val));
      }
    }
  }

  return info_str;
}

std::string Server::GetRocksDBStatsJson() const {
  jsoncons::json stats_json;

  auto stats = storage->GetDB()->GetDBOptions().statistics;
  for (const auto &iter : rocksdb::TickersNameMap) {
    stats_json[iter.second] = stats->getTickerCount(iter.first);
  }

  for (const auto &iter : rocksdb::HistogramsNameMap) {
    rocksdb::HistogramData hist_data;
    stats->histogramData(iter.first, &hist_data);
    /* P50 P95 P99 P100 COUNT SUM */
    stats_json[iter.second] =
        jsoncons::json(jsoncons::json_array_arg, {hist_data.median, hist_data.percentile95, hist_data.percentile99,
                                                  hist_data.max, hist_data.count, hist_data.sum});
  }

  return stats_json.to_string();
}

// This function is called by replication thread when finished fetching all files from its master.
// Before restoring the db from backup or checkpoint, we should
// guarantee other threads don't access DB and its column families, then close db.
bool Server::PrepareRestoreDB() {
  // Stop feeding slaves thread
  LOG(INFO) << "[server] Disconnecting slaves...";
  DisconnectSlaves();

  // If the DB is restored, the object 'db_' will be destroyed, but
  // 'db_' will be accessed in data migration task. To avoid wrong
  // accessing, data migration task should be stopped before restoring DB
  WaitNoMigrateProcessing();

  // Workers will disallow to run commands which may access DB, so we should
  // enable this flag to stop workers from running new commands. And wait for
  // the exclusive guard to be released to guarantee no worker is running.
  is_loading_ = true;

  // To guarantee work threads don't access DB, we should release 'ExclusivityGuard'
  // ASAP to avoid user can't receive responses for long time, because the following
  // 'CloseDB' may cost much time to acquire DB mutex.
  LOG(INFO) << "[server] Waiting workers for finishing executing commands...";
  while (!works_concurrency_rw_lock_.try_lock()) {
    if (replication_thread_->IsStopped()) {
      is_loading_ = false;
      return false;
    }
    usleep(1000);
  }
  works_concurrency_rw_lock_.unlock();

  // Stop task runner
  LOG(INFO) << "[server] Stopping the task runner and clear task queue...";
  task_runner_.Cancel();
  if (auto s = task_runner_.Join(); !s) {
    LOG(WARNING) << "[server] " << s.Msg();
  }

  // Cron thread, compaction checker thread, full synchronization thread
  // may always run in the background, we need to close db, so they don't actually work.
  LOG(INFO) << "[server] Waiting for closing DB...";
  storage->CloseDB();
  return true;
}

void Server::WaitNoMigrateProcessing() {
  if (config_->cluster_enabled) {
    LOG(INFO) << "[server] Waiting until no migration task is running...";
    slot_migrator->SetStopMigrationFlag(true);
    while (slot_migrator->GetCurrentSlotMigrationStage() != SlotMigrationStage::kNone) {
      usleep(500);
    }
  }
}

Status Server::AsyncCompactDB(const std::string &begin_key, const std::string &end_key) {
  if (is_loading_) {
    return {Status::NotOK, "loading in-progress"};
  }

  std::lock_guard<std::mutex> lg(db_job_mu_);
  if (db_compacting_) {
    return {Status::NotOK, "compact in-progress"};
  }

  db_compacting_ = true;

  return task_runner_.TryPublish([begin_key, end_key, this] {
    std::unique_ptr<Slice> begin = nullptr, end = nullptr;
    if (!begin_key.empty()) begin = std::make_unique<Slice>(begin_key);
    if (!end_key.empty()) end = std::make_unique<Slice>(end_key);

    auto s = storage->Compact(nullptr, begin.get(), end.get());
    if (!s.ok()) {
      LOG(ERROR) << "[task runner] Failed to do compaction: " << s.ToString();
    }

    std::lock_guard<std::mutex> lg(db_job_mu_);
    db_compacting_ = false;
  });
}

Status Server::AsyncBgSaveDB() {
  std::lock_guard<std::mutex> lg(db_job_mu_);
  if (is_bgsave_in_progress_) {
    return {Status::NotOK, "bgsave in-progress"};
  }

  is_bgsave_in_progress_ = true;

  return task_runner_.TryPublish([this] {
    auto start_bgsave_time_secs = util::GetTimeStamp<std::chrono::seconds>();
    Status s = storage->CreateBackup();
    auto stop_bgsave_time_secs = util::GetTimeStamp<std::chrono::seconds>();

    std::lock_guard<std::mutex> lg(db_job_mu_);
    is_bgsave_in_progress_ = false;
    last_bgsave_timestamp_secs_ = start_bgsave_time_secs;
    last_bgsave_status_ = s.IsOK() ? "ok" : "err";
    last_bgsave_duration_secs_ = stop_bgsave_time_secs - start_bgsave_time_secs;
  });
}

Status Server::AsyncPurgeOldBackups(uint32_t num_backups_to_keep, uint32_t backup_max_keep_hours) {
  return task_runner_.TryPublish([num_backups_to_keep, backup_max_keep_hours, this] {
    storage->PurgeOldBackups(num_backups_to_keep, backup_max_keep_hours);
  });
}

Status Server::AsyncScanDBSize(const std::string &ns) {
  std::lock_guard<std::mutex> lg(db_job_mu_);

  if (auto iter = db_scan_infos_.find(ns); iter == db_scan_infos_.end()) {
    db_scan_infos_[ns] = DBScanInfo{};
  }

  if (db_scan_infos_[ns].is_scanning) {
    return {Status::NotOK, "scanning the db now"};
  }

  db_scan_infos_[ns].is_scanning = true;

  return task_runner_.TryPublish([ns, this] {
    redis::Database db(storage, ns);

    KeyNumStats stats;
    engine::Context ctx(storage);
    auto s = db.GetKeyNumStats(ctx, "", &stats);
    if (!s.ok()) {
      LOG(ERROR) << "failed to retrieve key num stats: " << s.ToString();
    }

    std::lock_guard<std::mutex> lg(db_job_mu_);

    db_scan_infos_[ns].key_num_stats = stats;
    db_scan_infos_[ns].last_scan_time_secs = util::GetTimeStamp();
    db_scan_infos_[ns].is_scanning = false;
  });
}

Status Server::autoResizeBlockAndSST() {
  auto total_size = storage->GetTotalSize(kDefaultNamespace);
  uint64_t total_keys = 0, estimate_keys = 0;
  for (const auto &cf_handle : *storage->GetCFHandles()) {
    storage->GetDB()->GetIntProperty(cf_handle, "rocksdb.estimate-num-keys", &estimate_keys);
    total_keys += estimate_keys;
  }

  if (total_size == 0 || total_keys == 0) {
    return Status::OK();
  }

  auto average_kv_size = total_size / total_keys;
  int target_file_size_base = 0;
  int block_size = 0;
  if (average_kv_size > 512 * KiB) {
    target_file_size_base = 1024;
    block_size = 1 * MiB;
  } else if (average_kv_size > 256 * KiB) {
    target_file_size_base = 512;
    block_size = 512 * KiB;
  } else if (average_kv_size > 32 * KiB) {
    target_file_size_base = 256;
    block_size = 256 * KiB;
  } else if (average_kv_size > 1 * KiB) {
    target_file_size_base = 128;
    block_size = 32 * KiB;
  } else if (average_kv_size > 128) {
    target_file_size_base = 64;
    block_size = 8 * KiB;
  } else {
    target_file_size_base = 16;
    block_size = 2 * KiB;
  }

  if (target_file_size_base == config_->rocks_db.target_file_size_base &&
      target_file_size_base == config_->rocks_db.write_buffer_size && block_size == config_->rocks_db.block_size) {
    return Status::OK();
  }

  if (target_file_size_base != config_->rocks_db.target_file_size_base) {
    auto old_target_file_size_base = config_->rocks_db.target_file_size_base;
    auto s = config_->Set(this, "rocksdb.target_file_size_base", std::to_string(target_file_size_base));
    LOG(INFO) << "[server] Resize rocksdb.target_file_size_base from " << old_target_file_size_base << " to "
              << target_file_size_base << ", average_kv_size: " << average_kv_size << ", total_size: " << total_size
              << ", total_keys: " << total_keys << ", result: " << s.Msg();
    if (!s.IsOK()) {
      return s;
    }
  }

  if (target_file_size_base != config_->rocks_db.write_buffer_size) {
    auto old_write_buffer_size = config_->rocks_db.write_buffer_size;
    auto s = config_->Set(this, "rocksdb.write_buffer_size", std::to_string(target_file_size_base));
    LOG(INFO) << "[server] Resize rocksdb.write_buffer_size from " << old_write_buffer_size << " to "
              << target_file_size_base << ", average_kv_size: " << average_kv_size << ", total_size: " << total_size
              << ", total_keys: " << total_keys << ", result: " << s.Msg();
    if (!s.IsOK()) {
      return s;
    }
  }

  if (block_size != config_->rocks_db.block_size) {
    auto s = storage->SetOptionForAllColumnFamilies("table_factory.block_size", std::to_string(block_size));
    LOG(INFO) << "[server] Resize rocksdb.block_size from " << config_->rocks_db.block_size << " to " << block_size
              << ", average_kv_size: " << average_kv_size << ", total_size: " << total_size
              << ", total_keys: " << total_keys << ", result: " << s.Msg();
    if (!s.IsOK()) {
      return s;
    }

    config_->rocks_db.block_size = block_size;
  }

  auto s = config_->Rewrite(namespace_.List());
  LOG(INFO) << "[server] Rewrite config, result: " << s.Msg();

  return Status::OK();
}

void Server::GetLatestKeyNumStats(const std::string &ns, KeyNumStats *stats) {
  auto iter = db_scan_infos_.find(ns);
  if (iter != db_scan_infos_.end()) {
    std::lock_guard<std::mutex> lg(db_job_mu_);
    *stats = iter->second.key_num_stats;
  }
}

int64_t Server::GetLastScanTime(const std::string &ns) const {
  auto iter = db_scan_infos_.find(ns);
  if (iter != db_scan_infos_.end()) {
    return iter->second.last_scan_time_secs;
  }
  return 0;
}

StatusOr<std::vector<rocksdb::BatchResult>> Server::PollUpdates(uint64_t next_sequence, int64_t count,
                                                                bool is_strict) const {
  std::vector<rocksdb::BatchResult> batches;
  auto latest_sequence = storage->LatestSeqNumber();
  if (next_sequence == latest_sequence + 1) {
    // return empty result if there is no new updates
    return batches;
  } else if (next_sequence > latest_sequence + 1) {
    return {Status::NotOK, "next sequence is out of range"};
  }

  std::unique_ptr<rocksdb::TransactionLogIterator> iter;
  if (auto s = storage->GetWALIter(next_sequence, &iter); !s.IsOK()) return s;
  if (!iter) {
    return Status{Status::NotOK, "unable to get WAL iterator"};
  }

  for (int64_t i = 0; i < count && iter->Valid() && iter->status().ok(); ++i, iter->Next()) {
    // The first batch should have the same sequence number as the next sequence number
    // if it requires strictly matched.
    auto batch = iter->GetBatch();
    if (i == 0 && is_strict && batch.sequence != next_sequence) {
      return {Status::NotOK,
              fmt::format("mismatched sequence number, expected {} but got {}", next_sequence, batch.sequence)};
    }
    batches.emplace_back(std::move(batch));
  }
  return batches;
}

void Server::SlowlogPushEntryIfNeeded(const std::vector<std::string> *args, uint64_t duration,
                                      const redis::Connection *conn) {
  int64_t threshold = config_->slowlog_log_slower_than;
  if (threshold < 0 || static_cast<int64_t>(duration) < threshold) return;

  auto entry = std::make_unique<SlowEntry>();
  size_t argc = args->size() > kSlowLogMaxArgc ? kSlowLogMaxArgc : args->size();
  for (size_t i = 0; i < argc; i++) {
    if (argc != args->size() && i == argc - 1) {
      entry->args.emplace_back(fmt::format("... ({} more arguments)", args->size() - argc + 1));
      break;
    }

    if ((*args)[i].length() <= kSlowLogMaxString) {
      entry->args.emplace_back((*args)[i]);
    } else {
      entry->args.emplace_back(fmt::format("{}... ({} more bytes)", (*args)[i].substr(0, kSlowLogMaxString),
                                           (*args)[i].length() - kSlowLogMaxString));
    }
  }

  entry->duration = duration;
  entry->client_name = conn->GetName();
  entry->ip = conn->GetIP();
  entry->port = conn->GetPort();
  slow_log_.PushEntry(std::move(entry));
}

std::string Server::GetClientsStr() {
  std::string clients;
  for (const auto &t : worker_threads_) {
    clients.append(t->GetWorker()->GetClientsStr());
  }

  std::lock_guard<std::mutex> guard(slave_threads_mu_);

  for (const auto &st : slave_threads_) {
    clients.append(st->GetConn()->ToString());
  }

  return clients;
}

void Server::KillClient(int64_t *killed, const std::string &addr, uint64_t id, uint64_t type, bool skipme,
                        redis::Connection *conn) {
  *killed = 0;

  // Normal clients and pubsub clients
  for (const auto &t : worker_threads_) {
    int64_t killed_in_worker = 0;
    t->GetWorker()->KillClient(conn, id, addr, type, skipme, &killed_in_worker);
    *killed += killed_in_worker;
  }

  // Slave clients
  slave_threads_mu_.lock();
  for (const auto &st : slave_threads_) {
    if ((type & kTypeSlave) ||
        (!addr.empty() && (st->GetConn()->GetAddr() == addr || st->GetConn()->GetAnnounceAddr() == addr)) ||
        (id != 0 && st->GetConn()->GetID() == id)) {
      st->Stop();
      (*killed)++;
    }
  }
  slave_threads_mu_.unlock();

  // Master client
  if (IsSlave() &&
      (type & kTypeMaster || (!addr.empty() && addr == master_host_ + ":" + std::to_string(master_port_)))) {
    // Stop replication thread and start a new one to replicate
    if (auto s = AddMaster(master_host_, master_port_, true); !s.IsOK()) {
      LOG(ERROR) << "[server] Failed to add master " << master_host_ << ":" << master_port_
                 << " with error: " << s.Msg();
    }
    (*killed)++;
  }
}

ReplState Server::GetReplicationState() {
  std::lock_guard<std::mutex> guard(slaveof_mu_);
  if (IsSlave() && replication_thread_) {
    return replication_thread_->State();
  }
  return kReplConnecting;
}

StatusOr<std::unique_ptr<redis::Commander>> Server::LookupAndCreateCommand(const std::string &cmd_name) {
  if (cmd_name.empty()) return {Status::RedisUnknownCmd};

  auto commands = redis::CommandTable::Get();
  auto cmd_iter = commands->find(util::ToLower(cmd_name));
  if (cmd_iter == commands->end()) {
    return {Status::RedisUnknownCmd};
  }

  auto cmd_attr = cmd_iter->second;
  auto cmd = cmd_attr->factory();
  cmd->SetAttributes(cmd_attr);

  return std::move(cmd);
}

Status Server::ScriptExists(const std::string &sha) const {
  std::string body;
  return ScriptGet(sha, &body);
}

Status Server::ScriptGet(const std::string &sha, std::string *body) const {
  std::string func_name = engine::kLuaFuncSHAPrefix + sha;
  auto cf = storage->GetCFHandle(ColumnFamilyID::Propagate);
  engine::Context ctx(storage);
  auto s = storage->Get(ctx, ctx.GetReadOptions(), cf, func_name, body);
  if (!s.ok()) {
    return {s.IsNotFound() ? Status::NotFound : Status::NotOK, s.ToString()};
  }
  return Status::OK();
}

Status Server::ScriptSet(const std::string &sha, const std::string &body) const {
  std::string func_name = engine::kLuaFuncSHAPrefix + sha;
  engine::Context ctx(storage);
  return storage->WriteToPropagateCF(ctx, func_name, body);
}

Status Server::FunctionGetCode(const std::string &lib, std::string *code) const {
  std::string func_name = engine::kLuaLibCodePrefix + lib;
  auto cf = storage->GetCFHandle(ColumnFamilyID::Propagate);
  engine::Context ctx(storage);
  auto s = storage->Get(ctx, ctx.GetReadOptions(), cf, func_name, code);
  if (!s.ok()) {
    return {s.IsNotFound() ? Status::NotFound : Status::NotOK, s.ToString()};
  }
  return Status::OK();
}

Status Server::FunctionGetLib(const std::string &func, std::string *lib) const {
  std::string func_name = engine::kLuaFuncLibPrefix + func;
  auto cf = storage->GetCFHandle(ColumnFamilyID::Propagate);
  engine::Context ctx(storage);
  auto s = storage->Get(ctx, ctx.GetReadOptions(), cf, func_name, lib);
  if (!s.ok()) {
    return {s.IsNotFound() ? Status::NotFound : Status::NotOK, s.ToString()};
  }
  return Status::OK();
}

Status Server::FunctionSetCode(const std::string &lib, const std::string &code) const {
  std::string func_name = engine::kLuaLibCodePrefix + lib;
  engine::Context ctx(storage);
  return storage->WriteToPropagateCF(ctx, func_name, code);
}

Status Server::FunctionSetLib(const std::string &func, const std::string &lib) const {
  std::string func_name = engine::kLuaFuncLibPrefix + func;
  engine::Context ctx(storage);
  return storage->WriteToPropagateCF(ctx, func_name, lib);
}

void Server::ScriptReset() {
  for (auto &wt : worker_threads_) {
    wt->GetWorker()->LuaReset();
  }
}

Status Server::ScriptFlush() {
  auto cf = storage->GetCFHandle(ColumnFamilyID::Propagate);
  engine::Context ctx(storage);
  auto s = storage->FlushScripts(ctx, storage->DefaultWriteOptions(), cf);
  if (!s.ok()) return {Status::NotOK, s.ToString()};
  ScriptReset();
  return Status::OK();
}

// Generally, we store data into RocksDB and just replicate WAL instead of propagating
// commands. But sometimes, we need to update inner states or do special operations
// for specific commands, such as `script flush`.
// channel: we put the same function commands into one channel to handle uniformly
// tokens: the serialized commands
Status Server::Propagate(const std::string &channel, const std::vector<std::string> &tokens) const {
  std::string value = redis::MultiLen(tokens.size());
  for (const auto &iter : tokens) {
    value += redis::BulkString(iter);
  }
  engine::Context ctx(storage);
  return storage->WriteToPropagateCF(ctx, channel, value);
}

Status Server::ExecPropagateScriptCommand(const std::vector<std::string> &tokens) {
  auto subcommand = util::ToLower(tokens[1]);
  if (subcommand == "flush") {
    ScriptReset();
  }
  return Status::OK();
}

Status Server::ExecPropagatedCommand(const std::vector<std::string> &tokens) {
  if (tokens.empty()) return Status::OK();

  auto command = util::ToLower(tokens[0]);
  if (command == "script" && tokens.size() >= 2) {
    return ExecPropagateScriptCommand(tokens);
  }

  return Status::OK();
}

// AdjustOpenFilesLimit only try best to raise the max open files according to
// the max clients and RocksDB open file configuration. It also reserves a number
// of file descriptors(128) for extra operations of persistence, listening sockets,
// log files and so forth.
void Server::AdjustOpenFilesLimit() {
  const int min_reserved_fds = 128;
  auto rocksdb_max_open_file = static_cast<rlim_t>(config_->rocks_db.max_open_files);
  auto max_clients = static_cast<rlim_t>(config_->maxclients);
  auto max_files = max_clients + rocksdb_max_open_file + min_reserved_fds;

  rlimit limit;
  if (getrlimit(RLIMIT_NOFILE, &limit) == -1) {
    return;
  }

  rlim_t old_limit = limit.rlim_cur;
  // Set the max number of files only if the current limit is not enough
  if (old_limit >= max_files) {
    return;
  }

  int setrlimit_error = 0;
  rlim_t best_limit = max_files;

  while (best_limit > old_limit) {
    limit.rlim_cur = best_limit;
    limit.rlim_max = best_limit;
    if (setrlimit(RLIMIT_NOFILE, &limit) != -1) break;

    setrlimit_error = errno;

    rlim_t decr_step = 16;
    if (best_limit < decr_step) {
      best_limit = old_limit;
      break;
    }

    best_limit -= decr_step;
  }

  if (best_limit < old_limit) best_limit = old_limit;

  if (best_limit < max_files) {
    if (best_limit <= static_cast<int>(min_reserved_fds)) {
      LOG(WARNING) << "[server] Your current 'ulimit -n' of " << old_limit << " is not enough for the server to start."
                   << "Please increase your open file limit to at least " << max_files << ". Exiting.";
      exit(1);
    }

    LOG(WARNING) << "[server] You requested max clients of " << max_clients << " and RocksDB max open files of "
                 << rocksdb_max_open_file << " requiring at least " << max_files << " max file descriptors.";
    LOG(WARNING) << "[server] Server can't set maximum open files to " << max_files
                 << " because of OS error: " << strerror(setrlimit_error);
  } else {
    LOG(WARNING) << "[server] Increased maximum number of open files to " << max_files << " (it's originally set to "
                 << old_limit << ")";
  }
}

void Server::AdjustWorkerThreads() {
  auto new_worker_threads = static_cast<size_t>(config_->workers);
  if (new_worker_threads == worker_threads_.size()) {
    return;
  }
  size_t delta = 0;
  if (new_worker_threads > worker_threads_.size()) {
    delta = new_worker_threads - worker_threads_.size();
    increaseWorkerThreads(delta);
    LOG(INFO) << "[server] Increase worker threads from " << worker_threads_.size() << " to " << new_worker_threads;
    return;
  }

  delta = worker_threads_.size() - new_worker_threads;
  LOG(INFO) << "[server] Decrease worker threads from " << worker_threads_.size() << " to " << new_worker_threads;
  decreaseWorkerThreads(delta);
}

void Server::increaseWorkerThreads(size_t delta) {
  for (size_t i = 0; i < delta; i++) {
    auto worker = std::make_unique<Worker>(this, config_);
    auto worker_thread = std::make_unique<WorkerThread>(std::move(worker));
    worker_thread->Start();
    worker_threads_.emplace_back(std::move(worker_thread));
  }
}

void Server::decreaseWorkerThreads(size_t delta) {
  auto current_worker_threads = worker_threads_.size();
  DCHECK(current_worker_threads > delta);
  auto remain_worker_threads = current_worker_threads - delta;
  for (size_t i = remain_worker_threads; i < current_worker_threads; i++) {
    // Unix socket will be listening on the first worker,
    // so it MUST remove workers from the end of the vector.
    // Otherwise, the unix socket will be closed.
    auto worker_thread = std::move(worker_threads_.back());
    worker_threads_.pop_back();
    // Migrate connections to other workers before stopping the worker,
    // we use round-robin to choose the target worker here.
    auto connections = worker_thread->GetWorker()->GetConnections();
    for (const auto &iter : connections) {
      auto target_worker = worker_threads_[iter.first % remain_worker_threads]->GetWorker();
      worker_thread->GetWorker()->MigrateConnection(target_worker, iter.second);
    }
    worker_thread->Stop(10 /* graceful timeout */);
    // Don't join the worker thread here, because it may join itself.
    recycle_worker_threads_.push(std::move(worker_thread));
  }
}

void Server::cleanupExitedWorkerThreads(bool force) {
  std::unique_ptr<WorkerThread> worker_thread = nullptr;
  auto total = recycle_worker_threads_.unsafe_size();
  for (size_t i = 0; i < total; i++) {
    if (!recycle_worker_threads_.try_pop(worker_thread)) {
      break;
    }
    if (worker_thread->IsTerminated() || force) {
      worker_thread->Join();
      worker_thread.reset();
    } else {
      // Push the worker thread back to the queue if it's still running.
      recycle_worker_threads_.push(std::move(worker_thread));
    }
  }
}

std::string ServerLogData::Encode() const {
  if (type_ == kReplIdLog) {
    return std::string(1, kReplIdTag) + " " + content_;
  }
  return content_;
}

Status ServerLogData::Decode(const rocksdb::Slice &blob) {
  if (blob.size() == 0) {
    return {Status::NotOK};
  }

  const char *header = blob.data();
  // Only support `kReplIdTag` now
  if (*header == kReplIdTag && blob.size() == 2 + kReplIdLength) {
    type_ = kReplIdLog;
    content_ = std::string(blob.data() + 2, blob.size() - 2);
    return Status::OK();
  }
  return {Status::NotOK};
}

void Server::updateWatchedKeysFromRange(const std::vector<std::string> &args, const redis::CommandKeyRange &range) {
  std::shared_lock lock(watched_key_mutex_);

  for (size_t i = range.first_key; range.last_key > 0 ? i <= size_t(range.last_key) : i <= args.size() + range.last_key;
       i += range.key_step) {
    if (auto iter = watched_key_map_.find(args[i]); iter != watched_key_map_.end()) {
      for (auto *conn : iter->second) {
        conn->watched_keys_modified = true;
      }
    }
  }
}

void Server::updateAllWatchedKeys() {
  std::shared_lock lock(watched_key_mutex_);

  for (auto &[_, conn_map] : watched_key_map_) {
    for (auto *conn : conn_map) {
      conn->watched_keys_modified = true;
    }
  }
}

void Server::UpdateWatchedKeysFromArgs(const std::vector<std::string> &args, const redis::CommandAttributes &attr) {
  if ((attr.GenerateFlags(args) & redis::kCmdWrite) && watched_key_size_ > 0) {
    attr.ForEachKeyRange([this](const std::vector<std::string> &args,
                                redis::CommandKeyRange range) { updateWatchedKeysFromRange(args, range); },
                         args, [this](const std::vector<std::string> &) { updateAllWatchedKeys(); });
  }
}

void Server::UpdateWatchedKeysManually(const std::vector<std::string> &keys) {
  std::shared_lock lock(watched_key_mutex_);

  for (const auto &key : keys) {
    if (auto iter = watched_key_map_.find(key); iter != watched_key_map_.end()) {
      for (auto *conn : iter->second) {
        conn->watched_keys_modified = true;
      }
    }
  }
}

void Server::WatchKey(redis::Connection *conn, const std::vector<std::string> &keys) {
  std::unique_lock lock(watched_key_mutex_);

  for (const auto &key : keys) {
    if (auto iter = watched_key_map_.find(key); iter != watched_key_map_.end()) {
      iter->second.emplace(conn);
    } else {
      watched_key_map_.emplace(key, std::set<redis::Connection *>{conn});
    }

    conn->watched_keys.insert(key);
  }

  watched_key_size_ = watched_key_map_.size();
}

bool Server::IsWatchedKeysModified(redis::Connection *conn) { return conn->watched_keys_modified; }

void Server::ResetWatchedKeys(redis::Connection *conn) {
  if (watched_key_size_ != 0) {
    std::unique_lock lock(watched_key_mutex_);

    for (const auto &key : conn->watched_keys) {
      if (auto iter = watched_key_map_.find(key); iter != watched_key_map_.end()) {
        iter->second.erase(conn);

        if (iter->second.empty()) {
          watched_key_map_.erase(iter);
        }
      }
    }

    conn->watched_keys.clear();
    conn->watched_keys_modified = false;
    watched_key_size_ = watched_key_map_.size();
  }
}

std::list<std::pair<std::string, uint32_t>> Server::GetSlaveHostAndPort() {
  std::list<std::pair<std::string, uint32_t>> result;
  slave_threads_mu_.lock();
  for (const auto &slave : slave_threads_) {
    if (slave->IsStopped()) continue;
    std::pair<std::string, int> host_port_pair = {slave->GetConn()->GetAnnounceIP(),
                                                  slave->GetConn()->GetListeningPort()};
    result.emplace_back(host_port_pair);
  }
  slave_threads_mu_.unlock();
  return result;
}

// The numeric cursor consists of a 16-bit counter, a 16-bit time stamp, a 29-bit hash,and a 3-bit cursor type. The
// hash is used to prevent information leakage. The time_stamp is used to prevent the generation of the same cursor in
// the extremely short period before and after a restart.
NumberCursor::NumberCursor(CursorType cursor_type, uint16_t counter, const std::string &key_name) {
  auto hash = static_cast<uint32_t>(std::hash<std::string>{}(key_name));
  auto time_stamp = static_cast<uint16_t>(util::GetTimeStamp());
  // make hash top 3-bit zero
  constexpr uint64_t hash_mask = 0x1FFFFFFFFFFFFFFF;
  cursor_ = static_cast<uint64_t>(counter) | static_cast<uint64_t>(time_stamp) << 16 |
            (static_cast<uint64_t>(hash) << 32 & hash_mask) | static_cast<uint64_t>(cursor_type) << 61;
}

bool NumberCursor::IsMatch(const CursorDictElement &element, CursorType cursor_type) const {
  return cursor_ == element.cursor.cursor_ && cursor_type == getCursorType();
}

std::string Server::GenerateCursorFromKeyName(const std::string &key_name, CursorType cursor_type, const char *prefix) {
  if (!config_->redis_cursor_compatible) {
    // add prefix for SCAN
    return prefix + key_name;
  }
  auto counter = cursor_counter_.fetch_add(1);
  auto number_cursor = NumberCursor(cursor_type, counter, key_name);
  cursor_dict_->at(number_cursor.GetIndex()) = {number_cursor, key_name};
  return number_cursor.ToString();
}

std::string Server::GetKeyNameFromCursor(const std::string &cursor, CursorType cursor_type) {
  // When cursor is 0, cursor string is empty
  if (cursor.empty() || !config_->redis_cursor_compatible) {
    return cursor;
  }

  auto s = ParseInt<uint64_t>(cursor, 10);
  // When Cursor 0 or not a Integer return empty string.
  // Although the parameter 'cursor' is not expected to be 0, we still added a check for 0 to increase the robustness of
  // the code.
  if (!s.IsOK() || *s == 0) {
    return {};
  }
  auto number_cursor = NumberCursor(*s);
  // Because the index information is fully stored in the cursor, we can directly obtain the index from the cursor.
  auto item = cursor_dict_->at(number_cursor.GetIndex());
  if (number_cursor.IsMatch(item, cursor_type)) {
    return item.key_name;
  }

  return {};
}

AuthResult Server::AuthenticateUser(const std::string &user_password, std::string *ns) {
  const auto &requirepass = GetConfig()->requirepass;
  if (requirepass.empty()) {
    return AuthResult::NO_REQUIRE_PASS;
  }

  auto get_ns = GetNamespace()->GetByToken(user_password);
  if (get_ns.IsOK()) {
    *ns = get_ns.GetValue();
    return AuthResult::IS_USER;
  }

  if (user_password != requirepass) {
    return AuthResult::INVALID_PASSWORD;
  }
  *ns = kDefaultNamespace;
  return AuthResult::IS_ADMIN;
}
