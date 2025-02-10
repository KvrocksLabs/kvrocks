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

#pragma once

#include <rocksdb/options.h>
#include <sys/resource.h>

#include <map>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "config_type.h"
#include "cron.h"
#include "status.h"
#include "storage/redis_metadata.h"

// forward declaration
class Server;
enum class MigrationType;
namespace engine {
class Storage;
}

constexpr const uint32_t PORT_LIMIT = 65535;

enum SupervisedMode { kSupervisedNone = 0, kSupervisedAutoDetect, kSupervisedSystemd, kSupervisedUpStart };

constexpr const char *TLS_AUTH_CLIENTS_NO = "no";
constexpr const char *TLS_AUTH_CLIENTS_OPTIONAL = "optional";

constexpr const size_t KiB = 1024L;
constexpr const size_t MiB = 1024L * KiB;
constexpr const size_t GiB = 1024L * MiB;
constexpr const uint32_t kDefaultPort = 6666;

constexpr const char *kDefaultNamespace = "__namespace";
constexpr int KVROCKS_MAX_LSM_LEVEL = 7;

enum class BlockCacheType { kCacheTypeLRU = 0, kCacheTypeHCC };

struct CLIOptions {
  std::string conf_file;
  std::vector<std::pair<std::string, std::string>> cli_options;

  CLIOptions() = default;
  explicit CLIOptions(std::string_view file) : conf_file(file) {}
};

struct Config {
 public:
  Config();
  ~Config() = default;
  uint32_t port = 0;
  int socket_fd = -1;

  uint32_t tls_port = 0;
  std::string tls_cert_file;
  std::string tls_key_file;
  std::string tls_key_file_pass;
  std::string tls_ca_cert_file;
  std::string tls_ca_cert_dir;
  std::string tls_auth_clients;
  bool tls_prefer_server_ciphers = false;
  std::string tls_ciphers;
  std::string tls_ciphersuites;
  std::string tls_protocols;
  bool tls_session_caching = true;
  int tls_session_cache_size = 1024 * 20;
  int tls_session_cache_timeout = 300;
  bool tls_replication = false;

  int workers = 0;
  int timeout = 0;
  int log_level = 0;
  int backlog = 511;
  int maxclients = 10000;
  int max_backup_to_keep = 1;
  int max_backup_keep_hours = 24;
  int slowlog_log_slower_than = 100000;
  int slowlog_max_len = 128;
  uint64_t proto_max_bulk_len = 512 * 1024 * 1024;
  bool daemonize = false;
  SupervisedMode supervised_mode = kSupervisedNone;
  bool slave_readonly = true;
  bool slave_serve_stale_data = true;
  bool slave_empty_db_before_fullsync = false;
  int slave_priority = 100;
  int replication_connect_timeout_ms = 3100;
  int replication_recv_timeout_ms = 3200;
  int max_db_size = 0;
  int max_replication_mb = 0;
  int max_io_mb = 0;
  int max_bitmap_to_string_mb = 16;
  bool master_use_repl_port = false;
  bool purge_backup_on_fullsync = false;
  bool auto_resize_block_and_sst = true;
  int fullsync_recv_file_delay = 0;
  bool use_rsid_psync = false;
  std::vector<std::string> binds;
  std::string dir;
  std::string db_dir;
  std::string backup_dir;  // GUARD_BY(backup_mu_)
  std::string pidfile;
  std::string backup_sync_dir;
  std::string checkpoint_dir;
  std::string sync_checkpoint_dir;
  std::string log_dir;
  std::string db_name;
  std::string masterauth;
  std::string requirepass;
  std::string master_host;
  std::string unixsocket;
  int unixsocketperm = 0777;
  uint32_t master_port = 0;
  Cron compact_cron;
  Cron bgsave_cron;
  Cron dbsize_scan_cron;
  Cron compaction_checker_cron;
  int64_t force_compact_file_age;
  int force_compact_file_min_deleted_percentage;
  bool repl_namespace_enabled = false;
  std::string replica_announce_ip;
  uint32_t replica_announce_port = 0;

  bool persist_cluster_nodes_enabled = true;
  bool slot_id_encoded = false;
  bool cluster_enabled = false;

  int migrate_speed;
  int pipeline_size;
  int sequence_gap;
  MigrationType migrate_type;
  int migrate_batch_size_kb;
  int migrate_batch_rate_limit_mb;

  bool redis_cursor_compatible = false;
  bool resp3_enabled = false;
  int log_retention_days;

  // load_tokens is used to buffer the tokens when loading,
  // don't use it to authenticate or rewrite the configuration file.
  std::map<std::string, std::string> load_tokens;

  // profiling
  int profiling_sample_ratio = 0;
  int profiling_sample_record_threshold_ms = 0;
  int profiling_sample_record_max_len = 128;
  std::set<std::string> profiling_sample_commands;
  bool profiling_sample_all_commands = false;

  // json
  int json_max_nesting_depth = 1024;
  JsonStorageFormat json_storage_format = JsonStorageFormat::JSON;

  // Enable transactional mode in engine::Context
  bool txn_context_enabled = false;

  bool skip_block_cache_deallocation_on_close = false;

  std::vector<double> histogram_bucket_boundaries;

  struct RocksDB {
    int block_size;
    bool cache_index_and_filter_blocks;
    int block_cache_size;
    BlockCacheType block_cache_type;
    int metadata_block_cache_size;
    int subkey_block_cache_size;
    bool share_metadata_and_subkey_block_cache;
    int row_cache_size;
    int max_open_files;
    int write_buffer_size;
    int max_write_buffer_number;
    int max_background_compactions;
    int max_background_flushes;
    int max_subcompactions;
    int stats_dump_period_sec;
    bool enable_pipelined_write;
    int64_t delayed_write_rate;
    int compaction_readahead_size;
    int target_file_size_base;
    rocksdb::CompressionType wal_compression;
    int wal_ttl_seconds;
    int wal_size_limit_mb;
    int max_total_wal_size;
    bool dump_malloc_stats;
    int level0_slowdown_writes_trigger;
    int level0_stop_writes_trigger;
    int level0_file_num_compaction_trigger;
    rocksdb::CompressionType compression;
    int compression_start_level;
    int compression_level;
    bool disable_auto_compactions;
    bool enable_blob_files;
    int min_blob_size;
    int blob_file_size;
    bool enable_blob_garbage_collection;
    int blob_garbage_collection_age_cutoff;
    int max_bytes_for_level_base;
    int max_bytes_for_level_multiplier;
    bool level_compaction_dynamic_level_bytes;
    int max_background_jobs;
    bool rate_limiter_auto_tuned;
    bool avoid_unnecessary_blocking_io = true;
    bool partition_filters;

    struct WriteOptions {
      bool sync;
      bool disable_wal;
      bool no_slowdown;
      bool low_pri;
      bool memtable_insert_hint_per_batch;
      int write_batch_max_bytes;
    } write_options;

    struct ReadOptions {
      bool async_io;
    } read_options;
  } rocks_db;

  mutable std::mutex backup_mu;

  std::string NodesFilePath() const;
  Status Rewrite(const std::map<std::string, std::string> &tokens);
  Status Load(const CLIOptions &path);
  void Get(const std::string &key, std::vector<std::string> *values) const;
  Status Set(Server *srv, std::string key, const std::string &value);
  void SetMaster(const std::string &host, uint32_t port);
  void ClearMaster();
  bool IsSlave() const { return !master_host.empty(); }
  bool HasConfigFile() const { return !path_.empty(); }
  std::string ConfigFilePath() const { return path_; }

 private:
  std::string path_;
  std::string binds_str_;
  std::string slaveof_;
  std::string compact_cron_str_;
  std::string bgsave_cron_str_;
  std::string dbsize_scan_cron_str_;
  std::string compaction_checker_range_str_;
  std::string compaction_checker_cron_str_;
  std::string profiling_sample_commands_str_;
  std::map<std::string, std::unique_ptr<ConfigField>> fields_;
  std::vector<std::string> rename_command_;
  std::string histogram_bucket_boundaries_str_;

  void initFieldValidator();
  void initFieldCallback();
  Status parseConfigFromPair(const std::pair<std::string, std::string> &input, int line_number);
  Status parseConfigFromString(const std::string &input, int line_number);
  bool checkFieldValueIsDefault(const std::string &key, const std::string &value) const;
  Status finish();
};
