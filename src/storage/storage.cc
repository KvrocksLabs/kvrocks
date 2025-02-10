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

#include "storage.h"

#include <event2/buffer.h>
#include <fcntl.h>
#include <glog/logging.h>
#include <rocksdb/convenience.h>
#include <rocksdb/env.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/rate_limiter.h>
#include <rocksdb/sst_file_manager.h>
#include <rocksdb/utilities/checkpoint.h>
#include <rocksdb/utilities/table_properties_collectors.h>

#include <algorithm>
#include <iostream>
#include <memory>
#include <random>

#include "compact_filter.h"
#include "db_util.h"
#include "event_listener.h"
#include "event_util.h"
#include "redis_db.h"
#include "redis_metadata.h"
#include "rocksdb/cache.h"
#include "rocksdb/options.h"
#include "rocksdb/write_batch.h"
#include "rocksdb_crc32c.h"
#include "server/server.h"
#include "storage/batch_indexer.h"
#include "table_properties_collector.h"
#include "time_util.h"
#include "unique_fd.h"

namespace engine {

constexpr const char *kReplicationIdKey = "replication_id_";

// used in creating rocksdb::LRUCache, set `num_shard_bits` to -1 means let rocksdb choose a good default shard count
// based on the capacity and the implementation.
constexpr int kRocksdbLRUAutoAdjustShardBits = -1;

// used as the default argument for `strict_capacity_limit` in creating rocksdb::Cache.
constexpr bool kRocksdbCacheStrictCapacityLimit = false;

// used as the default argument for `high_pri_pool_ratio` in creating block cache.
constexpr double kRocksdbLRUBlockCacheHighPriPoolRatio = 0.75;

// used as the default argument for `high_pri_pool_ratio` in creating row cache.
constexpr double kRocksdbLRURowCacheHighPriPoolRatio = 0.5;

// used in creating rocksdb::HyperClockCache, set`estimated_entry_charge` to 0 means let rocksdb dynamically and
// automatically adjust the table size for the cache.
constexpr size_t kRockdbHCCAutoAdjustCharge = 0;

const int64_t kIORateLimitMaxMb = 1024000;

using rocksdb::Slice;

Storage::Storage(Config *config)
    : backup_creating_time_secs_(util::GetTimeStamp<std::chrono::seconds>()),
      env_(rocksdb::Env::Default()),
      config_(config),
      lock_mgr_(16),
      db_stats_(std::make_unique<DBStats>()) {
  Metadata::InitVersionCounter();
  SetWriteOptions(config->rocks_db.write_options);
}

Storage::~Storage() {
  DestroyBackup();
  CloseDB();
  TrySkipBlockCacheDeallocationOnClose();
}

void Storage::TrySkipBlockCacheDeallocationOnClose() {
  if (config_->skip_block_cache_deallocation_on_close) {
    shared_block_cache_->DisownData();
  }
}

void Storage::CloseDB() {
  auto guard = WriteLockGuard();
  if (!db_) return;

  db_closing_ = true;
  db_->SyncWAL();
  rocksdb::CancelAllBackgroundWork(db_.get(), true);
  for (auto handle : cf_handles_) db_->DestroyColumnFamilyHandle(handle);
  db_ = nullptr;
}

void Storage::SetWriteOptions(const Config::RocksDB::WriteOptions &config) {
  default_write_opts_.sync = config.sync;
  default_write_opts_.disableWAL = config.disable_wal;
  default_write_opts_.no_slowdown = config.no_slowdown;
  default_write_opts_.low_pri = config.low_pri;
  default_write_opts_.memtable_insert_hint_per_batch = config.memtable_insert_hint_per_batch;
}

rocksdb::ReadOptions Storage::DefaultScanOptions() const {
  rocksdb::ReadOptions read_options;
  read_options.fill_cache = false;
  read_options.async_io = config_->rocks_db.read_options.async_io;

  return read_options;
}

rocksdb::ReadOptions Storage::DefaultMultiGetOptions() const {
  rocksdb::ReadOptions read_options;
  read_options.async_io = config_->rocks_db.read_options.async_io;

  return read_options;
}

rocksdb::BlockBasedTableOptions Storage::InitTableOptions() {
  rocksdb::BlockBasedTableOptions table_options;
  table_options.format_version = 5;
  table_options.index_type = rocksdb::BlockBasedTableOptions::IndexType::kTwoLevelIndexSearch;
  table_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10, false));
  table_options.partition_filters = config_->rocks_db.partition_filters;
  table_options.optimize_filters_for_memory = true;
  table_options.metadata_block_size = 4096;
  table_options.data_block_index_type = rocksdb::BlockBasedTableOptions::DataBlockIndexType::kDataBlockBinaryAndHash;
  table_options.data_block_hash_table_util_ratio = 0.75;
  table_options.block_size = static_cast<size_t>(config_->rocks_db.block_size);
  return table_options;
}

void Storage::SetBlobDB(rocksdb::ColumnFamilyOptions *cf_options) {
  cf_options->enable_blob_files = config_->rocks_db.enable_blob_files;
  cf_options->min_blob_size = config_->rocks_db.min_blob_size;
  cf_options->blob_file_size = config_->rocks_db.blob_file_size;
  cf_options->blob_compression_type = config_->rocks_db.compression;
  cf_options->enable_blob_garbage_collection = config_->rocks_db.enable_blob_garbage_collection;
  // Use 100.0 to force converting blob_garbage_collection_age_cutoff to double
  cf_options->blob_garbage_collection_age_cutoff = config_->rocks_db.blob_garbage_collection_age_cutoff / 100.0;
}

rocksdb::Options Storage::InitRocksDBOptions() {
  rocksdb::Options options;
  options.create_if_missing = true;
  options.create_missing_column_families = true;
  // options.IncreaseParallelism(2);
  // NOTE: the overhead of statistics is 5%-10%, so it should be configurable in prod env
  // See: https://github.com/facebook/rocksdb/wiki/Statistics
  options.statistics = rocksdb::CreateDBStatistics();
  options.stats_dump_period_sec = config_->rocks_db.stats_dump_period_sec;
  options.max_open_files = config_->rocks_db.max_open_files;
  options.compaction_style = rocksdb::CompactionStyle::kCompactionStyleLevel;
  options.max_subcompactions = static_cast<uint32_t>(config_->rocks_db.max_subcompactions);
  options.max_background_flushes = config_->rocks_db.max_background_flushes;
  options.max_background_compactions = config_->rocks_db.max_background_compactions;
  options.max_write_buffer_number = config_->rocks_db.max_write_buffer_number;
  options.min_write_buffer_number_to_merge = 2;
  options.write_buffer_size = config_->rocks_db.write_buffer_size * MiB;
  options.num_levels = KVROCKS_MAX_LSM_LEVEL;
  options.compression_opts.level = config_->rocks_db.compression_level;
  options.compression_per_level.resize(options.num_levels);
  options.wal_compression = config_->rocks_db.wal_compression;
  for (int i = 0; i < options.num_levels; ++i) {
    if (i < config_->rocks_db.compression_start_level) {
      options.compression_per_level[i] = rocksdb::CompressionType::kNoCompression;
    } else {
      options.compression_per_level[i] = config_->rocks_db.compression;
    }
  }

  if (config_->rocks_db.row_cache_size) {
    options.row_cache = rocksdb::NewLRUCache(config_->rocks_db.row_cache_size * MiB, kRocksdbLRUAutoAdjustShardBits,
                                             kRocksdbCacheStrictCapacityLimit, kRocksdbLRURowCacheHighPriPoolRatio);
  }

  options.enable_pipelined_write = config_->rocks_db.enable_pipelined_write;
  options.target_file_size_base = config_->rocks_db.target_file_size_base * MiB;
  options.max_manifest_file_size = 64 * MiB;
  options.max_log_file_size = 256 * MiB;
  options.keep_log_file_num = 12;
  options.WAL_ttl_seconds = static_cast<uint64_t>(config_->rocks_db.wal_ttl_seconds);
  options.WAL_size_limit_MB = static_cast<uint64_t>(config_->rocks_db.wal_size_limit_mb);
  options.max_total_wal_size = static_cast<uint64_t>(config_->rocks_db.max_total_wal_size * MiB);
  options.listeners.emplace_back(new EventListener(this));
  options.dump_malloc_stats = config_->rocks_db.dump_malloc_stats;
  sst_file_manager_ = std::shared_ptr<rocksdb::SstFileManager>(rocksdb::NewSstFileManager(rocksdb::Env::Default()));
  options.sst_file_manager = sst_file_manager_;
  int64_t max_io_mb = kIORateLimitMaxMb;
  if (config_->max_io_mb > 0) max_io_mb = config_->max_io_mb;

  rate_limiter_ = std::shared_ptr<rocksdb::RateLimiter>(rocksdb::NewGenericRateLimiter(
      max_io_mb * static_cast<int64_t>(MiB), 100 * 1000, /* default */
      10,                                                /* default */
      rocksdb::RateLimiter::Mode::kWritesOnly, config_->rocks_db.rate_limiter_auto_tuned));

  options.rate_limiter = rate_limiter_;
  options.delayed_write_rate = static_cast<uint64_t>(config_->rocks_db.delayed_write_rate);
  options.compaction_readahead_size = static_cast<size_t>(config_->rocks_db.compaction_readahead_size);
  options.level0_slowdown_writes_trigger = config_->rocks_db.level0_slowdown_writes_trigger;
  options.level0_stop_writes_trigger = config_->rocks_db.level0_stop_writes_trigger;
  options.level0_file_num_compaction_trigger = config_->rocks_db.level0_file_num_compaction_trigger;
  options.max_bytes_for_level_base = config_->rocks_db.max_bytes_for_level_base;
  options.max_bytes_for_level_multiplier = config_->rocks_db.max_bytes_for_level_multiplier;
  options.level_compaction_dynamic_level_bytes = config_->rocks_db.level_compaction_dynamic_level_bytes;
  options.max_background_jobs = config_->rocks_db.max_background_jobs;

  // avoid blocking io on iteration
  // see https://github.com/facebook/rocksdb/wiki/IO#avoid-blocking-io
  options.avoid_unnecessary_blocking_io = config_->rocks_db.avoid_unnecessary_blocking_io;
  return options;
}

Status Storage::SetOptionForAllColumnFamilies(const std::string &key, const std::string &value) {
  for (auto &cf_handle : cf_handles_) {
    auto s = db_->SetOptions(cf_handle, {{key, value}});
    if (!s.ok()) return {Status::NotOK, s.ToString()};
  }
  return Status::OK();
}

Status Storage::SetDBOption(const std::string &key, const std::string &value) {
  auto s = db_->SetDBOptions({{key, value}});
  if (!s.ok()) return {Status::NotOK, s.ToString()};
  return Status::OK();
}

Status Storage::CreateColumnFamilies(const rocksdb::Options &options) {
  rocksdb::ColumnFamilyOptions cf_options(options);
  auto res = util::DBOpen(options, config_->db_dir);
  if (res) {
    std::vector<std::string> cf_names_except_default;
    for (const auto &cf : ColumnFamilyConfigs::ListColumnFamiliesWithoutDefault()) {
      cf_names_except_default.emplace_back(cf.Name());
    }
    std::vector<rocksdb::ColumnFamilyHandle *> cf_handles;
    auto s = (*res)->CreateColumnFamilies(cf_options, cf_names_except_default, &cf_handles);
    if (!s.ok()) {
      return {Status::DBOpenErr, s.ToString()};
    }

    for (auto handle : cf_handles) (*res)->DestroyColumnFamilyHandle(handle);
    (*res)->Close();
  } else {
    // We try to create column families by opening the database without column families.
    // If it's ok means we didn't create column families (cannot open without column families if created).
    // When goes wrong, we need to check whether it's caused by column families NOT being opened or not.
    // If the status message contains `Column families not opened` means that we have created the column
    // families, let's ignore the error.
    const char *not_opened_prefix = "Column families not opened";
    if (res.Msg().find(not_opened_prefix) != std::string::npos) {
      return Status::OK();
    }

    return std::move(res);
  }

  return Status::OK();
}

Status Storage::Open(DBOpenMode mode) {
  auto guard = WriteLockGuard();
  db_closing_ = false;

  bool cache_index_and_filter_blocks = config_->rocks_db.cache_index_and_filter_blocks;
  size_t block_cache_size = config_->rocks_db.block_cache_size * MiB;
  size_t metadata_block_cache_size = config_->rocks_db.metadata_block_cache_size * MiB;
  size_t subkey_block_cache_size = config_->rocks_db.subkey_block_cache_size * MiB;
  if (block_cache_size == 0) {
    block_cache_size = metadata_block_cache_size + subkey_block_cache_size;
  }

  rocksdb::Options options = InitRocksDBOptions();
  if (mode == kDBOpenModeDefault) {
    if (auto s = CreateColumnFamilies(options); !s.IsOK()) {
      return s.Prefixed("failed to create column families");
    }
  }

  if (config_->rocks_db.block_cache_type == BlockCacheType::kCacheTypeLRU) {
    shared_block_cache_ = rocksdb::NewLRUCache(block_cache_size, kRocksdbLRUAutoAdjustShardBits,
                                               kRocksdbCacheStrictCapacityLimit, kRocksdbLRUBlockCacheHighPriPoolRatio);
  } else {
    rocksdb::HyperClockCacheOptions hcc_cache_options(block_cache_size, kRockdbHCCAutoAdjustCharge);
    shared_block_cache_ = hcc_cache_options.MakeSharedCache();
  }

  rocksdb::BlockBasedTableOptions metadata_table_opts = InitTableOptions();
  metadata_table_opts.block_cache = shared_block_cache_;
  metadata_table_opts.pin_l0_filter_and_index_blocks_in_cache = true;
  metadata_table_opts.cache_index_and_filter_blocks = cache_index_and_filter_blocks;
  metadata_table_opts.cache_index_and_filter_blocks_with_high_priority = true;

  rocksdb::ColumnFamilyOptions metadata_opts(options);
  metadata_opts.table_factory.reset(rocksdb::NewBlockBasedTableFactory(metadata_table_opts));
  metadata_opts.compaction_filter_factory = std::make_shared<MetadataFilterFactory>(this);
  metadata_opts.disable_auto_compactions = config_->rocks_db.disable_auto_compactions;
  // Enable whole key bloom filter in memtable
  metadata_opts.memtable_whole_key_filtering = true;
  metadata_opts.memtable_prefix_bloom_size_ratio = 0.1;
  metadata_opts.table_properties_collector_factories.emplace_back(
      NewCompactOnExpiredTableCollectorFactory(std::string(kMetadataColumnFamilyName), 0.3));
  SetBlobDB(&metadata_opts);

  rocksdb::BlockBasedTableOptions subkey_table_opts = InitTableOptions();
  subkey_table_opts.block_cache = shared_block_cache_;
  subkey_table_opts.pin_l0_filter_and_index_blocks_in_cache = true;
  subkey_table_opts.cache_index_and_filter_blocks = cache_index_and_filter_blocks;
  subkey_table_opts.cache_index_and_filter_blocks_with_high_priority = true;
  rocksdb::ColumnFamilyOptions subkey_opts(options);
  subkey_opts.table_factory.reset(rocksdb::NewBlockBasedTableFactory(subkey_table_opts));
  subkey_opts.compaction_filter_factory = std::make_shared<SubKeyFilterFactory>(this);
  subkey_opts.disable_auto_compactions = config_->rocks_db.disable_auto_compactions;
  subkey_opts.table_properties_collector_factories.emplace_back(
      NewCompactOnExpiredTableCollectorFactory(std::string(kPrimarySubkeyColumnFamilyName), 0.3));
  SetBlobDB(&subkey_opts);

  rocksdb::BlockBasedTableOptions pubsub_table_opts = InitTableOptions();
  rocksdb::ColumnFamilyOptions pubsub_opts(options);
  pubsub_opts.table_factory.reset(rocksdb::NewBlockBasedTableFactory(pubsub_table_opts));
  pubsub_opts.compaction_filter_factory = std::make_shared<PubSubFilterFactory>();
  pubsub_opts.disable_auto_compactions = config_->rocks_db.disable_auto_compactions;
  SetBlobDB(&pubsub_opts);

  rocksdb::BlockBasedTableOptions propagate_table_opts = InitTableOptions();
  rocksdb::ColumnFamilyOptions propagate_opts(options);
  propagate_opts.table_factory.reset(rocksdb::NewBlockBasedTableFactory(propagate_table_opts));
  propagate_opts.compaction_filter_factory = std::make_shared<PropagateFilterFactory>();
  propagate_opts.disable_auto_compactions = config_->rocks_db.disable_auto_compactions;
  SetBlobDB(&propagate_opts);

  rocksdb::BlockBasedTableOptions search_table_opts = InitTableOptions();
  rocksdb::ColumnFamilyOptions search_opts(options);
  search_opts.table_factory.reset(rocksdb::NewBlockBasedTableFactory(search_table_opts));
  search_opts.compaction_filter_factory = std::make_shared<SearchFilterFactory>();
  search_opts.disable_auto_compactions = config_->rocks_db.disable_auto_compactions;
  SetBlobDB(&search_opts);

  std::vector<rocksdb::ColumnFamilyDescriptor> column_families;
  // Caution: don't change the order of column family, or the handle will be mismatched
  column_families.emplace_back(rocksdb::kDefaultColumnFamilyName, subkey_opts);
  column_families.emplace_back(std::string(kMetadataColumnFamilyName), metadata_opts);
  column_families.emplace_back(std::string(kSecondarySubkeyColumnFamilyName), subkey_opts);
  column_families.emplace_back(std::string(kPubSubColumnFamilyName), pubsub_opts);
  column_families.emplace_back(std::string(kPropagateColumnFamilyName), propagate_opts);
  column_families.emplace_back(std::string(kStreamColumnFamilyName), subkey_opts);
  column_families.emplace_back(std::string(kSearchColumnFamilyName), search_opts);

  std::vector<std::string> old_column_families;
  auto s = rocksdb::DB::ListColumnFamilies(options, config_->db_dir, &old_column_families);
  if (!s.ok()) return {Status::NotOK, s.ToString()};

  auto start = std::chrono::high_resolution_clock::now();
  switch (mode) {
    case DBOpenMode::kDBOpenModeDefault: {
      db_ = GET_OR_RET(util::DBOpen(options, config_->db_dir, column_families, &cf_handles_));
      break;
    }
    case DBOpenMode::kDBOpenModeForReadOnly: {
      db_ = GET_OR_RET(util::DBOpenForReadOnly(options, config_->db_dir, column_families, &cf_handles_));
      break;
    }
    case DBOpenMode::kDBOpenModeAsSecondaryInstance: {
      db_ = GET_OR_RET(
          util::DBOpenAsSecondaryInstance(options, config_->db_dir, config_->dir, column_families, &cf_handles_));
      break;
    }
    default:
      __builtin_unreachable();
  }
  auto end = std::chrono::high_resolution_clock::now();
  int64_t duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();

  if (!db_) {
    LOG(INFO) << "[storage] Failed to load the data from disk: " << duration << " ms";
    return {Status::DBOpenErr};
  }
  LOG(INFO) << "[storage] Success to load the data from disk: " << duration << " ms";

  return Status::OK();
}

Status Storage::CreateBackup(uint64_t *sequence_number) {
  LOG(INFO) << "[storage] Start to create new backup";
  std::lock_guard<std::mutex> lg(config_->backup_mu);
  std::string task_backup_dir = config_->backup_dir;

  std::string tmpdir = task_backup_dir + ".tmp";
  // Maybe there is a dirty tmp checkpoint, try to clean it
  rocksdb::DestroyDB(tmpdir, rocksdb::Options());

  // 1) Create checkpoint of rocksdb for backup
  rocksdb::Checkpoint *checkpoint = nullptr;
  rocksdb::Status s = rocksdb::Checkpoint::Create(db_.get(), &checkpoint);
  if (!s.ok()) {
    LOG(WARNING) << "Failed to create checkpoint object for backup. Error: " << s.ToString();
    return {Status::NotOK, s.ToString()};
  }

  std::unique_ptr<rocksdb::Checkpoint> checkpoint_guard(checkpoint);
  s = checkpoint->CreateCheckpoint(tmpdir, config_->rocks_db.write_buffer_size * MiB, sequence_number);
  if (!s.ok()) {
    LOG(WARNING) << "Failed to create checkpoint (snapshot) for backup. Error: " << s.ToString();
    return {Status::DBBackupErr, s.ToString()};
  }

  // 2) Rename tmp backup to real backup dir
  if (s = rocksdb::DestroyDB(task_backup_dir, rocksdb::Options()); !s.ok()) {
    LOG(WARNING) << "[storage] Failed to clean old backup. Error: " << s.ToString();
    return {Status::NotOK, s.ToString()};
  }

  if (s = env_->RenameFile(tmpdir, task_backup_dir); !s.ok()) {
    LOG(WARNING) << "[storage] Failed to rename tmp backup. Error: " << s.ToString();
    // Just try best effort
    if (s = rocksdb::DestroyDB(tmpdir, rocksdb::Options()); !s.ok()) {
      LOG(WARNING) << "[storage] Failed to clean tmp backup. Error: " << s.ToString();
    }

    return {Status::NotOK, s.ToString()};
  }

  // 'backup_mu_' can guarantee 'backup_creating_time_secs_' is thread-safe
  backup_creating_time_secs_ = util::GetTimeStamp<std::chrono::seconds>();

  LOG(INFO) << "[storage] Success to create new backup";
  return Status::OK();
}

void Storage::DestroyBackup() {
  if (!backup_) {
    return;
  }
  backup_->StopBackup();
  backup_ = nullptr;
}

Status Storage::RestoreFromBackup() {
  // TODO(@ruoshan): assert role to be slave
  // We must reopen the backup engine every time, as the files is changed
  rocksdb::BackupEngineOptions bk_option(config_->backup_sync_dir);
  auto bes = util::BackupEngineOpen(db_->GetEnv(), bk_option);
  if (!bes) return std::move(bes);

  backup_ = std::move(*bes);

  auto s = backup_->RestoreDBFromLatestBackup(config_->db_dir, config_->db_dir);
  if (!s.ok()) {
    LOG(ERROR) << "[storage] Failed to restore database from the latest backup. Error: " << s.ToString();
  } else {
    LOG(INFO) << "[storage] Database was restored from the latest backup";
  }

  // Reopen DB （should always try to reopen db even if restore failed, replication SST file CRC check may use it）
  auto s2 = Open();
  if (!s2.IsOK()) {
    LOG(ERROR) << "[storage] Failed to reopen the database. Error: " << s2.Msg();
    return {Status::DBOpenErr, s2.Msg()};
  }

  // Clean up backup engine
  backup_->PurgeOldBackups(0);
  DestroyBackup();

  return s.ok() ? Status::OK() : Status(Status::DBBackupErr, s.ToString());
}

Status Storage::RestoreFromCheckpoint() {
  std::string checkpoint_dir = config_->sync_checkpoint_dir;
  std::string tmp_dir = config_->db_dir + ".tmp";

  // Clean old backups and checkpoints because server will work on the new db
  PurgeOldBackups(0, 0);
  rocksdb::DestroyDB(config_->checkpoint_dir, rocksdb::Options());
  rocksdb::DestroyDB(tmp_dir, rocksdb::Options());

  // Maybe there is no database directory
  auto s = env_->CreateDirIfMissing(config_->db_dir);
  if (!s.ok()) {
    return {Status::NotOK,
            fmt::format("Failed to create database directory '{}'. Error: {}", config_->db_dir, s.ToString())};
  }

  // Rename database directory to tmp, so we can restore if replica fails to load the checkpoint from master.
  // But only try best effort to make data safe
  s = env_->RenameFile(config_->db_dir, tmp_dir);
  if (!s.ok()) {
    if (auto s1 = Open(); !s1.IsOK()) {
      LOG(ERROR) << "[storage] Failed to reopen database. Error: " << s1.Msg();
    }
    return {Status::NotOK, fmt::format("Failed to rename database directory '{}' to '{}'. Error: {}", config_->db_dir,
                                       tmp_dir, s.ToString())};
  }

  // Rename checkpoint directory to database directory
  if (s = env_->RenameFile(checkpoint_dir, config_->db_dir); !s.ok()) {
    env_->RenameFile(tmp_dir, config_->db_dir);
    if (auto s1 = Open(); !s1.IsOK()) {
      LOG(ERROR) << "[storage] Failed to reopen database. Error: " << s1.Msg();
    }
    return {Status::NotOK, fmt::format("Failed to rename checkpoint directory '{}' to '{}'. Error: {}", checkpoint_dir,
                                       config_->db_dir, s.ToString())};
  }

  // Open the new database, restore if replica fails to open
  auto s2 = Open();
  if (!s2.IsOK()) {
    LOG(WARNING) << "[storage] Failed to open master checkpoint. Error: " << s2.Msg();
    rocksdb::DestroyDB(config_->db_dir, rocksdb::Options());
    env_->RenameFile(tmp_dir, config_->db_dir);
    if (auto s1 = Open(); !s1.IsOK()) {
      LOG(ERROR) << "[storage] Failed to reopen database. Error: " << s1.Msg();
    }
    return {Status::DBOpenErr, "Failed to open master checkpoint. Error: " + s2.Msg()};
  }

  // Destroy the origin database
  if (s = rocksdb::DestroyDB(tmp_dir, rocksdb::Options()); !s.ok()) {
    LOG(WARNING) << "[storage] Failed to destroy the origin database at '" << tmp_dir << "'. Error: " << s.ToString();
  }
  return Status::OK();
}

bool Storage::IsEmptyDB() {
  std::unique_ptr<rocksdb::Iterator> iter(
      db_->NewIterator(DefaultScanOptions(), GetCFHandle(ColumnFamilyID::Metadata)));
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    Metadata metadata(kRedisNone, false);
    // If cannot decode the metadata we think the key is alive, so the db is not empty
    if (!metadata.Decode(iter->value()).ok() || !metadata.Expired()) {
      return false;
    }
  }
  return true;
}

void Storage::EmptyDB() {
  // Clean old backups and checkpoints
  PurgeOldBackups(0, 0);
  rocksdb::DestroyDB(config_->checkpoint_dir, rocksdb::Options());

  auto s = rocksdb::DestroyDB(config_->db_dir, rocksdb::Options());
  if (!s.ok()) {
    LOG(ERROR) << "[storage] Failed to destroy database. Error: " << s.ToString();
  }
}

void Storage::PurgeOldBackups(uint32_t num_backups_to_keep, uint32_t backup_max_keep_hours) {
  auto now_secs = util::GetTimeStamp<std::chrono::seconds>();
  std::lock_guard<std::mutex> lg(config_->backup_mu);
  std::string task_backup_dir = config_->backup_dir;

  // Return if there is no backup
  auto s = env_->FileExists(task_backup_dir);
  if (!s.ok()) return;

  // No backup is needed to keep or the backup is expired, we will clean it.
  bool backup_expired =
      (backup_max_keep_hours != 0 && backup_creating_time_secs_ + backup_max_keep_hours * 3600 < now_secs);
  if (num_backups_to_keep == 0 || backup_expired) {
    s = rocksdb::DestroyDB(task_backup_dir, rocksdb::Options());
    if (s.ok()) {
      LOG(INFO) << "[storage] Succeeded cleaning old backup that was created at " << backup_creating_time_secs_;
    } else {
      LOG(INFO) << "[storage] Failed cleaning old backup that was created at " << backup_creating_time_secs_
                << ". Error: " << s.ToString();
    }
  }
}

Status Storage::GetWALIter(rocksdb::SequenceNumber seq, std::unique_ptr<rocksdb::TransactionLogIterator> *iter) {
  auto s = db_->GetUpdatesSince(seq, iter);
  if (!s.ok()) return {Status::DBGetWALErr, s.ToString()};

  if (!(*iter)->Valid()) return {Status::DBGetWALErr, "iterator is not valid"};

  return Status::OK();
}

rocksdb::SequenceNumber Storage::LatestSeqNumber() { return db_->GetLatestSequenceNumber(); }

rocksdb::Status Storage::Get(engine::Context &ctx, const rocksdb::ReadOptions &options, const rocksdb::Slice &key,
                             std::string *value) {
  return Get(ctx, options, db_->DefaultColumnFamily(), key, value);
}

rocksdb::Status Storage::Get(engine::Context &ctx, const rocksdb::ReadOptions &options,
                             rocksdb::ColumnFamilyHandle *column_family, const rocksdb::Slice &key,
                             std::string *value) {
  if (ctx.txn_context_enabled) {
    DCHECK_NE(options.snapshot, nullptr);
    DCHECK_EQ(ctx.GetSnapshot()->GetSequenceNumber(), options.snapshot->GetSequenceNumber());
  }
  rocksdb::Status s;
  if (is_txn_mode_ && txn_write_batch_->GetWriteBatch()->Count() > 0) {
    s = txn_write_batch_->GetFromBatchAndDB(db_.get(), options, column_family, key, value);
  } else if (ctx.batch && ctx.txn_context_enabled) {
    s = ctx.batch->GetFromBatchAndDB(db_.get(), options, column_family, key, value);
  } else {
    s = db_->Get(options, column_family, key, value);
  }

  recordKeyspaceStat(column_family, s);
  return s;
}

rocksdb::Status Storage::Get(engine::Context &ctx, const rocksdb::ReadOptions &options, const rocksdb::Slice &key,
                             rocksdb::PinnableSlice *value) {
  return Get(ctx, options, db_->DefaultColumnFamily(), key, value);
}

rocksdb::Status Storage::Get(engine::Context &ctx, const rocksdb::ReadOptions &options,
                             rocksdb::ColumnFamilyHandle *column_family, const rocksdb::Slice &key,
                             rocksdb::PinnableSlice *value) {
  if (ctx.txn_context_enabled) {
    DCHECK_NE(options.snapshot, nullptr);
    DCHECK_EQ(ctx.GetSnapshot()->GetSequenceNumber(), options.snapshot->GetSequenceNumber());
  }
  rocksdb::Status s;
  if (is_txn_mode_ && txn_write_batch_->GetWriteBatch()->Count() > 0) {
    s = txn_write_batch_->GetFromBatchAndDB(db_.get(), options, column_family, key, value);
  } else if (ctx.txn_context_enabled && ctx.batch) {
    s = ctx.batch->GetFromBatchAndDB(db_.get(), options, column_family, key, value);
  } else {
    s = db_->Get(options, column_family, key, value);
  }

  recordKeyspaceStat(column_family, s);
  return s;
}

rocksdb::Iterator *Storage::NewIterator(engine::Context &ctx, const rocksdb::ReadOptions &options) {
  return NewIterator(ctx, options, db_->DefaultColumnFamily());
}

void Storage::recordKeyspaceStat(const rocksdb::ColumnFamilyHandle *column_family, const rocksdb::Status &s) {
  if (column_family->GetName() != kMetadataColumnFamilyName) return;

  // Don't record keyspace hits here because we cannot tell
  // if the key was expired or not. So we record it when parsing the metadata.
  if (s.IsNotFound() || s.IsInvalidArgument()) {
    RecordStat(StatType::KeyspaceMisses, 1);
  }
}

rocksdb::Iterator *Storage::NewIterator(engine::Context &ctx, const rocksdb::ReadOptions &options,
                                        rocksdb::ColumnFamilyHandle *column_family) {
  if (ctx.txn_context_enabled) {
    DCHECK_NE(options.snapshot, nullptr);
    DCHECK_EQ(ctx.GetSnapshot()->GetSequenceNumber(), options.snapshot->GetSequenceNumber());
  }
  auto iter = db_->NewIterator(options, column_family);
  if (is_txn_mode_ && txn_write_batch_->GetWriteBatch()->Count() > 0) {
    return txn_write_batch_->NewIteratorWithBase(column_family, iter, &options);
  } else if (ctx.txn_context_enabled && ctx.batch && ctx.batch->GetWriteBatch()->Count() > 0) {
    return ctx.batch->NewIteratorWithBase(column_family, iter, &options);
  }
  return iter;
}

void Storage::MultiGet(engine::Context &ctx, const rocksdb::ReadOptions &options,
                       rocksdb::ColumnFamilyHandle *column_family, const size_t num_keys, const rocksdb::Slice *keys,
                       rocksdb::PinnableSlice *values, rocksdb::Status *statuses) {
  if (ctx.txn_context_enabled) {
    DCHECK_NE(options.snapshot, nullptr);
    DCHECK_EQ(ctx.GetSnapshot()->GetSequenceNumber(), options.snapshot->GetSequenceNumber());
  }
  if (is_txn_mode_ && txn_write_batch_->GetWriteBatch()->Count() > 0) {
    txn_write_batch_->MultiGetFromBatchAndDB(db_.get(), options, column_family, num_keys, keys, values, statuses,
                                             false);
  } else if (ctx.txn_context_enabled && ctx.batch) {
    ctx.batch->MultiGetFromBatchAndDB(db_.get(), options, column_family, num_keys, keys, values, statuses, false);
  } else {
    db_->MultiGet(options, column_family, num_keys, keys, values, statuses, false);
  }

  for (size_t i = 0; i < num_keys; i++) {
    recordKeyspaceStat(column_family, statuses[i]);
  }
}

rocksdb::Status Storage::Write(engine::Context &ctx, const rocksdb::WriteOptions &options,
                               rocksdb::WriteBatch *updates) {
  if (is_txn_mode_) {
    // The batch won't be flushed until the transaction was committed or rollback
    return rocksdb::Status::OK();
  }
  return writeToDB(ctx, options, updates);
}

rocksdb::Status Storage::writeToDB(engine::Context &ctx, const rocksdb::WriteOptions &options,
                                   rocksdb::WriteBatch *updates) {
  // Put replication id logdata at the end of `updates`.
  if (replid_.length() == kReplIdLength) {
    updates->PutLogData(ServerLogData(kReplIdLog, replid_).Encode());
  }

  if (ctx.txn_context_enabled) {
    // Extract writes from the updates and append to the ctx.batch
    if (ctx.batch == nullptr) {
      ctx.batch = std::make_unique<rocksdb::WriteBatchWithIndex>();
    }
    WriteBatchIndexer handle(ctx);
    auto s = updates->Iterate(&handle);
    if (!s.ok()) return s;
  } else {
    DCHECK(ctx.batch == nullptr);
  }

  return db_->Write(options, updates);
}

rocksdb::Status Storage::Delete(engine::Context &ctx, const rocksdb::WriteOptions &options,
                                rocksdb::ColumnFamilyHandle *cf_handle, const rocksdb::Slice &key) {
  auto batch = GetWriteBatchBase();
  auto s = batch->Delete(cf_handle, key);
  if (!s.ok()) {
    return s;
  }
  return Write(ctx, options, batch->GetWriteBatch());
}

rocksdb::Status Storage::DeleteRange(engine::Context &ctx, const rocksdb::WriteOptions &options,
                                     rocksdb::ColumnFamilyHandle *cf_handle, Slice begin, Slice end) {
  auto batch = GetWriteBatchBase();
  auto s = batch->DeleteRange(cf_handle, begin, end);
  if (!s.ok()) {
    return s;
  }

  return Write(ctx, options, batch->GetWriteBatch());
}

rocksdb::Status Storage::DeleteRange(engine::Context &ctx, Slice begin, Slice end) {
  return DeleteRange(ctx, default_write_opts_, GetCFHandle(ColumnFamilyID::Metadata), begin, end);
}

rocksdb::Status Storage::FlushScripts(engine::Context &ctx, const rocksdb::WriteOptions &options,
                                      rocksdb::ColumnFamilyHandle *cf_handle) {
  std::string begin_key = kLuaFuncSHAPrefix, end_key = begin_key;
  // we need to increase one here since the DeleteRange api
  // didn't contain the end key.
  end_key[end_key.size() - 1] += 1;

  auto batch = GetWriteBatchBase();
  auto s = batch->DeleteRange(cf_handle, begin_key, end_key);
  if (!s.ok()) {
    return s;
  }

  return Write(ctx, options, batch->GetWriteBatch());
}

Status Storage::ReplicaApplyWriteBatch(rocksdb::WriteBatch *batch) {
  return applyWriteBatch(default_write_opts_, batch);
}

Status Storage::applyWriteBatch(const rocksdb::WriteOptions &options, rocksdb::WriteBatch *batch) {
  if (db_size_limit_reached_) {
    return {Status::NotOK, "reach space limit"};
  }
  auto s = db_->Write(options, batch);
  if (!s.ok()) {
    return {Status::NotOK, s.ToString()};
  }
  return Status::OK();
}

Status Storage::ApplyWriteBatch(const rocksdb::WriteOptions &options, std::string &&raw_batch) {
  auto batch = rocksdb::WriteBatch(std::move(raw_batch));
  return applyWriteBatch(options, &batch);
}

void Storage::RecordStat(StatType type, uint64_t v) {
  switch (type) {
    case StatType::FlushCount:
      db_stats_->flush_count.fetch_add(v, std::memory_order_relaxed);
      break;
    case StatType::CompactionCount:
      db_stats_->compaction_count.fetch_add(v, std::memory_order_relaxed);
      break;
    case StatType::KeyspaceHits:
      db_stats_->keyspace_hits.fetch_add(v, std::memory_order_relaxed);
      break;
    case StatType::KeyspaceMisses:
      db_stats_->keyspace_misses.fetch_add(v, std::memory_order_relaxed);
      break;
  }
}

rocksdb::ColumnFamilyHandle *Storage::GetCFHandle(ColumnFamilyID id) { return cf_handles_[static_cast<size_t>(id)]; }

rocksdb::Status Storage::Compact(rocksdb::ColumnFamilyHandle *cf, const Slice *begin, const Slice *end) {
  rocksdb::CompactRangeOptions compact_opts;
  compact_opts.change_level = true;
  // For the manual compaction, we would like to force the bottommost level to be compacted.
  // Or it may use the trivial mode and some expired key-values were still exist in the bottommost level.
  compact_opts.bottommost_level_compaction = rocksdb::BottommostLevelCompaction::kForceOptimized;
  const auto &cf_handles = cf ? std::vector<rocksdb::ColumnFamilyHandle *>{cf} : cf_handles_;
  for (const auto &cf_handle : cf_handles) {
    rocksdb::Status s = db_->CompactRange(compact_opts, cf_handle, begin, end);
    if (!s.ok()) return s;
  }
  return rocksdb::Status::OK();
}

uint64_t Storage::GetTotalSize(const std::string &ns) {
  if (ns == kDefaultNamespace) {
    return sst_file_manager_->GetTotalSize();
  }

  auto begin_key = ComposeNamespaceKey(ns, "", false);
  auto end_key = util::StringNext(begin_key);

  redis::Database db(this, ns);
  uint64_t size = 0, total_size = 0;
  rocksdb::DB::SizeApproximationFlags include_both =
      rocksdb::DB::SizeApproximationFlags::INCLUDE_FILES | rocksdb::DB::SizeApproximationFlags::INCLUDE_MEMTABLES;

  for (auto cf_handle : cf_handles_) {
    if (cf_handle == GetCFHandle(ColumnFamilyID::PubSub) || cf_handle == GetCFHandle(ColumnFamilyID::Propagate)) {
      continue;
    }

    rocksdb::Range r(begin_key, end_key);
    db_->GetApproximateSizes(cf_handle, &r, 1, &size, include_both);
    total_size += size;
  }

  return total_size;
}

void Storage::CheckDBSizeLimit() {
  bool limit_reached = false;
  if (config_->max_db_size > 0) {
    limit_reached = GetTotalSize() >= config_->max_db_size * GiB;
  }

  if (db_size_limit_reached_ == limit_reached) {
    return;
  }

  db_size_limit_reached_ = limit_reached;
  if (db_size_limit_reached_) {
    LOG(WARNING) << "[storage] ENABLE db_size limit " << config_->max_db_size << " GB."
                 << "Switch kvrocks to read-only mode.";
  } else {
    LOG(WARNING) << "[storage] DISABLE db_size limit. Switch kvrocks to read-write mode.";
  }
}

void Storage::SetIORateLimit(int64_t max_io_mb) {
  if (max_io_mb == 0) {
    max_io_mb = kIORateLimitMaxMb;
  }
  rate_limiter_->SetBytesPerSecond(max_io_mb * static_cast<int64_t>(MiB));
}

rocksdb::DB *Storage::GetDB() { return db_.get(); }

Status Storage::BeginTxn() {
  if (is_txn_mode_) {
    return Status{Status::NotOK, "cannot begin a new transaction while already in transaction mode"};
  }
  // The EXEC command is exclusive and shouldn't have multi transaction at the same time,
  // so it's fine to reset the global write batch without any lock.
  is_txn_mode_ = true;
  // Set overwrite_key to false to avoid overwriting the existing key in case
  // like downstream would parse the replication log etc.
  txn_write_batch_ = std::make_unique<rocksdb::WriteBatchWithIndex>(
      /*backup_index_comparator=*/rocksdb::BytewiseComparator(),
      /*reserved_bytes=*/0, /*overwrite_key=*/false, /*max_bytes=*/GetWriteBatchMaxBytes());
  return Status::OK();
}

Status Storage::CommitTxn() {
  if (!is_txn_mode_) {
    return Status{Status::NotOK, "cannot commit while not in transaction mode"};
  }
  engine::Context ctx(this);
  auto s = writeToDB(ctx, default_write_opts_, txn_write_batch_->GetWriteBatch());

  is_txn_mode_ = false;
  txn_write_batch_ = nullptr;
  if (s.ok()) {
    return Status::OK();
  }
  return {Status::NotOK, s.ToString()};
}

ObserverOrUniquePtr<rocksdb::WriteBatchBase> Storage::GetWriteBatchBase() {
  if (is_txn_mode_) {
    return ObserverOrUniquePtr<rocksdb::WriteBatchBase>(txn_write_batch_.get(), ObserverOrUnique::Observer);
  }
  return ObserverOrUniquePtr<rocksdb::WriteBatchBase>(
      new rocksdb::WriteBatch(0 /*reserved_bytes*/, GetWriteBatchMaxBytes()), ObserverOrUnique::Unique);
}

Status Storage::WriteToPropagateCF(engine::Context &ctx, const std::string &key, const std::string &value) {
  if (config_->IsSlave()) {
    return {Status::NotOK, "cannot write to propagate column family in slave mode"};
  }
  auto batch = GetWriteBatchBase();
  auto cf = GetCFHandle(ColumnFamilyID::Propagate);
  auto s = batch->Put(cf, key, value);
  s = Write(ctx, default_write_opts_, batch->GetWriteBatch());
  if (!s.ok()) {
    return {Status::NotOK, s.ToString()};
  }
  return Status::OK();
}

Status Storage::ShiftReplId(engine::Context &ctx) {
  static constexpr std::string_view charset = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";

  // Do nothing if rsid psync is not enabled
  if (!config_->use_rsid_psync) return Status::OK();

  std::random_device rd;
  std::mt19937 gen(rd() + getpid());
  std::uniform_int_distribution<size_t> distrib(0, charset.size() - 1);

  std::string rand_str(kReplIdLength, 0);
  for (int i = 0; i < kReplIdLength; i++) {
    rand_str[i] = charset[distrib(gen)];
  }
  replid_ = std::move(rand_str);
  LOG(INFO) << "[replication] New replication id: " << replid_;

  // Write new replication id into db engine
  return WriteToPropagateCF(ctx, kReplicationIdKey, replid_);
}

std::string Storage::GetReplIdFromWalBySeq(rocksdb::SequenceNumber seq) {
  std::unique_ptr<rocksdb::TransactionLogIterator> iter = nullptr;

  if (!WALHasNewData(seq) || !GetWALIter(seq, &iter).IsOK()) return "";

  // An extractor to extract update from raw writebatch
  class ReplIdExtractor : public rocksdb::WriteBatch::Handler {
   public:
    rocksdb::Status PutCF([[maybe_unused]] uint32_t column_family_id, [[maybe_unused]] const Slice &key,
                          [[maybe_unused]] const Slice &value) override {
      return rocksdb::Status::OK();
    }
    rocksdb::Status DeleteCF([[maybe_unused]] uint32_t column_family_id,
                             [[maybe_unused]] const rocksdb::Slice &key) override {
      return rocksdb::Status::OK();
    }
    rocksdb::Status DeleteRangeCF([[maybe_unused]] uint32_t column_family_id,
                                  [[maybe_unused]] const rocksdb::Slice &begin_key,
                                  [[maybe_unused]] const rocksdb::Slice &end_key) override {
      return rocksdb::Status::OK();
    }

    void LogData(const rocksdb::Slice &blob) override {
      // Currently, we always put replid log data at the end.
      if (ServerLogData::IsServerLogData(blob.data())) {
        ServerLogData server_log;
        if (server_log.Decode(blob).IsOK()) {
          if (server_log.GetType() == kReplIdLog) {
            replid_in_wal_ = server_log.GetContent();
          }
        }
      }
    };

    std::string GetReplId() { return replid_in_wal_; }

   private:
    std::string replid_in_wal_;
  };

  auto batch = iter->GetBatch();
  ReplIdExtractor write_batch_handler;
  rocksdb::Status s = batch.writeBatchPtr->Iterate(&write_batch_handler);
  if (!s.ok()) return "";

  return write_batch_handler.GetReplId();
}

std::string Storage::GetReplIdFromDbEngine() {
  std::string replid_in_db;
  auto cf = GetCFHandle(ColumnFamilyID::Propagate);
  auto s = db_->Get(rocksdb::ReadOptions(), cf, kReplicationIdKey, &replid_in_db);
  return replid_in_db;
}

std::shared_lock<std::shared_mutex> Storage::ReadLockGuard() { return std::shared_lock(db_rw_lock_); }

std::unique_lock<std::shared_mutex> Storage::WriteLockGuard() { return std::unique_lock(db_rw_lock_); }

Status Storage::ReplDataManager::GetFullReplDataInfo(Storage *storage, std::string *files) {
  auto guard = storage->ReadLockGuard();
  if (storage->IsClosing()) return {Status::NotOK, "DB is closing"};

  std::string data_files_dir = storage->config_->checkpoint_dir;
  std::unique_lock<std::mutex> ulm(storage->checkpoint_mu_);

  // Create checkpoint if not exist
  if (!storage->env_->FileExists(data_files_dir).ok()) {
    rocksdb::Checkpoint *checkpoint = nullptr;
    rocksdb::Status s = rocksdb::Checkpoint::Create(storage->db_.get(), &checkpoint);
    if (!s.ok()) {
      LOG(WARNING) << "Failed to create checkpoint object. Error: " << s.ToString();
      return {Status::NotOK, s.ToString()};
    }

    std::unique_ptr<rocksdb::Checkpoint> checkpoint_guard(checkpoint);

    // Create checkpoint of rocksdb
    uint64_t checkpoint_latest_seq = 0;
    s = checkpoint->CreateCheckpoint(data_files_dir, storage->config_->rocks_db.write_buffer_size * MiB,
                                     &checkpoint_latest_seq);
    auto now_secs = util::GetTimeStamp<std::chrono::seconds>();
    storage->checkpoint_info_.create_time_secs = now_secs;
    storage->checkpoint_info_.access_time_secs = now_secs;
    storage->checkpoint_info_.latest_seq = checkpoint_latest_seq;
    if (!s.ok()) {
      LOG(WARNING) << "[storage] Failed to create checkpoint (snapshot). Error: " << s.ToString();
      return {Status::NotOK, s.ToString()};
    }

    LOG(INFO) << "[storage] Create checkpoint successfully";
  } else {
    // Replicas can share checkpoint to replication if the checkpoint existing time is less than a half of WAL TTL.
    int64_t can_shared_time_secs = storage->config_->rocks_db.wal_ttl_seconds / 2;
    if (can_shared_time_secs > 60 * 60) can_shared_time_secs = 60 * 60;
    if (can_shared_time_secs < 10 * 60) can_shared_time_secs = 10 * 60;

    auto now_secs = util::GetTimeStamp<std::chrono::seconds>();
    if (now_secs - storage->GetCheckpointCreateTimeSecs() > can_shared_time_secs) {
      LOG(WARNING) << "[storage] Can't use current checkpoint, waiting next checkpoint";
      return {Status::NotOK, "Can't use current checkpoint, waiting for next checkpoint"};
    }

    // Should not use current checkpoint if its latest sequence was out of the WAL boundary,
    // or the slave will fall into the full sync loop since it won't create new checkpoint.
    auto s = storage->InWALBoundary(storage->checkpoint_info_.latest_seq);
    if (!s.IsOK()) {
      LOG(WARNING) << "[storage] Can't use current checkpoint, error: " << s.Msg();
      return {Status::NotOK, fmt::format("Can't use current checkpoint, error: {}", s.Msg())};
    }
    LOG(INFO) << "[storage] Using current existing checkpoint";
  }

  ulm.unlock();

  // Get checkpoint file list
  std::vector<std::string> result;
  storage->env_->GetChildren(data_files_dir, &result);
  for (const auto &f : result) {
    if (f == "." || f == "..") continue;

    files->append(f);
    files->push_back(',');
  }
  files->pop_back();

  return Status::OK();
}

bool Storage::ExistCheckpoint() {
  std::lock_guard<std::mutex> lg(checkpoint_mu_);
  return env_->FileExists(config_->checkpoint_dir).ok();
}

bool Storage::ExistSyncCheckpoint() { return env_->FileExists(config_->sync_checkpoint_dir).ok(); }

Status Storage::InWALBoundary(rocksdb::SequenceNumber seq) {
  std::unique_ptr<rocksdb::TransactionLogIterator> iter;
  auto s = GetWALIter(seq, &iter);
  if (!s.IsOK()) return s;
  auto wal_seq = iter->GetBatch().sequence;
  if (seq < wal_seq) {
    return {Status::NotOK, fmt::format("checkpoint seq: {} is smaller than the WAL seq: {}", seq, wal_seq)};
  }
  return Status::OK();
}

Status Storage::ReplDataManager::CleanInvalidFiles(Storage *storage, const std::string &dir,
                                                   std::vector<std::string> valid_files) {
  if (!storage->env_->FileExists(dir).ok()) {
    return Status::OK();
  }

  std::vector<std::string> tmp_files, files;
  storage->env_->GetChildren(dir, &tmp_files);
  for (const auto &file : tmp_files) {
    if (file == "." || file == "..") continue;
    files.push_back(file);
  }

  // Find invalid files
  std::sort(files.begin(), files.end());
  std::sort(valid_files.begin(), valid_files.end());
  std::vector<std::string> invalid_files(files.size() + valid_files.size());
  auto it =
      std::set_difference(files.begin(), files.end(), valid_files.begin(), valid_files.end(), invalid_files.begin());

  // Delete invalid files
  Status ret;
  invalid_files.resize(it - invalid_files.begin());
  for (it = invalid_files.begin(); it != invalid_files.end(); ++it) {
    auto s = storage->env_->DeleteFile(dir + "/" + *it);
    if (!s.ok()) {
      ret = Status(Status::NotOK, s.ToString());
      LOG(INFO) << "[storage] Failed to delete invalid file " << *it << " of master checkpoint";
    } else {
      LOG(INFO) << "[storage] Succeed deleting invalid file " << *it << " of master checkpoint";
    }
  }
  return ret;
}

int Storage::ReplDataManager::OpenDataFile(Storage *storage, const std::string &repl_file, uint64_t *file_size) {
  std::string abs_path = storage->config_->checkpoint_dir + "/" + repl_file;
  auto s = storage->env_->FileExists(abs_path);
  if (!s.ok()) {
    LOG(ERROR) << "[storage] Data file [" << abs_path << "] not found";
    return NullFD;
  }

  storage->env_->GetFileSize(abs_path, file_size);
  auto rv = open(abs_path.c_str(), O_RDONLY);
  if (rv < 0) {
    LOG(ERROR) << "[storage] Failed to open file: " << strerror(errno);
  }

  return rv;
}

Status Storage::ReplDataManager::ParseMetaAndSave(Storage *storage, rocksdb::BackupID meta_id, evbuffer *evbuf,
                                                  Storage::ReplDataManager::MetaInfo *meta) {
  auto meta_file = "meta/" + std::to_string(meta_id);
  DLOG(INFO) << "[meta] id: " << meta_id;

  // Save the meta to tmp file
  auto wf = NewTmpFile(storage, storage->config_->backup_sync_dir, meta_file);
  auto data = evbuffer_pullup(evbuf, -1);
  wf->Append(rocksdb::Slice(reinterpret_cast<char *>(data), evbuffer_get_length(evbuf)));
  wf->Close();

  // timestamp;
  UniqueEvbufReadln line(evbuf, EVBUFFER_EOL_LF);
  DLOG(INFO) << "[meta] timestamp: " << line.get();
  meta->timestamp = std::strtoll(line.get(), nullptr, 10);
  // sequence
  line = UniqueEvbufReadln(evbuf, EVBUFFER_EOL_LF);
  DLOG(INFO) << "[meta] seq:" << line.get();
  meta->seq = std::strtoull(line.get(), nullptr, 10);
  // optional metadata
  line = UniqueEvbufReadln(evbuf, EVBUFFER_EOL_LF);
  if (strncmp(line.get(), "metadata", 8) == 0) {
    DLOG(INFO) << "[meta] meta: " << line.get();
    meta->meta_data = std::string(line.get(), line.length);
    line = UniqueEvbufReadln(evbuf, EVBUFFER_EOL_LF);
  }
  DLOG(INFO) << "[meta] file count: " << line.get();
  // file list
  while (true) {
    line = UniqueEvbufReadln(evbuf, EVBUFFER_EOL_LF);
    if (!line) {
      break;
    }

    DLOG(INFO) << "[meta] file info: " << line.get();
    auto cptr = line.get();
    while (*(cptr++) != ' ') {
    }

    auto filename = std::string(line.get(), cptr - line.get() - 1);
    while (*(cptr++) != ' ') {
    }

    auto crc32 = std::strtoul(cptr, nullptr, 10);
    meta->files.emplace_back(filename, crc32);
  }

  return SwapTmpFile(storage, storage->config_->backup_sync_dir, meta_file);
}

Status MkdirRecursively(rocksdb::Env *env, const std::string &dir) {
  if (env->CreateDirIfMissing(dir).ok()) return Status::OK();

  std::string parent;
  for (auto pos = dir.find('/', 1); pos != std::string::npos; pos = dir.find('/', pos + 1)) {
    parent = dir.substr(0, pos);
    if (auto s = env->CreateDirIfMissing(parent); !s.ok()) {
      LOG(ERROR) << "[storage] Failed to create directory '" << parent << "' recursively. Error: " << s.ToString();
      return {Status::NotOK};
    }
  }

  if (env->CreateDirIfMissing(dir).ok()) return Status::OK();

  return {Status::NotOK};
}

std::unique_ptr<rocksdb::WritableFile> Storage::ReplDataManager::NewTmpFile(Storage *storage, const std::string &dir,
                                                                            const std::string &repl_file) {
  std::string tmp_file = dir + "/" + repl_file + ".tmp";
  auto s = storage->env_->FileExists(tmp_file);
  if (s.ok()) {
    LOG(ERROR) << "[storage] Data file exists, override";
    storage->env_->DeleteFile(tmp_file);
  }

  // Create directory if missing
  auto abs_dir = tmp_file.substr(0, tmp_file.rfind('/'));
  if (!MkdirRecursively(storage->env_, abs_dir).IsOK()) {
    return nullptr;
  }

  std::unique_ptr<rocksdb::WritableFile> wf;
  s = storage->env_->NewWritableFile(tmp_file, &wf, rocksdb::EnvOptions());
  if (!s.ok()) {
    LOG(ERROR) << "[storage] Failed to create data file '" << tmp_file << "'. Error: " << s.ToString();
    return nullptr;
  }

  return wf;
}

Status Storage::ReplDataManager::SwapTmpFile(Storage *storage, const std::string &dir, const std::string &repl_file) {
  std::string tmp_file = dir + "/" + repl_file + ".tmp";
  std::string orig_file = dir + "/" + repl_file;

  auto s = storage->env_->RenameFile(tmp_file, orig_file);
  if (!s.ok()) {
    return {Status::NotOK, fmt::format("unable to rename '{}' to '{}'. Error: {}", tmp_file, orig_file, s.ToString())};
  }

  return Status::OK();
}

bool Storage::ReplDataManager::FileExists(Storage *storage, const std::string &dir, const std::string &repl_file,
                                          uint32_t crc) {
  if (storage->IsClosing()) return false;

  auto file_path = dir + "/" + repl_file;
  auto s = storage->env_->FileExists(file_path);
  if (!s.ok()) return false;

  // If crc is 0, we needn't verify, return true directly.
  if (crc == 0) return true;

  std::unique_ptr<rocksdb::SequentialFile> src_file;
  const rocksdb::EnvOptions env_options;
  s = storage->env_->NewSequentialFile(file_path, &src_file, env_options);
  if (!s.ok()) return false;

  uint64_t size = 0;
  s = storage->env_->GetFileSize(file_path, &size);
  if (!s.ok()) return false;

  auto src_reader = std::make_unique<rocksdb::SequentialFileWrapper>(src_file.get());

  char buffer[4096];
  Slice slice;
  uint32_t tmp_crc = 0;
  while (size > 0) {
    size_t bytes_to_read = std::min(sizeof(buffer), static_cast<size_t>(size));
    s = src_reader->Read(bytes_to_read, &slice, buffer);
    if (!s.ok()) return false;

    if (slice.size() == 0) return false;

    tmp_crc = rocksdb::crc32c::Extend(0, slice.data(), slice.size());
    size -= slice.size();
  }

  return crc == tmp_crc;
}

[[nodiscard]] rocksdb::ReadOptions Context::GetReadOptions() {
  rocksdb::ReadOptions read_options;
  if (txn_context_enabled) read_options.snapshot = GetSnapshot();
  return read_options;
}

[[nodiscard]] rocksdb::ReadOptions Context::DefaultScanOptions() {
  rocksdb::ReadOptions read_options = storage->DefaultScanOptions();
  if (txn_context_enabled) read_options.snapshot = GetSnapshot();
  return read_options;
}

[[nodiscard]] rocksdb::ReadOptions Context::DefaultMultiGetOptions() {
  rocksdb::ReadOptions read_options = storage->DefaultMultiGetOptions();
  if (txn_context_enabled) read_options.snapshot = GetSnapshot();
  return read_options;
}

void Context::RefreshLatestSnapshot() {
  if (snapshot_) {
    storage->GetDB()->ReleaseSnapshot(snapshot_);
  }
  snapshot_ = storage->GetDB()->GetSnapshot();
  if (batch) {
    batch->Clear();
  }
}

}  // namespace engine
