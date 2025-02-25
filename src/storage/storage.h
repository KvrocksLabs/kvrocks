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

#include <event2/bufferevent.h>
#include <rocksdb/advanced_cache.h>
#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/table.h>
#include <rocksdb/utilities/backup_engine.h>
#include <rocksdb/utilities/write_batch_with_index.h>

#include <atomic>
#include <cinttypes>
#include <cstddef>
#include <memory>
#include <shared_mutex>
#include <string>
#include <utility>
#include <vector>

#include "common/port.h"
#include "config/config.h"
#include "lock_manager.h"
#include "observer_or_unique.h"
#include "rocksdb/write_batch.h"
#include "status.h"

enum class StorageEngineType : uint16_t {
  RocksDB,
};

inline constexpr StorageEngineType STORAGE_ENGINE_TYPE = StorageEngineType::KVROCKS_STORAGE_ENGINE;

const int kReplIdLength = 16;

enum DBOpenMode {
  kDBOpenModeDefault,
  kDBOpenModeForReadOnly,
  kDBOpenModeAsSecondaryInstance,
};

enum class ColumnFamilyID : uint32_t {
  PrimarySubkey = 0,
  Metadata,
  SecondarySubkey,
  PubSub,
  Propagate,
  Stream,
  Search,
};

constexpr uint32_t kMaxColumnFamilyID = static_cast<uint32_t>(ColumnFamilyID::Search);

namespace engine {

constexpr const char *kPropagateScriptCommand = "script";

constexpr const char *kLuaFuncSHAPrefix = "lua_f_";
constexpr const char *kLuaFuncLibPrefix = "lua_func_lib_";
constexpr const char *kLuaLibCodePrefix = "lua_lib_code_";

struct CompressionOption {
  rocksdb::CompressionType type;
  const std::string name;
  const std::string val;
};

inline const std::vector<CompressionOption> CompressionOptions = {
    {rocksdb::kNoCompression, "no", "kNoCompression"},
    {rocksdb::kSnappyCompression, "snappy", "kSnappyCompression"},
    {rocksdb::kZlibCompression, "zlib", "kZlibCompression"},
    {rocksdb::kLZ4Compression, "lz4", "kLZ4Compression"},
    {rocksdb::kZSTD, "zstd", "kZSTD"},
};

inline const std::vector<CompressionOption> WalCompressionOptions = {
    {rocksdb::kNoCompression, "no", "kNoCompression"},
    {rocksdb::kZSTD, "zstd", "kZSTD"},
};

struct CacheOption {
  BlockCacheType type;
  const std::string name;
  const std::string val;
};

inline const std::vector<CacheOption> CacheOptions = {
    {BlockCacheType::kCacheTypeLRU, "lru", "kCacheTypeLRU"},
    {BlockCacheType::kCacheTypeHCC, "hcc", "kCacheTypeHCC"},
};

enum class StatType : uint_fast8_t {
  CompactionCount,
  FlushCount,
  KeyspaceHits,
  KeyspaceMisses,
};

struct DBStats {
  alignas(CACHE_LINE_SIZE) std::atomic<uint_fast64_t> compaction_count = 0;
  alignas(CACHE_LINE_SIZE) std::atomic<uint_fast64_t> flush_count = 0;
  alignas(CACHE_LINE_SIZE) std::atomic<uint_fast64_t> keyspace_hits = 0;
  alignas(CACHE_LINE_SIZE) std::atomic<uint_fast64_t> keyspace_misses = 0;
};

class ColumnFamilyConfig {
 public:
  ColumnFamilyConfig(ColumnFamilyID id, std::string_view name, bool is_minor)
      : id_(id), name_(name), is_minor_(is_minor) {}
  ColumnFamilyID Id() const { return id_; }
  std::string_view Name() const { return name_; }
  bool IsMinor() const { return is_minor_; }

 private:
  ColumnFamilyID id_;
  std::string_view name_;
  bool is_minor_;
};

constexpr const std::string_view kPrimarySubkeyColumnFamilyName = "default";
constexpr const std::string_view kMetadataColumnFamilyName = "metadata";
constexpr const std::string_view kSecondarySubkeyColumnFamilyName = "zset_score";
constexpr const std::string_view kPubSubColumnFamilyName = "pubsub";
constexpr const std::string_view kPropagateColumnFamilyName = "propagate";
constexpr const std::string_view kStreamColumnFamilyName = "stream";
constexpr const std::string_view kSearchColumnFamilyName = "search";

class ColumnFamilyConfigs {
 public:
  /// DefaultSubkeyColumnFamily is the default column family in rocksdb.
  /// In kvrocks, we use it to store the data if metadata is not enough.
  static ColumnFamilyConfig PrimarySubkeyColumnFamily() {
    return {ColumnFamilyID::PrimarySubkey, kPrimarySubkeyColumnFamilyName, /*is_minor=*/false};
  }

  /// MetadataColumnFamily stores the metadata of data-structures.
  static ColumnFamilyConfig MetadataColumnFamily() {
    return {ColumnFamilyID::Metadata, kMetadataColumnFamilyName, /*is_minor=*/false};
  }

  /// SecondarySubkeyColumnFamily stores the score of zset or other secondary subkey.
  /// See https://kvrocks.apache.org/community/data-structure-on-rocksdb#zset for more details.
  static ColumnFamilyConfig SecondarySubkeyColumnFamily() {
    return {ColumnFamilyID::SecondarySubkey, kSecondarySubkeyColumnFamilyName,
            /*is_minor=*/true};
  }

  /// PubSubColumnFamily stores the pubsub data.
  static ColumnFamilyConfig PubSubColumnFamily() {
    return {ColumnFamilyID::PubSub, kPubSubColumnFamilyName, /*is_minor=*/true};
  }

  static ColumnFamilyConfig PropagateColumnFamily() {
    return {ColumnFamilyID::Propagate, kPropagateColumnFamilyName, /*is_minor=*/true};
  }

  static ColumnFamilyConfig StreamColumnFamily() {
    return {ColumnFamilyID::Stream, kStreamColumnFamilyName, /*is_minor=*/true};
  }

  static ColumnFamilyConfig SearchColumnFamily() {
    return {ColumnFamilyID::Search, kSearchColumnFamilyName, /*is_minor=*/true};
  }

  /// ListAllColumnFamilies returns all column families in kvrocks.
  static const std::vector<ColumnFamilyConfig> &ListAllColumnFamilies() { return AllCfs; }

  static const std::vector<ColumnFamilyConfig> &ListColumnFamiliesWithoutDefault() { return AllCfsWithoutDefault; }

  static const ColumnFamilyConfig &GetColumnFamily(ColumnFamilyID id) { return AllCfs[static_cast<size_t>(id)]; }

 private:
  // Caution: don't change the order of column family, or the handle will be mismatched
  inline const static std::vector<ColumnFamilyConfig> AllCfs = {
      PrimarySubkeyColumnFamily(), MetadataColumnFamily(), SecondarySubkeyColumnFamily(), PubSubColumnFamily(),
      PropagateColumnFamily(),     StreamColumnFamily(),   SearchColumnFamily(),
  };
  inline const static std::vector<ColumnFamilyConfig> AllCfsWithoutDefault = {
      MetadataColumnFamily(),  SecondarySubkeyColumnFamily(), PubSubColumnFamily(),
      PropagateColumnFamily(), StreamColumnFamily(),          SearchColumnFamily(),
  };
};

struct Context;

class Storage {
 public:
  explicit Storage(Config *config);
  ~Storage();

  void SetWriteOptions(const Config::RocksDB::WriteOptions &config);
  Status Open(DBOpenMode mode = kDBOpenModeDefault);
  void CloseDB();
  void TrySkipBlockCacheDeallocationOnClose();
  bool IsEmptyDB();
  void EmptyDB();
  rocksdb::BlockBasedTableOptions InitTableOptions();
  void SetBlobDB(rocksdb::ColumnFamilyOptions *cf_options);
  rocksdb::Options InitRocksDBOptions();
  Status SetOptionForAllColumnFamilies(const std::string &key, const std::string &value);
  Status SetDBOption(const std::string &key, const std::string &value);
  Status CreateColumnFamilies(const rocksdb::Options &options);
  // The sequence_number will be pointed to the value of the sequence number in range of DB,
  // but can't promise it's the latest sequence number. So you must check it by yourself before
  // using it.
  Status CreateBackup(uint64_t *sequence_number = nullptr);
  void DestroyBackup();
  Status RestoreFromBackup();
  Status RestoreFromCheckpoint();
  Status GetWALIter(rocksdb::SequenceNumber seq, std::unique_ptr<rocksdb::TransactionLogIterator> *iter);
  Status ReplicaApplyWriteBatch(rocksdb::WriteBatch *batch);
  Status ApplyWriteBatch(const rocksdb::WriteOptions &options, std::string &&raw_batch);
  rocksdb::SequenceNumber LatestSeqNumber();

  [[nodiscard]] rocksdb::Status Get(engine::Context &ctx, const rocksdb::ReadOptions &options,
                                    const rocksdb::Slice &key, std::string *value);
  [[nodiscard]] rocksdb::Status Get(engine::Context &ctx, const rocksdb::ReadOptions &options,
                                    rocksdb::ColumnFamilyHandle *column_family, const rocksdb::Slice &key,
                                    std::string *value);
  [[nodiscard]] rocksdb::Status Get(engine::Context &ctx, const rocksdb::ReadOptions &options,
                                    const rocksdb::Slice &key, rocksdb::PinnableSlice *value);
  [[nodiscard]] rocksdb::Status Get(engine::Context &ctx, const rocksdb::ReadOptions &options,
                                    rocksdb::ColumnFamilyHandle *column_family, const rocksdb::Slice &key,
                                    rocksdb::PinnableSlice *value);
  void MultiGet(engine::Context &ctx, const rocksdb::ReadOptions &options, rocksdb::ColumnFamilyHandle *column_family,
                size_t num_keys, const rocksdb::Slice *keys, rocksdb::PinnableSlice *values, rocksdb::Status *statuses);
  rocksdb::Iterator *NewIterator(engine::Context &ctx, const rocksdb::ReadOptions &options,
                                 rocksdb::ColumnFamilyHandle *column_family);
  rocksdb::Iterator *NewIterator(engine::Context &ctx, const rocksdb::ReadOptions &options);

  const rocksdb::WriteOptions &DefaultWriteOptions() const { return default_write_opts_; }
  rocksdb::ReadOptions DefaultScanOptions() const;
  rocksdb::ReadOptions DefaultMultiGetOptions() const;

  [[nodiscard]] rocksdb::Status Write(engine::Context &ctx, const rocksdb::WriteOptions &options,
                                      rocksdb::WriteBatch *updates);
  [[nodiscard]] rocksdb::Status Delete(engine::Context &ctx, const rocksdb::WriteOptions &options,
                                       rocksdb::ColumnFamilyHandle *cf_handle, const rocksdb::Slice &key);
  [[nodiscard]] rocksdb::Status DeleteRange(engine::Context &ctx, const rocksdb::WriteOptions &options,
                                            rocksdb::ColumnFamilyHandle *cf_handle, Slice begin, Slice end);
  [[nodiscard]] rocksdb::Status DeleteRange(engine::Context &ctx, Slice begin, Slice end);
  [[nodiscard]] rocksdb::Status FlushScripts(engine::Context &ctx, const rocksdb::WriteOptions &options,
                                             rocksdb::ColumnFamilyHandle *cf_handle);
  bool WALHasNewData(rocksdb::SequenceNumber seq) { return seq <= LatestSeqNumber(); }
  Status InWALBoundary(rocksdb::SequenceNumber seq);
  Status WriteToPropagateCF(engine::Context &ctx, const std::string &key, const std::string &value);

  [[nodiscard]] rocksdb::Status Compact(rocksdb::ColumnFamilyHandle *cf, const rocksdb::Slice *begin,
                                        const rocksdb::Slice *end);
  rocksdb::DB *GetDB();
  bool IsClosing() const { return db_closing_; }
  std::string GetName() const { return config_->db_name; }
  /// Get the column family handle by the column family id.
  rocksdb::ColumnFamilyHandle *GetCFHandle(ColumnFamilyID id);
  std::vector<rocksdb::ColumnFamilyHandle *> *GetCFHandles() { return &cf_handles_; }
  LockManager *GetLockManager() { return &lock_mgr_; }
  void PurgeOldBackups(uint32_t num_backups_to_keep, uint32_t backup_max_keep_hours);
  uint64_t GetTotalSize(const std::string &ns = kDefaultNamespace);
  void CheckDBSizeLimit();
  bool ReachedDBSizeLimit() { return db_size_limit_reached_; }
  void SetDBSizeLimit(bool limit) { db_size_limit_reached_ = limit; }
  void SetIORateLimit(int64_t max_io_mb);

  std::shared_lock<std::shared_mutex> ReadLockGuard();
  std::unique_lock<std::shared_mutex> WriteLockGuard();

  bool IsSlotIdEncoded() const { return config_->slot_id_encoded; }
  Config *GetConfig() const { return config_; }

  const DBStats *GetDBStats() const { return db_stats_.get(); }
  void RecordStat(StatType type, uint64_t v);

  Status BeginTxn();
  Status CommitTxn();
  ObserverOrUniquePtr<rocksdb::WriteBatchBase> GetWriteBatchBase();

  Storage(const Storage &) = delete;
  Storage &operator=(const Storage &) = delete;

  int GetWriteBatchMaxBytes() const { return config_->rocks_db.write_options.write_batch_max_bytes; }
  // Full replication data files manager
  class ReplDataManager {
   public:
    // Master side
    static Status GetFullReplDataInfo(Storage *storage, std::string *files);
    static int OpenDataFile(Storage *storage, const std::string &rel_file, uint64_t *file_size);
    static Status CleanInvalidFiles(Storage *storage, const std::string &dir, std::vector<std::string> valid_files);
    struct CheckpointInfo {
      // System clock time when the checkpoint was created.
      std::atomic<int64_t> create_time_secs = 0;
      // System clock time when the checkpoint was last accessed.
      std::atomic<int64_t> access_time_secs = 0;
      uint64_t latest_seq = 0;
    };

    // Slave side
    struct MetaInfo {
      int64_t timestamp;
      rocksdb::SequenceNumber seq;
      std::string meta_data;
      // [[filename, checksum]...]
      std::vector<std::pair<std::string, uint32_t>> files;
    };
    static Status ParseMetaAndSave(Storage *storage, rocksdb::BackupID meta_id, evbuffer *evbuf,
                                   Storage::ReplDataManager::MetaInfo *meta);
    static std::unique_ptr<rocksdb::WritableFile> NewTmpFile(Storage *storage, const std::string &dir,
                                                             const std::string &repl_file);
    static Status SwapTmpFile(Storage *storage, const std::string &dir, const std::string &repl_file);
    static bool FileExists(Storage *storage, const std::string &dir, const std::string &repl_file, uint32_t crc);
  };

  bool ExistCheckpoint();
  bool ExistSyncCheckpoint();
  int64_t GetCheckpointCreateTimeSecs() const { return checkpoint_info_.create_time_secs; }
  void SetCheckpointAccessTimeSecs(int64_t t) { checkpoint_info_.access_time_secs = t; }
  int64_t GetCheckpointAccessTimeSecs() const { return checkpoint_info_.access_time_secs; }
  void SetDBInRetryableIOError(bool yes_or_no) { db_in_retryable_io_error_ = yes_or_no; }
  bool IsDBInRetryableIOError() const { return db_in_retryable_io_error_; }

  /// Redis PSYNC relies on a Unique Replication Sequence Id when use-rsid-psync
  /// enabled.
  /// ShiftReplId would generate an Id and write it to propagate cf.
  Status ShiftReplId(engine::Context &ctx);
  std::string GetReplIdFromWalBySeq(rocksdb::SequenceNumber seq);
  std::string GetReplIdFromDbEngine();

 private:
  std::unique_ptr<rocksdb::DB> db_ = nullptr;
  std::string replid_;
  // The system clock time when the backup was created.
  int64_t backup_creating_time_secs_;
  std::unique_ptr<rocksdb::BackupEngine> backup_ = nullptr;
  rocksdb::Env *env_;
  std::shared_ptr<rocksdb::SstFileManager> sst_file_manager_;
  std::shared_ptr<rocksdb::RateLimiter> rate_limiter_;
  ReplDataManager::CheckpointInfo checkpoint_info_;
  std::mutex checkpoint_mu_;
  Config *config_ = nullptr;
  std::vector<rocksdb::ColumnFamilyHandle *> cf_handles_;
  LockManager lock_mgr_;
  std::atomic<bool> db_size_limit_reached_{false};

  std::unique_ptr<DBStats> db_stats_;

  std::shared_mutex db_rw_lock_;
  bool db_closing_ = true;

  std::atomic<bool> db_in_retryable_io_error_{false};

  // is_txn_mode_ is used to determine whether the current Storage is in transactional mode,
  // .i.e, in "EXEC" command(CommandExec).
  std::atomic<bool> is_txn_mode_ = false;
  // txn_write_batch_ is used as the global write batch for the transaction mode,
  // all writes will be grouped in this write batch when entering the transaction mode,
  // then write it at once when committing.
  //
  // Notice: the reason why we can use the global transaction? because the EXEC is an exclusive
  // command, so it won't have multi transactions to be executed at the same time.
  std::unique_ptr<rocksdb::WriteBatchWithIndex> txn_write_batch_;

  rocksdb::WriteOptions default_write_opts_;

  // rocksdb used global block cache
  std::shared_ptr<rocksdb::Cache> shared_block_cache_;

  rocksdb::Status writeToDB(engine::Context &ctx, const rocksdb::WriteOptions &options, rocksdb::WriteBatch *updates);
  void recordKeyspaceStat(const rocksdb::ColumnFamilyHandle *column_family, const rocksdb::Status &s);
  Status applyWriteBatch(const rocksdb::WriteOptions &options, rocksdb::WriteBatch *batch);
};

/// Context passes fixed snapshot and batch between APIs
///
/// Limitations: Performing a large number of writes or apply operations like DeleteRange
/// on the same Context may reduce performance.
/// Please choose to use the same Context or create a new Context based on the actual
/// situation.
///
/// Context does not provide thread safety guarantees and is generally only passed as a parameter between APIs.
struct Context {
  engine::Storage *storage = nullptr;

  /// batch can be nullptr if
  /// 1. The Context is not in transactional mode.
  /// 2. The Context is in transactional mode, but no write operation is performed.
  std::unique_ptr<rocksdb::WriteBatchWithIndex> batch = nullptr;

  /// txn_context_enabled is used to determine whether the current Context is in transactional mode,
  /// if it is not transactional mode, then Context is equivalent to a Storage.
  /// If the configuration of txn-context-enabled is no, it is false.
  bool txn_context_enabled = true;

  /// NoTransactionContext returns a Context with a is_txn_mode of false
  static Context NoTransactionContext(engine::Storage *storage) { return Context(storage, /*txn_mode=*/false); }

  /// GetReadOptions returns a default ReadOptions, and if txn_context_enabled = true,
  /// then its snapshot is specified by the Context.
  /// Otherwise it is the same as Storage::DefaultReadOptions().
  [[nodiscard]] rocksdb::ReadOptions GetReadOptions();
  /// DefaultScanOptions returns a DefaultScanOptions, and if txn_context_enabled = true,
  /// then its snapshot is specified by the Context.
  /// Otherwise it is the same as Storage::DefaultScanOptions().
  [[nodiscard]] rocksdb::ReadOptions DefaultScanOptions();
  /// DefaultMultiGetOptions returns a DefaultMultiGetOptions, and if txn_context_enabled = true,
  /// then its snapshot is specified by the Context.
  /// Otherwise it is the same as Storage::DefaultMultiGetOptions
  [[nodiscard]] rocksdb::ReadOptions DefaultMultiGetOptions();

  void RefreshLatestSnapshot();

  /// TODO: Change it to defer getting the context, and the snapshot is pinned after the first read operation
  explicit Context(engine::Storage *storage)
      : storage(storage), txn_context_enabled(storage->GetConfig()->txn_context_enabled) {}
  ~Context() {
    // A moved-from object doesn't have `storage`.
    if (storage) {
      if (snapshot_ && storage->GetDB()) {
        storage->GetDB()->ReleaseSnapshot(snapshot_);
      }
    }
  }
  Context(const Context &) = delete;
  Context &operator=(const Context &) = delete;
  Context &operator=(Context &&ctx) noexcept {
    if (this != &ctx) {
      storage = ctx.storage;
      snapshot_ = ctx.snapshot_;
      batch = std::move(ctx.batch);

      ctx.storage = nullptr;
      ctx.snapshot_ = nullptr;
    }
    return *this;
  }
  Context(Context &&ctx) noexcept : storage(ctx.storage), batch(std::move(ctx.batch)), snapshot_(ctx.snapshot_) {
    ctx.storage = nullptr;
    ctx.snapshot_ = nullptr;
  }

  // GetSnapshot will create a snapshot first if it doesn't exist,
  // and it's not a thread-safe operation.
  const rocksdb::Snapshot *GetSnapshot() {
    if (snapshot_ == nullptr) {
      // Should not acquire a snapshot_ on a moved-from object.
      DCHECK(storage != nullptr);
      snapshot_ = storage->GetDB()->GetSnapshot();  // NOLINT
    }
    return snapshot_;
  }

 private:
  /// It is only used by NonTransactionContext
  explicit Context(engine::Storage *storage, bool txn_mode) : storage(storage), txn_context_enabled(txn_mode) {}

  /// If is_txn_mode is true, snapshot should be specified instead of nullptr when used,
  /// and should be consistent with snapshot in ReadOptions to avoid ambiguity.
  /// Normally it will be fixed to the latest Snapshot when the Context is constructed.
  /// If is_txn_mode is false, the snapshot is nullptr.
  const rocksdb::Snapshot *snapshot_ = nullptr;
};

}  // namespace engine
