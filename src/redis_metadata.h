#pragma once

#include <rocksdb/env.h>
#include <rocksdb/status.h>
#include <rocksdb/db.h>
#include <string>
#include <vector>
#include <map>

#include "redis_encoding.h"
#include "storage.h"

enum RedisType {
  kRedisNone,
  kRedisString,
  kRedisHash,
  kRedisList,
  kRedisSet,
  kRedisZSet,
  kRedisBitmap
};

enum RedisCommand {
  kRedisCmdLSet,
  kRedisCmdLInsert,
  kRedisCmdLTrim,
  kRedisCmdLPop,
  kRedisCmdRPop,
  kRedisCmdLRem,
  kRedisCmdLPush,
  kRedisCmdRPush,
  kRedisCmdExpire,
};

using rocksdb::Slice;

void ExtractNamespaceKey(Slice ns_key, std::string *ns, std::string *key);
void ComposeNamespaceKey(const Slice &ns, const Slice &key, std::string *ns_key);

class InternalKey {
 public:
  explicit InternalKey(Slice ns_key, Slice sub_key, uint64_t version);
  explicit InternalKey(Slice input);
  ~InternalKey();

  Slice GetNamespace() const;
  Slice GetKey() const;
  Slice GetSubKey() const;
  uint64_t GetVersion() const;
  void Encode(std::string *out);
  bool operator==(const InternalKey &that) const;

 private:
  Slice namespace_;
  Slice key_;
  Slice sub_key_;
  uint64_t version_;
  char *buf_;
  char prealloc_[256];
};

class Metadata {
 public:
  uint8_t flags;
  int expire;
  uint64_t version;
  uint32_t size;

 public:
  explicit Metadata(RedisType type);

  RedisType Type() const;
  virtual int32_t TTL() const;
  virtual bool Expired() const;
  virtual void Encode(std::string *dst);
  virtual rocksdb::Status Decode(const std::string &bytes);
  bool operator==(const Metadata &that) const;

 private:
  uint64_t generateVersion();
};

class HashMetadata : public Metadata {
 public:
  HashMetadata():Metadata(kRedisHash){}
};

class SetMetadata : public Metadata {
 public:
  SetMetadata(): Metadata(kRedisSet) {}
};

class ZSetMetadata : public Metadata {
 public:
  ZSetMetadata(): Metadata(kRedisZSet){}
};

class BitmapMetadata : public Metadata {
 public:
  BitmapMetadata(): Metadata(kRedisBitmap){}
};

class ListMetadata : public Metadata {
 public:
  uint64_t head;
  uint64_t tail;
  ListMetadata();
 public:
  void Encode(std::string *dst) override;
  rocksdb::Status Decode(const std::string &bytes) override;
};

class RedisDB {
 public:
  explicit RedisDB(Engine::Storage *storage, const std::string &ns = "");
  rocksdb::Status GetMetadata(RedisType type, const Slice &ns_key, Metadata *metadata);
  rocksdb::Status Expire(const Slice &user_key, int timestamp);
  rocksdb::Status Del(const Slice &user_key);
  rocksdb::Status Exists(const std::vector<Slice> &keys, int *ret);
  rocksdb::Status TTL(const Slice &user_key, int *ttl);
  rocksdb::Status Type(const Slice &user_key, RedisType *type);
  rocksdb::Status FlushAll();
  uint64_t GetKeyNum(const std::string &prefix = "");
  uint64_t Keys(std::string prefix, std::vector<std::string> *keys);
  rocksdb::Status Scan(const std::string &cursor,
                       uint64_t limit,
                       const std::string &prefix,
                       std::vector<std::string> *keys);
  rocksdb::Status RandomKey(const std::string &cursor, std::string *key);
  void AppendNamespacePrefix(const Slice &user_key, std::string *output);

 protected:
  Engine::Storage *storage_;
  rocksdb::DB *db_;
  rocksdb::ColumnFamilyHandle *metadata_cf_handle_;
  std::string namespace_;

  class LatestSnapShot {
   public:
    explicit LatestSnapShot(rocksdb::DB *db): db_(db) {
      snapshot_ = db_->GetSnapshot();
    }
    ~LatestSnapShot() {
      db_->ReleaseSnapshot(snapshot_);
    }
    const rocksdb::Snapshot *GetSnapShot() { return snapshot_; }
   private:
    rocksdb::DB *db_ = nullptr;
    const rocksdb::Snapshot* snapshot_ = nullptr;
  };
};

class RedisSubKeyScanner : public RedisDB {
 public:
  explicit RedisSubKeyScanner(Engine::Storage *storage, const std::string &ns)
      : RedisDB(storage, ns) {}
  rocksdb::Status Scan(RedisType type,
                       const Slice &user_key,
                       const std::string &cursor,
                       uint64_t limit,
                       const std::string &subkey_prefix,
                       std::vector<std::string> *keys);
};

class LockGuard {
 public:
  explicit LockGuard(LockManager *lock_mgr, Slice key):
      lock_mgr_(lock_mgr),
      key_(key) {
      lock_mgr->Lock(key_);
  }
  ~LockGuard() {
    lock_mgr_->UnLock(key_);
  }
 private:
  LockManager *lock_mgr_ = nullptr;
  Slice key_;
};

class WriteBatchLogData {
 public:
  WriteBatchLogData() = default;
  explicit WriteBatchLogData(RedisType type) : type_(type) {}
  explicit WriteBatchLogData(RedisType type, std::vector<std::string> &&args) :
      type_(type), args_(std::move(args)) {}

  RedisType GetRedisType();
  std::vector<std::string> *GetArguments();
  std::string Encode();
  Status Decode(const rocksdb::Slice &blob);

 private:
  RedisType type_ = kRedisNone;
  std::vector<std::string> args_;
};
