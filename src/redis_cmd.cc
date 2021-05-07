#include <arpa/inet.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <algorithm>
#include <cctype>
#include <cmath>
#include <chrono>
#include <thread>
#include <utility>
#include <memory>
#include <glog/logging.h>

#include "redis_db.h"
#include "redis_cmd.h"
#include "redis_hash.h"
#include "redis_bitmap.h"
#include "redis_list.h"
#include "redis_request.h"
#include "redis_connection.h"
#include "redis_set.h"
#include "redis_string.h"
#include "redis_zset.h"
#include "redis_geo.h"
#include "redis_pubsub.h"
#include "redis_sortedint.h"
#include "replication.h"
#include "util.h"
#include "storage.h"
#include "worker.h"
#include "server.h"
#include "log_collector.h"

namespace Redis {

const char *errInvalidSyntax = "syntax error";
const char *errInvalidExpireTime = "invalid expire time";
const char *errWrongNumOfArguments = "wrong number of arguments";
const char *errValueNotInterger = "value is not an integer or out of range";
const char *errAdministorPermissionRequired = "administor permission required to perform the command";

std::string Commander::Name() {
  return this->attributes_->name;
}

int Commander::GetArity() const {
  return this->attributes_->arity;
}

bool Commander::IsWrite() const {
  return this->attributes_->is_write;
}

class CommandAuth : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    Config *config = svr->GetConfig();
    auto user_password = args_[1];
    if (!conn->IsRepl()) {
      auto iter = config->tokens.find(user_password);
      if (iter != config->tokens.end()) {
        conn->SetNamespace(iter->second);
        conn->BecomeUser();
        *output = Redis::SimpleString("OK");
        return Status::OK();
      }
    }
    const auto requirepass = conn->IsRepl() ? config->masterauth : config->requirepass;
    if (!requirepass.empty() && user_password != requirepass) {
      *output = Redis::Error("ERR invaild password");
      return Status::OK();
    }
    conn->SetNamespace(kDefaultNamespace);
    conn->BecomeAdmin();
    if (requirepass.empty()) {
      *output = Redis::Error("ERR Client sent AUTH, but no password is set");
    } else {
      *output = Redis::SimpleString("OK");
    }
    return Status::OK();
  }
};

class CommandNamespace : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    if (!conn->IsAdmin()) {
      *output = Redis::Error(errAdministorPermissionRequired);
      return Status::OK();
    }
    Config *config = svr->GetConfig();
    std::string sub_command = Util::ToLower(args_[1]);
    if (args_.size() == 3 && sub_command == "get") {
      if (args_[2] == "*") {
        std::vector<std::string> namespaces;
        auto tokens = config->tokens;
        for (auto iter = tokens.begin(); iter != tokens.end(); iter++) {
          namespaces.emplace_back(iter->second);  // namespace
          namespaces.emplace_back(iter->first);   // token
        }
        *output = Redis::MultiBulkString(namespaces, false);
      } else {
        std::string token;
        auto s = config->GetNamespace(args_[2], &token);
        if (s.IsNotFound()) {
          *output = Redis::NilString();
        } else {
          *output = Redis::BulkString(token);
        }
      }
    } else if (args_.size() == 4 && sub_command == "set") {
      Status s = config->SetNamespace(args_[2], args_[3]);
      *output = s.IsOK() ? Redis::SimpleString("OK") : Redis::Error(s.Msg());
      LOG(WARNING) << "Updated namespace: " << args_[2] << " with token: " << args_[3]
      << ", addr: " << conn->GetAddr() << ", result: " << s.Msg();
    } else if (args_.size() == 4 && sub_command == "add") {
      Status s = config->AddNamespace(args_[2], args_[3]);
      *output = s.IsOK() ? Redis::SimpleString("OK") : Redis::Error(s.Msg());
      LOG(WARNING) << "New namespace: " << args_[2] << " with token: " << args_[3]
                   << ", addr: " << conn->GetAddr() << ", result: " << s.Msg();
    } else if (args_.size() == 3 && sub_command == "del") {
      Status s = config->DelNamespace(args_[2]);
      *output = s.IsOK() ? Redis::SimpleString("OK") : Redis::Error(s.Msg());
      LOG(WARNING) << "Deleted namespace: " << args_[2]
                   << ", addr: " << conn->GetAddr() << ", result: " << s.Msg();
    } else {
      *output = Redis::Error(
          "NAMESPACE subcommand must be one of GET, SET, DEL, ADD");
    }
    return Status::OK();
  }
};

class CommandKeys : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    std::string prefix = args_[1];
    std::vector<std::string> keys;
    Redis::Database redis(svr->storage_, conn->GetNamespace());
    if (prefix == "*") {
      redis.Keys(std::string(), &keys);
    } else {
      if (prefix[prefix.size() - 1] != '*') {
        *output = Redis::Error("ERR only keys prefix match was supported");
        return Status::OK();
      }
      redis.Keys(prefix.substr(0, prefix.size() - 1), &keys);
    }
    *output = Redis::MultiBulkString(keys);
    return Status::OK();
  }
};

class CommandFlushDB : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    Redis::Database redis(svr->storage_, conn->GetNamespace());
    rocksdb::Status s = redis.FlushDB();
    LOG(WARNING) << "DB keys in namespce: " << conn->GetNamespace()
              << " was flushed, addr: " << conn->GetAddr();
    if (s.ok()) {
      *output = Redis::SimpleString("OK");
      return Status::OK();
    }
    return Status(Status::RedisExecErr, s.ToString());
  }
};

class CommandFlushAll : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    if (!conn->IsAdmin()) {
      *output = Redis::Error(errAdministorPermissionRequired);
      return Status::OK();
    }
    Redis::Database redis(svr->storage_, conn->GetNamespace());
    rocksdb::Status s = redis.FlushAll();
    LOG(WARNING) << "All DB keys was flushed, addr: " << conn->GetAddr();
    if (s.ok()) {
      *output = Redis::SimpleString("OK");
      return Status::OK();
    }
    return Status(Status::RedisExecErr, s.ToString());
  }
};

class CommandPing : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    *output = Redis::SimpleString("PONG");
    return Status::OK();
  }
};

class CommandSelect: public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    *output = Redis::SimpleString("OK");
    return Status::OK();
  }
};

class CommandConfig : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    if (!conn->IsAdmin()) {
      *output = Redis::Error(errAdministorPermissionRequired);
      return Status::OK();
    }
    Config *config = svr->GetConfig();
    std::string sub_command = Util::ToLower(args_[1]);
    if ((sub_command == "rewrite" && args_.size() != 2) ||
        (sub_command == "get" && args_.size() != 3) ||
        (sub_command == "set" && args_.size() != 4)) {
      *output = Redis::Error(errWrongNumOfArguments);
      return Status::OK();
    }
    if (args_.size() == 2 && sub_command == "rewrite") {
      Status s = config->Rewrite();
      if (!s.IsOK()) return Status(Status::RedisExecErr, s.Msg());
      *output = Redis::SimpleString("OK");
      LOG(INFO) << "# CONFIG REWRITE executed with success";
    } else if (args_.size() == 3 && sub_command == "get") {
      std::vector<std::string> values;
      config->Get(args_[2], &values);
      *output = Redis::MultiBulkString(values);
    } else if (args_.size() == 4 && sub_command == "set") {
      Status s = config->Set(svr, args_[2], args_[3]);
      if (!s.IsOK()) {
        *output = Redis::Error("CONFIG SET '"+args_[2]+"' error: "+s.Msg());
      } else {
        *output = Redis::SimpleString("OK");
      }
    } else {
      *output = Redis::Error("CONFIG subcommand must be one of GET, SET, REWRITE");
    }
    return Status::OK();
  }
};

class CommandGet : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    std::string value;
    Redis::String string_db(svr->storage_, conn->GetNamespace());
    rocksdb::Status s = string_db.Get(args_[1], &value);
    if (!s.ok() && !s.IsNotFound()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    *output = s.IsNotFound() ? Redis::NilString() : Redis::BulkString(value);
    return Status::OK();
  }
};

class CommandStrlen: public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    std::string value;
    Redis::String string_db(svr->storage_, conn->GetNamespace());
    rocksdb::Status s = string_db.Get(args_[1], &value);
    if (!s.ok() && !s.IsNotFound()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    if (s.IsNotFound()) {
      *output = Redis::Integer(0);
    } else {
      *output = Redis::Integer(value.size());
    }
    return Status::OK();
  }
};

class CommandGetSet : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    Redis::String string_db(svr->storage_, conn->GetNamespace());
    std::string old_value;
    rocksdb::Status s = string_db.GetSet(args_[1], args_[2], &old_value);
    if (!s.ok() && !s.IsNotFound()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    if (s.IsNotFound()) {
      *output = Redis::NilString();
    } else {
      *output = Redis::BulkString(old_value);
    }
    return Status::OK();
  }
};

class CommandGetRange: public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    try {
      start_ = std::stoi(args[2]);
      stop_ = std::stoi(args[3]);
    } catch (std::exception &e) {
      return Status(Status::RedisParseErr, errValueNotInterger);
    }
    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    std::string value;
    Redis::String string_db(svr->storage_, conn->GetNamespace());
    rocksdb::Status s = string_db.Get(args_[1], &value);
    if (!s.ok() && !s.IsNotFound()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    if (s.IsNotFound()) {
      *output = Redis::NilString();
      return Status::OK();
    }
    if (start_ < 0) start_ = static_cast<int>(value.size()) + start_;
    if (stop_ < 0) stop_ = static_cast<int>(value.size()) + stop_;
    if (start_ < 0) start_ = 0;
    if (stop_ > static_cast<int>(value.size())) stop_ = static_cast<int>(value.size());
    if (start_ > stop_) {
      *output = Redis::NilString();
    } else {
      *output = Redis::BulkString(value.substr(start_, stop_+1));
    }
    return Status::OK();
  }

 private:
  int start_ = 0, stop_ = 0;
};

class CommandSetRange: public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    try {
      offset_ = std::stoi(args[2]);
    } catch (std::exception &e) {
      return Status(Status::RedisParseErr, errValueNotInterger);
    }
    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    int ret;
    Redis::String string_db(svr->storage_, conn->GetNamespace());
    rocksdb::Status s = string_db.SetRange(args_[1], offset_, args_[3], &ret);
    if (!s.ok()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    *output = Redis::Integer(ret);
    return Status::OK();
  }

 private:
  int offset_ = 0;
};

class CommandMGet : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    Redis::String string_db(svr->storage_, conn->GetNamespace());
    std::vector<Slice> keys;
    for (size_t i = 1; i < args_.size(); i++) {
      keys.emplace_back(args_[i]);
    }
    std::vector<std::string> values;
    // always return OK
    auto statuses = string_db.MGet(keys, &values);
    *output = Redis::MultiBulkString(values, statuses);
    return Status::OK();
  }
};

class CommandAppend: public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    int ret;
    Redis::String string_db(svr->storage_, conn->GetNamespace());
    rocksdb::Status s = string_db.Append(args_[1], args_[2], &ret);
    if (!s.ok()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    *output = Redis::Integer(ret);
    return Status::OK();
  }
};

class CommandSet : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    bool last_arg;
    for (size_t i = 3; i < args.size(); i++) {
      last_arg = (i == args.size()-1);
      std::string opt = Util::ToLower(args[i]);
      if (opt == "nx") {
        nx_ = true;
      } else if (opt == "xx") {
        xx_ = true;
      } else if (opt == "ex") {
        if (last_arg) return Status(Status::NotOK, errInvalidSyntax);
        ttl_ = atoi(args_[++i].c_str());
        if (ttl_ <= 0) return Status(Status::RedisParseErr, errInvalidExpireTime);
      } else if (opt == "px") {
        if (last_arg) return Status(Status::NotOK, errInvalidSyntax);
        auto ttl_ms = atol(args[++i].c_str());
        if (ttl_ms <= 0) return Status(Status::RedisParseErr, errInvalidExpireTime);
        if (ttl_ms > 0 && ttl_ms < 1000) {
          // round up the pttl to second
          ttl_ = 1;
        } else {
          ttl_ = static_cast<int>(ttl_ms/1000);
        }
      } else {
        return Status(Status::NotOK, errInvalidSyntax);
      }
    }
    return Commander::Parse(args);
  }
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    int ret;
    Redis::String string_db(svr->storage_, conn->GetNamespace());
    rocksdb::Status s;
    if (nx_) {
      s = string_db.SetNX(args_[1], args_[2], ttl_, &ret);
    } else if (xx_) {
      s = string_db.SetXX(args_[1], args_[2], ttl_, &ret);
    } else {
      s = string_db.SetEX(args_[1], args_[2], ttl_);
    }
    if (!s.ok()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    if ((nx_ || xx_) && !ret) {
      *output = Redis::NilString();
    } else {
      *output = Redis::SimpleString("OK");
    }
    return Status::OK();
  }

 private:
  bool xx_ = false;
  bool nx_ = false;
  int ttl_ = 0;
};

class CommandSetEX : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    try {
      ttl_ = std::stoi(args[2]);
    } catch (std::exception &e) {
      return Status(Status::RedisParseErr, errValueNotInterger);
    }
    if (ttl_ <= 0) return Status(Status::RedisParseErr, errInvalidExpireTime);
    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    Redis::String string_db(svr->storage_, conn->GetNamespace());
    rocksdb::Status s = string_db.SetEX(args_[1], args_[3], ttl_);
    *output = Redis::SimpleString("OK");
    return Status::OK();
  }

 private:
  int ttl_ = 0;
};

class CommandPSetEX : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    try {
      auto ttl_ms = std::stol(args[2]);
      if (ttl_ms <= 0) return Status(Status::RedisParseErr, errInvalidExpireTime);
      if (ttl_ms > 0 && ttl_ms < 1000) {
        ttl_ = 1;
      } else {
        ttl_ = ttl_ms / 1000;
      }
    } catch (std::exception &e) {
      return Status(Status::RedisParseErr, errValueNotInterger);
    }
    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    Redis::String string_db(svr->storage_, conn->GetNamespace());
    rocksdb::Status s = string_db.SetEX(args_[1], args_[3], ttl_);
    *output = Redis::SimpleString("OK");
    return Status::OK();
  }

 private:
  int ttl_ = 0;
};

class CommandMSet : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    if (args.size() % 2 != 1) {
      return Status(Status::RedisParseErr, errWrongNumOfArguments);
    }
    return Commander::Parse(args);
  }
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    Redis::String string_db(svr->storage_, conn->GetNamespace());
    std::vector<StringPair> kvs;
    for (size_t i = 1; i < args_.size(); i+=2) {
      kvs.emplace_back(StringPair{args_[i], args_[i+1]});
    }
    rocksdb::Status s = string_db.MSet(kvs);
    if (!s.ok()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    *output = Redis::SimpleString("OK");
    return Status::OK();
  }
};

class CommandSetNX : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    int ret;
    Redis::String string_db(svr->storage_, conn->GetNamespace());
    rocksdb::Status s = string_db.SetNX(args_[1], args_[2], 0, &ret);
    if (!s.ok()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    *output = Redis::Integer(ret);
    return Status::OK();
  }
};

class CommandMSetNX : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    if (args.size() % 2 != 1) {
      return Status(Status::RedisParseErr, errWrongNumOfArguments);
    }
    return Commander::Parse(args);
  }
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    int ret;
    std::vector<StringPair> kvs;
    Redis::String string_db(svr->storage_, conn->GetNamespace());
    for (size_t i = 1; i < args_.size(); i+=2) {
      kvs.emplace_back(StringPair{args_[i], args_[i+1]});
    }
    rocksdb::Status s = string_db.MSetNX(kvs, 0, &ret);
    if (!s.ok()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    *output = Redis::Integer(ret);
    return Status::OK();
  }
};

class CommandIncr : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    int64_t ret;
    Redis::String string_db(svr->storage_, conn->GetNamespace());
    rocksdb::Status s = string_db.IncrBy(args_[1], 1, &ret);
    if (!s.ok()) return Status(Status::RedisExecErr, s.ToString());
    *output = Redis::Integer(ret);
    return Status::OK();
  }
};

class CommandDecr : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    int64_t ret;
    Redis::String string_db(svr->storage_, conn->GetNamespace());
    rocksdb::Status s = string_db.IncrBy(args_[1], -1, &ret);
    if (!s.ok()) return Status(Status::RedisExecErr, s.ToString());
    *output = Redis::Integer(ret);
    return Status::OK();
  }
};

class CommandIncrBy : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    try {
      increment_ = std::stoll(args[2]);
    } catch (std::exception &e) {
      return Status(Status::RedisParseErr, errValueNotInterger);
    }
    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    int64_t ret;
    Redis::String string_db(svr->storage_, conn->GetNamespace());
    rocksdb::Status s = string_db.IncrBy(args_[1], increment_, &ret);
    if (!s.ok()) return Status(Status::RedisExecErr, s.ToString());
    *output = Redis::Integer(ret);
    return Status::OK();
  }

 private:
  int64_t increment_ = 0;
};

class CommandIncrByFloat : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    try {
      increment_ = std::stod(args[2]);
    } catch (std::exception &e) {
      return Status(Status::RedisParseErr, errValueNotInterger);
    }
    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    double ret;
    Redis::String string_db(svr->storage_, conn->GetNamespace());
    rocksdb::Status s = string_db.IncrByFloat(args_[1], increment_, &ret);
    if (!s.ok()) return Status(Status::RedisExecErr, s.ToString());
    *output = Redis::BulkString(Util::Float2String(ret));
    return Status::OK();
  }

 private:
  double increment_ = 0;
};

class CommandDecrBy : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    try {
      increment_ = std::stoll(args[2]);
    } catch (std::exception &e) {
      return Status(Status::RedisParseErr, errValueNotInterger);
    }
    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    int64_t ret;
    Redis::String string_db(svr->storage_, conn->GetNamespace());
    rocksdb::Status s = string_db.IncrBy(args_[1], -1 * increment_, &ret);
    if (!s.ok()) return Status(Status::RedisExecErr, s.ToString());
    *output = Redis::Integer(ret);
    return Status::OK();
  }

 private:
  int64_t increment_ = 0;
};

class CommandDel : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    int cnt = 0;
    Redis::Database redis(svr->storage_, conn->GetNamespace());
    for (unsigned int i = 1; i < args_.size(); i++) {
      rocksdb::Status s = redis.Del(args_[i]);
      if (s.ok()) cnt++;
    }
    *output = Redis::Integer(cnt);
    return Status::OK();
  }
};

Status getBitOffsetFromArgument(std::string arg, uint32_t *offset) {
  int64_t offset_arg = 0;
  try {
    offset_arg = std::stoll(arg);
  } catch (std::exception &e) {
    return Status(Status::RedisParseErr, errValueNotInterger);
  }
  if (offset_arg < 0 || offset_arg > INT_MAX) {
    return Status(Status::RedisParseErr, "bit offset is out of range");
  }
  *offset = static_cast<uint32_t>(offset_arg);
  return Status::OK();
}

class CommandGetBit : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    Status s = getBitOffsetFromArgument(args[2], &offset_);
    if (!s.IsOK()) return s;
    return Commander::Parse(args);
  }
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    bool bit;
    Redis::Bitmap bitmap_db(svr->storage_, conn->GetNamespace());
    rocksdb::Status s = bitmap_db.GetBit(args_[1], offset_, &bit);
    if (!s.ok()) return Status(Status::RedisExecErr, s.ToString());
    *output = Redis::Integer(bit ? 1 : 0);
    return Status::OK();
  }
 private:
  uint32_t offset_ = 0;
};

class CommandSetBit : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    Status s = getBitOffsetFromArgument(args[2], &offset_);
    if (!s.IsOK()) return s;

    if (args[3] == "0") {
      bit_ = false;
    } else if (args[3] == "1") {
      bit_ = true;
    } else {
      return Status(Status::RedisParseErr, "bit is out of range");
    }
    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    bool old_bit;
    Redis::Bitmap bitmap_db(svr->storage_, conn->GetNamespace());
    rocksdb::Status s = bitmap_db.SetBit(args_[1], offset_, bit_, &old_bit);
    if (!s.ok()) return Status(Status::RedisExecErr, s.ToString());
    *output = Redis::Integer(old_bit ? 1 : 0);
    return Status::OK();
  }

 private:
  uint32_t offset_ = 0;
  bool bit_ = false;
};

class CommandBitCount : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    try {
      if (args.size() >= 3) start_ = std::stoi(args[2]);
      if (args.size() >= 4) stop_ = std::stoi(args[3]);
    } catch (std::exception &e) {
      return Status(Status::RedisParseErr, errValueNotInterger);
    }
    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    uint32_t cnt;
    Redis::Bitmap bitmap_db(svr->storage_, conn->GetNamespace());
    rocksdb::Status s = bitmap_db.BitCount(args_[1], start_, stop_, &cnt);
    if (!s.ok()) return Status(Status::RedisExecErr, s.ToString());
    *output = Redis::Integer(cnt);
    return Status::OK();
  }
 private:
  int start_ = 0, stop_ = -1;
};

class CommandBitPos: public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    try {
      if (args.size() >= 4) start_ = std::stoi(args[3]);
      if (args.size() >= 5) {
        stop_given_ = true;
        stop_ = std::stoi(args[4]);
      }
    } catch (std::exception &e) {
      return Status(Status::RedisParseErr, errValueNotInterger);
    }
    if (args[2] == "0") {
      bit_ = false;
    } else if (args[2] == "1") {
      bit_ = true;
    } else {
      return Status(Status::RedisParseErr, "bit should be 0 or 1");
    }
    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    int pos;
    Redis::Bitmap bitmap_db(svr->storage_, conn->GetNamespace());
    rocksdb::Status s = bitmap_db.BitPos(args_[1], bit_, start_, stop_, stop_given_, &pos);
    if (!s.ok()) return Status(Status::RedisExecErr, s.ToString());
    *output = Redis::Integer(pos);
    return Status::OK();
  }

 private:
  int start_ = 0, stop_ = -1;
  bool bit_ = false, stop_given_ = false;
};

class CommandType : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    Redis::Database redis(svr->storage_, conn->GetNamespace());
    RedisType type;
    rocksdb::Status s = redis.Type(args_[1], &type);
    if (s.ok()) {
      *output = Redis::BulkString(RedisTypeNames[type]);
      return Status::OK();
    }
    return Status(Status::RedisExecErr, s.ToString());
  }
};

class CommandObject : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    if (Util::ToLower(args_[1]) == "dump") {
      Redis::Database redis(svr->storage_, conn->GetNamespace());
      std::vector<std::string> infos;
      rocksdb::Status s = redis.Dump(args_[2], &infos);
      if (!s.ok()) {
        return Status(Status::RedisExecErr, s.ToString());
      }
      output->append(Redis::MultiLen(infos.size()));
      for (const auto &info : infos) {
        output->append(Redis::BulkString(info));
      }
    } else {
      *output = Redis::Error("object subcommand must be dump");
    }
    return Status::OK();
  }
};

class CommandTTL : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    Redis::Database redis(svr->storage_, conn->GetNamespace());
    int ttl;
    rocksdb::Status s = redis.TTL(args_[1], &ttl);
    if (s.ok()) {
      *output = Redis::Integer(ttl);
      return Status::OK();
    } else {
      return Status(Status::RedisExecErr, s.ToString());
    }
  }
};

class CommandPTTL : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    Redis::Database redis(svr->storage_, conn->GetNamespace());
    int ttl;
    rocksdb::Status s = redis.TTL(args_[1], &ttl);
    if (!s.ok()) return Status(Status::RedisExecErr, s.ToString());
    if (ttl > 0) {
      *output = Redis::Integer(ttl*1000);
    } else {
      *output = Redis::Integer(ttl);
    }
    return Status::OK();
  }
};

class CommandExists : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    int cnt = 0;
    Redis::Database redis(svr->storage_, conn->GetNamespace());
    std::vector<rocksdb::Slice> keys;
    for (unsigned i = 1; i < args_.size(); i++) {
      keys.emplace_back(args_[i]);
    }
    redis.Exists(keys, &cnt);
    *output = Redis::Integer(cnt);
    return Status::OK();
  }
};

class CommandExpire : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    int64_t now;
    rocksdb::Env::Default()->GetCurrentTime(&now);
    try {
      seconds_ = std::stoi(args[2]);
      if (seconds_ >= INT32_MAX - now) {
        return Status(Status::RedisParseErr, "the expire time was overflow");
      }
      seconds_ += now;
    } catch (std::exception &e) {
      return Status(Status::RedisParseErr, errValueNotInterger);
    }
    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    Redis::Database redis(svr->storage_, conn->GetNamespace());
    rocksdb::Status s = redis.Expire(args_[1], seconds_);
    if (s.ok()) {
      *output = Redis::Integer(1);
    } else {
      *output = Redis::Integer(0);
    }
    return Status::OK();
  }

 private:
  int seconds_ = 0;
};

class CommandPExpire : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    int64_t now;
    rocksdb::Env::Default()->GetCurrentTime(&now);
    try {
      auto ttl_ms = std::stol(args[2]);
      if (ttl_ms > 0 && ttl_ms < 1000) {
        seconds_ = 1;
      } else {
        seconds_ = ttl_ms / 1000;
        if (seconds_ >= INT32_MAX - now) {
          return Status(Status::RedisParseErr, "the expire time was overflow");
        }
      }
      seconds_ += now;
    } catch (std::exception &e) {
      return Status(Status::RedisParseErr, errValueNotInterger);
    }
    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    Redis::Database redis(svr->storage_, conn->GetNamespace());
    rocksdb::Status s = redis.Expire(args_[1], seconds_);
    if (s.ok()) {
      *output = Redis::Integer(1);
    } else {
      *output = Redis::Integer(0);
    }
    return Status::OK();
  }

 private:
  int seconds_ = 0;
};

class CommandExpireAt : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    try {
      timestamp_ = std::stoi(args[2]);
      if (timestamp_ >= INT32_MAX) {
        return Status(Status::RedisParseErr, "the expire time was overflow");
      }
    } catch (std::exception &e) {
      return Status(Status::RedisParseErr, errValueNotInterger);
    }
    return Commander::Parse(args);
  }
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    Redis::Database redis(svr->storage_, conn->GetNamespace());
    rocksdb::Status s = redis.Expire(args_[1], timestamp_);
    if (s.ok()) {
      *output = Redis::Integer(1);
    } else {
      *output = Redis::Integer(0);
    }
    return Status::OK();
  }

 private:
  int timestamp_ = 0;
};

class CommandPExpireAt : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    try {
      timestamp_ = static_cast<int>(std::stol(args[2])/1000);
      if (timestamp_ >= INT32_MAX) {
        return Status(Status::RedisParseErr, "the expire time was overflow");
      }
    } catch (std::exception &e) {
      return Status(Status::RedisParseErr, errValueNotInterger);
    }
    return Commander::Parse(args);
  }
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    Redis::Database redis(svr->storage_, conn->GetNamespace());
    rocksdb::Status s = redis.Expire(args_[1], timestamp_);
    if (s.ok()) {
      *output = Redis::Integer(1);
    } else {
      *output = Redis::Integer(0);
    }
    return Status::OK();
  }

 private:
  int timestamp_ = 0;
};

class CommandPersist : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    int ttl;
    Redis::Database redis(svr->storage_, conn->GetNamespace());
    rocksdb::Status s = redis.TTL(args_[1], &ttl);
    if (!s.ok()) return Status(Status::RedisExecErr, s.ToString());
    if (ttl == -1 || ttl == -2) {
      *output = Redis::Integer(0);
      return Status::OK();
    }
    s = redis.Expire(args_[1], 0);
    if (!s.ok()) return Status(Status::RedisExecErr, s.ToString());
    *output = Redis::Integer(1);
    return Status::OK();
  }
};

class CommandHGet : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    Redis::Hash hash_db(svr->storage_, conn->GetNamespace());
    std::string value;
    rocksdb::Status s = hash_db.Get(args_[1], args_[2], &value);
    if (!s.ok() && !s.IsNotFound()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    *output = s.IsNotFound() ? Redis::NilString() : Redis::BulkString(value);
    return Status::OK();
  }
};

class CommandHSet : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    int ret;
    Redis::Hash hash_db(svr->storage_, conn->GetNamespace());
    rocksdb::Status s = hash_db.Set(args_[1], args_[2], args_[3], &ret);
    if (!s.ok()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    *output = Redis::Integer(ret);
    return Status::OK();
  }
};

class CommandHSetNX : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    int ret;
    Redis::Hash hash_db(svr->storage_, conn->GetNamespace());
    rocksdb::Status s = hash_db.SetNX(args_[1], args_[2], args_[3], &ret);
    if (!s.ok()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    *output = Redis::Integer(ret);
    return Status::OK();
  }
};

class CommandHStrlen : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    Redis::Hash hash_db(svr->storage_, conn->GetNamespace());
    std::string value;
    rocksdb::Status s = hash_db.Get(args_[1], args_[2], &value);
    if (!s.ok() && !s.IsNotFound()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    *output = Redis::Integer(static_cast<int>(value.size()));
    return Status::OK();
  }
};

class CommandHDel : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    int ret;
    Redis::Hash hash_db(svr->storage_, conn->GetNamespace());
    std::vector<Slice> fields;
    for (unsigned int i = 2; i < args_.size(); i++) {
      fields.emplace_back(Slice(args_[i]));
    }
    rocksdb::Status s = hash_db.Delete(args_[1], fields, &ret);
    if (!s.ok()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    *output = Redis::Integer(ret);
    return Status::OK();
  }
};

class CommandHExists : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    Redis::Hash hash_db(svr->storage_, conn->GetNamespace());
    std::string value;
    rocksdb::Status s = hash_db.Get(args_[1], args_[2], &value);
    if (!s.ok() && !s.IsNotFound()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    *output = s.IsNotFound() ? Redis::Integer(0) : Redis::Integer(1);
    return Status::OK();
  }
};

class CommandHLen : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    uint32_t count;
    Redis::Hash hash_db(svr->storage_, conn->GetNamespace());
    rocksdb::Status s = hash_db.Size(args_[1], &count);
    if (!s.ok() && !s.IsNotFound()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    *output = s.IsNotFound() ? Redis::Integer(0) : Redis::Integer(count);
    return Status::OK();
  }
};

class CommandHIncrBy : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    try {
      increment_ = std::stoll(args[3]);
    } catch (std::exception &e) {
      return Status(Status::RedisParseErr, errValueNotInterger);
    }
    return Commander::Parse(args);
  }
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    int64_t ret;
    Redis::Hash hash_db(svr->storage_, conn->GetNamespace());
    rocksdb::Status s = hash_db.IncrBy(args_[1], args_[2], increment_, &ret);
    if (!s.ok()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    *output = Redis::Integer(ret);
    return Status::OK();
  }

 private:
  int64_t increment_ = 0;
};

class CommandHIncrByFloat : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    try {
      increment_ = std::stod(args[3]);
    } catch (std::exception &e) {
      return Status(Status::RedisParseErr, errValueNotInterger);
    }
    return Commander::Parse(args);
  }
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    double ret;
    Redis::Hash hash_db(svr->storage_, conn->GetNamespace());
    rocksdb::Status s = hash_db.IncrByFloat(args_[1], args_[2], increment_, &ret);
    if (!s.ok()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    *output = Redis::BulkString(Util::Float2String(ret));
    return Status::OK();
  }

 private:
  double increment_ = 0;
};

class CommandHMGet : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    Redis::Hash hash_db(svr->storage_, conn->GetNamespace());
    std::vector<Slice> fields;
    for (unsigned int i = 2; i < args_.size(); i++) {
      fields.emplace_back(Slice(args_[i]));
    }
    std::vector<std::string> values;
    std::vector<rocksdb::Status> statuses;
    rocksdb::Status s = hash_db.MGet(args_[1], fields, &values, &statuses);
    if (!s.ok() && !s.IsNotFound()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    if (s.IsNotFound()) {
      values.resize(fields.size(), "");
      *output = Redis::MultiBulkString(values);
    } else {
      *output = Redis::MultiBulkString(values, statuses);
    }
    return Status::OK();
  }
};

class CommandHMSet : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    if (args.size() % 2 != 0) {
      return Status(Status::RedisParseErr, errWrongNumOfArguments);
    }
    return Commander::Parse(args);
  }
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    int ret;
    Redis::Hash hash_db(svr->storage_, conn->GetNamespace());
    std::vector<FieldValue> field_values;
    for (unsigned int i = 2; i < args_.size(); i += 2) {
      field_values.push_back(FieldValue{args_[i], args_[i + 1]});
    }
    rocksdb::Status s = hash_db.MSet(args_[1], field_values, false, &ret);
    if (!s.ok()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    *output = Redis::SimpleString("OK");
    return Status::OK();
  }
};

class CommandHKeys : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    Redis::Hash hash_db(svr->storage_, conn->GetNamespace());
    std::vector<FieldValue> field_values;
    rocksdb::Status s = hash_db.GetAll(args_[1], &field_values, HashFetchType::kOnlyKey);
    if (!s.ok()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    std::vector<std::string> keys;
    for (const auto &fv : field_values) {
      keys.emplace_back(fv.field);
    }
    *output = Redis::MultiBulkString(keys);
    return Status::OK();
  }
};

class CommandHVals : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    Redis::Hash hash_db(svr->storage_, conn->GetNamespace());
    std::vector<FieldValue> field_values;
    rocksdb::Status s = hash_db.GetAll(args_[1], &field_values, HashFetchType::kOnlyValue);
    if (!s.ok()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    *output = "*" + std::to_string(field_values.size()) + CRLF;
    for (const auto &fv : field_values) {
      *output += Redis::BulkString(fv.value);
    }
    return Status::OK();
  }
};

class CommandHGetAll : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    Redis::Hash hash_db(svr->storage_, conn->GetNamespace());
    std::vector<FieldValue> field_values;
    rocksdb::Status s = hash_db.GetAll(args_[1], &field_values);
    if (!s.ok()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    *output = "*" + std::to_string(field_values.size() * 2) + CRLF;
    for (const auto &fv : field_values) {
      *output += Redis::BulkString(fv.field);
      *output += Redis::BulkString(fv.value);
    }
    return Status::OK();
  }
};

class CommandPush : public Commander {
 public:
  CommandPush(bool create_if_missing, bool left) {
    left_ = left;
    create_if_missing_ = create_if_missing;
  }
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    Redis::List list_db(svr->storage_, conn->GetNamespace());
    std::vector<Slice> elems;
    for (unsigned int i = 2; i < args_.size(); i++) {
      elems.emplace_back(args_[i]);
    }
    int ret;
    rocksdb::Status s;
    if (create_if_missing_) {
      s = list_db.Push(args_[1], elems, left_, &ret);
    } else {
      s = list_db.PushX(args_[1], elems, left_, &ret);
    }
    if (!s.ok()) {
      return Status(Status::RedisExecErr, s.ToString());
    }

    svr->WakeupBlockingConns(args_[1], elems.size());

    *output = Redis::Integer(ret);
    return Status::OK();
  }

 private:
  bool left_;
  bool create_if_missing_;
};

class CommandLPush : public CommandPush {
 public:
  CommandLPush() : CommandPush(true, true) {}
};

class CommandRPush : public CommandPush {
 public:
  CommandRPush() : CommandPush(true, false) {}
};

class CommandLPushX : public CommandPush {
 public:
  CommandLPushX() : CommandPush(false, true) {}
};

class CommandRPushX : public CommandPush {
 public:
  CommandRPushX() : CommandPush(false, false) {}
};

class CommandPop : public Commander {
 public:
  explicit CommandPop(bool left) { left_ = left; }
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    Redis::List list_db(svr->storage_, conn->GetNamespace());
    std::string elem;
    rocksdb::Status s = list_db.Pop(args_[1], &elem, left_);
    if (!s.ok() && !s.IsNotFound()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    if (s.IsNotFound()) {
      *output = Redis::NilString();
    } else {
      *output = Redis::BulkString(elem);
    }
    return Status::OK();
  }

 private:
  bool left_;
};

class CommandLPop : public CommandPop {
 public:
  CommandLPop() : CommandPop(true) {}
};

class CommandRPop : public CommandPop {
 public:
  CommandRPop() : CommandPop(false) {}
};

class CommandBPop : public Commander {
 public:
  explicit CommandBPop(bool left) { left_ = left; }
  ~CommandBPop() {
    if (timer_ != nullptr) {
      event_free(timer_);
      timer_ = nullptr;
    }
  }

  Status Parse(const std::vector<std::string> &args) override {
    try {
      timeout_ = std::stoi(args[args.size() - 1]);
      if (timeout_ < 0) {
        return Status(Status::RedisParseErr, "timeout should not be negative");
      }
    } catch (std::exception &e) {
      return Status(Status::RedisParseErr, "timeout is not an integer or out of range");
    }
    keys_ = std::vector<std::string>(args.begin() + 1, args.end() - 1);
    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    svr_ = svr;
    conn_ = conn;

    auto bev = conn->GetBufferEvent();
    auto s = TryPopFromList();
    if (s.ok() || !s.IsNotFound()) {
      return Status::OK();  // error has already output in TryPopFromList
    }
    for (const auto &key : keys_) {
      svr_->AddBlockingKey(key, conn_);
    }
    bufferevent_setcb(bev, nullptr, WriteCB, EventCB, this);
    if (timeout_) {
      timer_ = evtimer_new(bufferevent_get_base(bev), TimerCB, this);
      timeval tm = {timeout_, 0};
      evtimer_add(timer_, &tm);
    }
    return Status::OK();
  }

  rocksdb::Status TryPopFromList() {
    Redis::List list_db(svr_->storage_, conn_->GetNamespace());
    std::string last_key, elem;
    rocksdb::Status s;
    for (const auto &key : keys_) {
     last_key = key;
      s = list_db.Pop(key, &elem, left_);
      if (s.ok() || !s.IsNotFound()) {
        break;
      }
    }
    if (s.ok()) {
      conn_->Reply(Redis::MultiBulkString({last_key, elem}));
    } else if (!s.IsNotFound()) {
      conn_->Reply(Redis::Error("ERR " + s.ToString()));
      LOG(ERROR) << "Failed to execute redis command: " << conn_->current_cmd_->Name()
                 << ", err: " << s.ToString();
    }
    return s;
  }

  static void WriteCB(bufferevent *bev, void *ctx) {
    auto self = reinterpret_cast<CommandBPop *>(ctx);
    auto s = self->TryPopFromList();
    // if pop fail ,currently we compromised to close bpop request
    if (s.IsNotFound()) {
      self->conn_->Reply(Redis::NilString());
      LOG(ERROR) << "[BPOP] Failed to execute redis command: " << self->conn_->current_cmd_->Name()
                 << ", err: another concurrent pop request must have stole the data before this bpop request"
                 << " or bpop is in a pipeline cmd list(cmd before bpop replyed trigger this writecb)";
    }
    if (self->timer_ != nullptr) {
      event_free(self->timer_);
      self->timer_ = nullptr;
    }
    self->unBlockingAll();
    bufferevent_setcb(bev, Redis::Connection::OnRead, Redis::Connection::OnWrite,
                      Redis::Connection::OnEvent, self->conn_);
    bufferevent_enable(bev, EV_READ);
  }

  static void EventCB(bufferevent *bev, int16_t events, void *ctx) {
    auto self = static_cast<CommandBPop *>(ctx);
    if (events & (BEV_EVENT_EOF | BEV_EVENT_ERROR)) {
      if (self->timer_ != nullptr) {
        event_free(self->timer_);
        self->timer_ = nullptr;
      }
      self->unBlockingAll();
    }
    Redis::Connection::OnEvent(bev, events, self->conn_);
  }

  static void TimerCB(int, int16_t events, void *ctx) {
    auto self = reinterpret_cast<CommandBPop *>(ctx);
    self->conn_->Reply(Redis::NilString());
    event_free(self->timer_);
    self->timer_ = nullptr;
    self->unBlockingAll();
    auto bev = self->conn_->GetBufferEvent();
    bufferevent_setcb(bev, Redis::Connection::OnRead, Redis::Connection::OnWrite,
                      Redis::Connection::OnEvent, self->conn_);
    bufferevent_enable(bev, EV_READ);
  }

 private:
  bool left_ = false;
  int timeout_ = 0;  // second
  std::vector<std::string> keys_;
  Server *svr_ = nullptr;
  Connection *conn_ = nullptr;
  event *timer_ = nullptr;

  void unBlockingAll() {
    for (const auto &key : keys_) {
      svr_->UnBlockingKey(key, conn_);
    }
  }
};

class CommandBLPop : public CommandBPop {
 public:
  CommandBLPop() : CommandBPop(true) {}
};

class CommandBRPop : public CommandBPop {
 public:
  CommandBRPop() : CommandBPop(false) {}
};

class CommandLRem : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    try {
      count_ = std::stoi(args[2]);
    } catch (std::exception &e) {
      return Status(Status::RedisParseErr, errValueNotInterger);
    }

    return Commander::Parse(args);
  }
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    int ret;
    Redis::List list_db(svr->storage_, conn->GetNamespace());
    rocksdb::Status s = list_db.Rem(args_[1], count_, args_[3], &ret);
    if (!s.ok() && !s.IsNotFound()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    *output = Redis::Integer(ret);
    return Status::OK();
  }

 private:
  int count_ = 0;
};

class CommandLInsert : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    if ((Util::ToLower(args[2]) == "before")) {
      before_ = true;
    } else if ((Util::ToLower(args[2]) == "after")) {
      before_ = false;
    } else {
      return Status(Status::RedisParseErr, errInvalidSyntax);
    }
    return Commander::Parse(args);
  }
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    int ret;
    Redis::List list_db(svr->storage_, conn->GetNamespace());
    rocksdb::Status s = list_db.Insert(args_[1], args_[3], args_[4], before_, &ret);
    if (!s.ok() && !s.IsNotFound()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    *output = Redis::Integer(ret);
    return Status::OK();
  }

 private:
  bool before_ = false;
};

class CommandLRange : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    try {
      start_ = std::stoi(args[2]);
      stop_ = std::stoi(args[3]);
    } catch (std::exception &e) {
      return Status(Status::RedisParseErr, errValueNotInterger);
    }
    return Commander::Parse(args);
  }
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    Redis::List list_db(svr->storage_, conn->GetNamespace());
    std::vector<std::string> elems;
    rocksdb::Status s = list_db.Range(args_[1], start_, stop_, &elems);
    if (!s.ok() && !s.IsNotFound()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    *output = Redis::MultiBulkString(elems, false);
    return Status::OK();
  }

 private:
  int start_ = 0, stop_ = 0;
};

class CommandLLen : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    Redis::List list_db(svr->storage_, conn->GetNamespace());
    uint32_t count;
    rocksdb::Status s = list_db.Size(args_[1], &count);
    if (!s.ok() && !s.IsNotFound()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    *output = Redis::Integer(count);
    return Status::OK();
  }
};

class CommandLIndex : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    try {
      index_ = std::stoi(args[2]);
    } catch (std::exception &e) {
      return Status(Status::RedisParseErr, errValueNotInterger);
    }
    return Commander::Parse(args);
  }
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    Redis::List list_db(svr->storage_, conn->GetNamespace());
    std::string elem;
    rocksdb::Status s = list_db.Index(args_[1], index_, &elem);
    if (!s.ok() && !s.IsNotFound()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    if (s.IsNotFound()) {
      *output = Redis::NilString();
    } else {
      *output = Redis::BulkString(elem);
    }
    return Status::OK();
  }

 private:
  int index_ = 0;
};

class CommandLSet : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    try {
      index_ = std::stoi(args[2]);
    } catch (std::exception &e) {
      return Status(Status::RedisParseErr, errValueNotInterger);
    }
    return Commander::Parse(args);
  }
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    Redis::List list_db(svr->storage_, conn->GetNamespace());
    rocksdb::Status s = list_db.Set(args_[1], index_, args_[3]);
    if (!s.ok()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    *output = Redis::SimpleString("OK");
    return Status::OK();
  }

 private:
  int index_ = 0;
};

class CommandLTrim : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    try {
      start_ = std::stoi(args[2]);
      stop_ = std::stoi(args[3]);
    } catch (std::exception &e) {
      return Status(Status::RedisParseErr, errValueNotInterger);
    }
    return Commander::Parse(args);
  }
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    Redis::List list_db(svr->storage_, conn->GetNamespace());
    rocksdb::Status s = list_db.Trim(args_[1], start_, stop_);
    if (!s.ok()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    *output = Redis::SimpleString("OK");
    return Status::OK();
  }

 private:
  int start_ = 0, stop_ = 0;
};

class CommandRPopLPUSH : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    Redis::List list_db(svr->storage_, conn->GetNamespace());
    std::string elem;
    rocksdb::Status s = list_db.RPopLPush(args_[1], args_[2], &elem);
    if (!s.ok() && !s.IsNotFound()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    *output = s.IsNotFound() ? Redis::NilString() : Redis::BulkString(elem);
    return Status::OK();
  }
};

class CommandSAdd : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    Redis::Set set_db(svr->storage_, conn->GetNamespace());
    std::vector<Slice> members;
    for (unsigned int i = 2; i < args_.size(); i++) {
      members.emplace_back(args_[i]);
    }
    int ret;
    rocksdb::Status s = set_db.Add(args_[1], members, &ret);
    if (!s.ok()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    *output = Redis::Integer(ret);
    return Status::OK();
  }
};

class CommandSRem : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    Redis::Set set_db(svr->storage_, conn->GetNamespace());
    std::vector<Slice> members;
    for (unsigned int i = 2; i < args_.size(); i++) {
      members.emplace_back(args_[i]);
    }
    int ret;
    rocksdb::Status s = set_db.Remove(args_[1], members, &ret);
    if (!s.ok()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    *output = Redis::Integer(ret);
    return Status::OK();
  }
};

class CommandSCard : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    Redis::Set set_db(svr->storage_, conn->GetNamespace());
    int ret;
    rocksdb::Status s = set_db.Card(args_[1], &ret);
    if (!s.ok()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    *output = Redis::Integer(ret);
    return Status::OK();
  }
};

class CommandSMembers : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    Redis::Set set_db(svr->storage_, conn->GetNamespace());
    std::vector<std::string> members;
    rocksdb::Status s = set_db.Members(args_[1], &members);
    if (!s.ok()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    *output = Redis::MultiBulkString(members, false);
    return Status::OK();
  }
};

class CommandSIsMember : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    Redis::Set set_db(svr->storage_, conn->GetNamespace());
    int ret;
    rocksdb::Status s = set_db.IsMember(args_[1], args_[2], &ret);
    if (!s.ok()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    *output = Redis::Integer(ret);
    return Status::OK();
  }
};

class CommandSPop : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    try {
      if (args.size() == 3) {
        count_ = std::stoi(args[2]);
      }
    } catch (std::exception &e) {
      return Status(Status::RedisParseErr, errValueNotInterger);
    }
    return Commander::Parse(args);
  }
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    Redis::Set set_db(svr->storage_, conn->GetNamespace());
    std::vector<std::string> members;
    rocksdb::Status s = set_db.Take(args_[1], &members, count_, true);
    if (!s.ok()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    *output = Redis::MultiBulkString(members, false);
    return Status::OK();
  }

 private:
  int count_ = 1;
};

class CommandSRandMember : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    try {
      if (args.size() == 3) {
        count_ = std::stoi(args[2]);
      }
    } catch (std::exception &e) {
      return Status(Status::RedisParseErr, errValueNotInterger);
    }
    return Commander::Parse(args);
  }
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    Redis::Set set_db(svr->storage_, conn->GetNamespace());
    std::vector<std::string> members;
    rocksdb::Status s = set_db.Take(args_[1], &members, count_, false);
    if (!s.ok()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    *output = Redis::MultiBulkString(members, false);
    return Status::OK();
  }

 private:
  int count_ = 1;
};

class CommandSMove : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    Redis::Set set_db(svr->storage_, conn->GetNamespace());
    int ret;
    rocksdb::Status s = set_db.Move(args_[1], args_[2], args_[3], &ret);
    if (!s.ok()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    *output = Redis::Integer(ret);
    return Status::OK();
  }
};

class CommandSDiff : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    std::vector<Slice> keys;
    for (size_t i = 1; i < args_.size(); i++) {
      keys.emplace_back(args_[i]);
    }
    std::vector<std::string> members;
    Redis::Set set_db(svr->storage_, conn->GetNamespace());
    auto s = set_db.Diff(keys, &members);
    if (!s.ok()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    *output = Redis::MultiBulkString(members, false);
    return Status::OK();
  }
};

class CommandSUnion : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    std::vector<Slice> keys;
    for (size_t i = 1; i < args_.size(); i++) {
      keys.emplace_back(args_[i]);
    }
    std::vector<std::string> members;
    Redis::Set set_db(svr->storage_, conn->GetNamespace());
    auto s = set_db.Union(keys, &members);
    if (!s.ok()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    *output = Redis::MultiBulkString(members, false);
    return Status::OK();
  }
};

class CommandSInter : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    std::vector<Slice> keys;
    for (size_t i = 1; i < args_.size(); i++) {
      keys.emplace_back(args_[i]);
    }
    std::vector<std::string> members;
    Redis::Set set_db(svr->storage_, conn->GetNamespace());
    auto s = set_db.Inter(keys, &members);
    if (!s.ok()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    *output = Redis::MultiBulkString(members, false);
    return Status::OK();
  }
};

class CommandSDiffStore: public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    int ret = 0;
    std::vector<Slice> keys;
    for (size_t i = 2; i < args_.size(); i++) {
      keys.emplace_back(args_[i]);
    }
    Redis::Set set_db(svr->storage_, conn->GetNamespace());
    auto s = set_db.DiffStore(args_[1], keys, &ret);
    if (!s.ok()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    *output = Redis::Integer(ret);
    return Status::OK();
  }
};

class CommandSUnionStore: public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    int ret = 0;
    std::vector<Slice> keys;
    for (size_t i = 2; i < args_.size(); i++) {
      keys.emplace_back(args_[i]);
    }
    Redis::Set set_db(svr->storage_, conn->GetNamespace());
    auto s = set_db.UnionStore(args_[1], keys, &ret);
    if (!s.ok()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    *output = Redis::Integer(ret);
    return Status::OK();
  }
};

class CommandSInterStore: public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    int ret = 0;
    std::vector<Slice> keys;
    for (size_t i = 2; i < args_.size(); i++) {
      keys.emplace_back(args_[i]);
    }
    Redis::Set set_db(svr->storage_, conn->GetNamespace());
    auto s = set_db.InterStore(args_[1], keys, &ret);
    if (!s.ok()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    *output = Redis::Integer(ret);
    return Status::OK();
  }
};

class CommandZAdd : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    if (args.size() % 2 != 0) {
      return Status(Status::RedisParseErr, errInvalidSyntax);
    }

    try {
      for (unsigned i = 2; i < args.size(); i += 2) {
        double score = std::stod(args[i]);
        if (std::isnan(score)) {
          return Status(Status::RedisParseErr, "ERR score is not a valid float");
        }
        member_scores_.emplace_back(MemberScore{args[i + 1], score});
      }
    } catch (const std::exception &e) {
      return Status(Status::RedisParseErr, "ERR value is not a valid float");
    }
    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    int ret;
    Redis::ZSet zset_db(svr->storage_, conn->GetNamespace());
    rocksdb::Status s = zset_db.Add(args_[1], 0, &member_scores_, &ret);
    if (!s.ok()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    *output = Redis::Integer(ret);
    return Status::OK();
  }

 private:
  std::vector<MemberScore> member_scores_;
};

class CommandZCount : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    Status s = Redis::ZSet::ParseRangeSpec(args[2], args[3], &spec_);
    if (!s.IsOK()) {
      return Status(Status::RedisParseErr, s.Msg());
    }
    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    int ret;

    Redis::ZSet zset_db(svr->storage_, conn->GetNamespace());
    rocksdb::Status s = zset_db.Count(args_[1], spec_, &ret);
    if (!s.ok()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    *output = Redis::Integer(ret);
    return Status::OK();
  }

 private:
  ZRangeSpec spec_;
};

class CommandZCard : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    int ret;

    Redis::ZSet zset_db(svr->storage_, conn->GetNamespace());
    rocksdb::Status s = zset_db.Card(args_[1], &ret);
    if (!s.ok() && !s.IsNotFound()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    *output = Redis::Integer(ret);
    return Status::OK();
  }
};

class CommandZIncrBy : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    try {
      incr_ = std::stod(args[2]);
    } catch (const std::exception &e) {
      return Status(Status::RedisParseErr, "value is not an double or out of range");
    }
    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    double score;

    Redis::ZSet zset_db(svr->storage_, conn->GetNamespace());
    rocksdb::Status s = zset_db.IncrBy(args_[1], args_[3], incr_, &score);
    if (!s.ok()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    *output = Redis::BulkString(Util::Float2String(score));
    return Status::OK();
  }

 private:
  double incr_ = 0.0;
};

class CommandZLexCount : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    Status s = Redis::ZSet::ParseRangeLexSpec(args[2], args[3], &spec_);
    if (!s.IsOK()) {
      return Status(Status::RedisParseErr, s.Msg());
    }
    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    int size;
    Redis::ZSet zset_db(svr->storage_, conn->GetNamespace());
    rocksdb::Status s = zset_db.RangeByLex(args_[1], spec_, nullptr, &size);
    if (!s.ok()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    *output = Redis::Integer(size);
    return Status::OK();
  }

 private:
  ZRangeLexSpec spec_;
};

class CommandZPop : public Commander {
 public:
  explicit CommandZPop(bool min) : min_(min) {}

  Status Parse(const std::vector<std::string> &args) override {
    if (args.size() > 2) {
      try {
        count_ = std::stoi(args[2]);
      } catch (const std::exception &e) {
        return Status(Status::RedisParseErr, errValueNotInterger);
      }
    }
    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    Redis::ZSet zset_db(svr->storage_, conn->GetNamespace());
    std::vector<MemberScore> memeber_scores;
    rocksdb::Status s = zset_db.Pop(args_[1], count_, min_, &memeber_scores);
    if (!s.ok()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    output->append(Redis::MultiLen(memeber_scores.size() * 2));
    for (const auto &ms : memeber_scores) {
      output->append(Redis::BulkString(ms.member));
      output->append(Redis::BulkString(Util::Float2String(ms.score)));
    }
    return Status::OK();
  }

 private:
  bool min_;
  int count_ = 1;
};

class CommandZPopMin : public CommandZPop {
 public:
  CommandZPopMin() : CommandZPop(true) {}
};

class CommandZPopMax : public CommandZPop {
 public:
  CommandZPopMax() : CommandZPop(false) {}
};

class CommandZRange : public Commander {
 public:
  explicit CommandZRange(bool reversed = false) { reversed_ = reversed; }
  Status Parse(const std::vector<std::string> &args) override {
    try {
      start_ = std::stoi(args[2]);
      stop_ = std::stoi(args[3]);
    } catch (const std::exception &e) {
      return Status(Status::RedisParseErr, errValueNotInterger);
    }
    if (args.size() > 4 && (Util::ToLower(args[4]) == "withscores")) {
      with_scores_ = true;
    }
    return Commander::Parse(args);
  }
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    Redis::ZSet zset_db(svr->storage_, conn->GetNamespace());
    std::vector<MemberScore> memeber_scores;
    uint8_t flags = !reversed_ ? 0 : ZSET_REVERSED;
    rocksdb::Status s =
        zset_db.Range(args_[1], start_, stop_, flags, &memeber_scores);
    if (!s.ok()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    if (!with_scores_) {
      output->append(Redis::MultiLen(memeber_scores.size()));
    } else {
      output->append(Redis::MultiLen(memeber_scores.size() * 2));
    }
    for (const auto &ms : memeber_scores) {
      output->append(Redis::BulkString(ms.member));
      if (with_scores_)
        output->append(Redis::BulkString(Util::Float2String(ms.score)));
    }
    return Status::OK();
  }

 private:
  int start_ = 0;
  int stop_ = 0;
  bool reversed_;
  bool with_scores_ = false;
};

class CommandZRevRange : public CommandZRange {
 public:
  CommandZRevRange() : CommandZRange(true) {}
};

class CommandZRangeByLex : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    Status s = Redis::ZSet::ParseRangeLexSpec(args[2], args[3], &spec_);
    if (!s.IsOK()) {
      return Status(Status::RedisParseErr, s.Msg());
    }
    try {
      if (args.size() == 7 && Util::ToLower(args[4]) == "limit") {
        spec_.offset = std::stoi(args[5]);
        spec_.count = std::stoi(args[6]);
      }
    } catch (const std::exception &e) {
      return Status(Status::RedisParseErr, errValueNotInterger);
    }
    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    int size;
    Redis::ZSet zset_db(svr->storage_, conn->GetNamespace());
    std::vector<std::string> members;
    rocksdb::Status s = zset_db.RangeByLex(args_[1], spec_, &members, &size);
    if (!s.ok()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    *output = Redis::MultiBulkString(members, false);
    return Status::OK();
  }

 private:
  ZRangeLexSpec spec_;
};

class CommandZRangeByScore : public Commander {
 public:
  explicit CommandZRangeByScore(bool reversed = false) {
    spec_.reversed = reversed;
  }
  Status Parse(const std::vector<std::string> &args) override {
    Status s;
    if (spec_.reversed) {
      s = Redis::ZSet::ParseRangeSpec(args[3], args[2], &spec_);
    } else {
      s = Redis::ZSet::ParseRangeSpec(args[2], args[3], &spec_);
    }
    if (!s.IsOK()) {
      return Status(Status::RedisParseErr, s.Msg());
    }
    try {
      size_t i = 4;
      while (i < args.size()) {
        if (Util::ToLower(args[i]) == "withscores") {
          with_scores_ = true;
          i++;
        } else if (Util::ToLower(args[i]) == "limit" && i + 2 < args.size()) {
          spec_.offset = std::stoi(args[i + 1]);
          spec_.count = std::stoi(args[i + 2]);
          i += 3;
        } else {
          return Status(Status::RedisParseErr, errInvalidSyntax);
        }
      }
    } catch (const std::exception &e) {
      return Status(Status::RedisParseErr, errValueNotInterger);
    }
    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    int size;
    Redis::ZSet zset_db(svr->storage_, conn->GetNamespace());
    std::vector<MemberScore> memeber_scores;
    rocksdb::Status s =
        zset_db.RangeByScore(args_[1], spec_, &memeber_scores, &size);
    if (!s.ok()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    if (!with_scores_) {
      output->append(Redis::MultiLen(memeber_scores.size()));
    } else {
      output->append(Redis::MultiLen(memeber_scores.size() * 2));
    }
    for (const auto &ms : memeber_scores) {
      output->append(Redis::BulkString(ms.member));
      if (with_scores_)
        output->append(Redis::BulkString(Util::Float2String(ms.score)));
    }
    return Status::OK();
  }

 private:
  ZRangeSpec spec_;
  bool with_scores_ = false;
};

class CommandZRank : public Commander {
 public:
  explicit CommandZRank(bool reversed = false)
      : reversed_(reversed) {}
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    int rank;
    Redis::ZSet zset_db(svr->storage_, conn->GetNamespace());
    rocksdb::Status s = zset_db.Rank(args_[1], args_[2], reversed_, &rank);
    if (!s.ok()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    if (rank == -1) {
      *output = Redis::NilString();
    } else {
      *output = Redis::Integer(rank);
    }
    return Status::OK();
  }

 private:
  bool reversed_;
};

class CommandZRevRank : public CommandZRank {
 public:
  CommandZRevRank() : CommandZRank(true) {}
};

class CommandZRevRangeByScore : public CommandZRangeByScore {
 public:
  CommandZRevRangeByScore() : CommandZRangeByScore(true) {}
};

class CommandZRem : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    int size;
    Redis::ZSet zset_db(svr->storage_, conn->GetNamespace());
    std::vector<rocksdb::Slice> members;
    for (unsigned i = 2; i < args_.size(); i++) {
      members.emplace_back(args_[i]);
    }
    rocksdb::Status s = zset_db.Remove(args_[1], members, &size);
    if (!s.ok()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    *output = Redis::Integer(size);
    return Status::OK();
  }
};

class CommandZRemRangeByRank : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    try {
      start_ = std::stoi(args[2]);
      stop_ = std::stoi(args[3]);
    } catch (const std::exception &e) {
      return Status(Status::RedisParseErr, errValueNotInterger);
    }
    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    int ret;

    Redis::ZSet zset_db(svr->storage_, conn->GetNamespace());
    rocksdb::Status s =
        zset_db.RemoveRangeByRank(args_[1], start_, stop_, &ret);
    if (!s.ok()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    *output = Redis::Integer(ret);
    return Status::OK();
  }

 private:
  int start_ = 0;
  int stop_ = 0;
};

class CommandZRemRangeByScore : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    Status s = Redis::ZSet::ParseRangeSpec(args[2], args[3], &spec_);
    if (!s.IsOK()) {
      return Status(Status::RedisParseErr, s.Msg());
    }
    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    int size;
    Redis::ZSet zset_db(svr->storage_, conn->GetNamespace());
    rocksdb::Status s = zset_db.RemoveRangeByScore(args_[1], spec_, &size);
    if (!s.ok()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    *output = Redis::Integer(size);
    return Status::OK();
  }

 private:
  ZRangeSpec spec_;
};

class CommandZRemRangeByLex : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    Status s = Redis::ZSet::ParseRangeLexSpec(args[2], args[3], &spec_);
    if (!s.IsOK()) {
      return Status(Status::RedisParseErr, s.Msg());
    }
    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    int size;
    Redis::ZSet zset_db(svr->storage_, conn->GetNamespace());
    rocksdb::Status s = zset_db.RemoveRangeByLex(args_[1], spec_, &size);
    if (!s.ok()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    *output = Redis::Integer(size);
    return Status::OK();
  }

 private:
  ZRangeLexSpec spec_;
};

class CommandZScore : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    double score;
    Redis::ZSet zset_db(svr->storage_, conn->GetNamespace());
    rocksdb::Status s = zset_db.Score(args_[1], args_[2], &score);
    if (!s.ok() && !s.IsNotFound()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    if (s.IsNotFound()) {
      *output = Redis::NilString();
    } else {
      *output = Redis::BulkString(Util::Float2String(score));
    }
    return Status::OK();
  }
};

class CommandZMScore : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    Redis::ZSet zset_db(svr->storage_, conn->GetNamespace());
    std::vector<Slice> members;
    for (unsigned int i = 2; i < args_.size(); i++) {
      members.emplace_back(Slice(args_[i]));
    }
    std::map<std::string, double> mscores;
    rocksdb::Status s = zset_db.MGet(args_[1], members, &mscores);
    if (!s.ok() && !s.IsNotFound()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    std::vector<std::string> values;
    if (s.IsNotFound()) {
      values.resize(members.size(), "");
    } else {
      for (const auto &member : members) {
        auto iter = mscores.find(member.ToString());
        if (iter == mscores.end()) {
          values.emplace_back("");
        } else {
          values.emplace_back(Util::Float2String(iter->second));
        }
      }
    }
    *output = Redis::MultiBulkString(values);
    return Status::OK();
  }
};

class CommandZUnionStore : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    try {
      numkeys_ = std::stoi(args[2]);
    } catch (const std::exception &e) {
      return Status(Status::RedisParseErr, errValueNotInterger);
    }
    if (numkeys_ > args.size() - 3) {
      return Status(Status::RedisParseErr, errInvalidSyntax);
    }
    size_t j = 0;
    while (j < numkeys_) {
      keys_weights_.emplace_back(KeyWeight{args[j + 3], 1});
      j++;
    }
    size_t i = 3 + numkeys_;
    while (i < args.size()) {
      if (Util::ToLower(args[i]) == "aggregate" && i + 1 < args.size()) {
        if (Util::ToLower(args[i + 1]) == "sum") {
          aggregate_method_ = kAggregateSum;
        } else if (Util::ToLower(args[i + 1]) == "min") {
          aggregate_method_ = kAggregateMin;
        } else if (Util::ToLower(args[i + 1]) == "max") {
          aggregate_method_ = kAggregateMax;
        } else {
          return Status(Status::RedisParseErr, "aggregate para error");
        }
        i += 2;
      } else if (Util::ToLower(args[i]) == "weights" && i + numkeys_ < args.size()) {
        size_t j = 0;
        while (j < numkeys_) {
          try {
            keys_weights_[j].weight = std::stod(args[i + j + 1]);
          } catch (const std::exception &e) {
            return Status(Status::RedisParseErr, "weight is not an double or out of range");
          }
          if (std::isnan(keys_weights_[j].weight)) {
            return Status(Status::RedisParseErr, "weight is not an double or out of range");
          }
          j++;
        }
        i += numkeys_ + 1;
      } else {
        return Status(Status::RedisParseErr, errInvalidSyntax);
      }
    }
    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    int size;
    Redis::ZSet zset_db(svr->storage_, conn->GetNamespace());
    rocksdb::Status s = zset_db.UnionStore(args_[1], keys_weights_, aggregate_method_, &size);
    if (!s.ok()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    *output = Redis::Integer(size);
    return Status::OK();
  }

 protected:
  size_t numkeys_ = 0;
  std::vector<KeyWeight> keys_weights_;
  AggregateMethod aggregate_method_ = kAggregateSum;
};

class CommandZInterStore : public CommandZUnionStore {
 public:
  CommandZInterStore() : CommandZUnionStore() {}

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    int size;
    Redis::ZSet zset_db(svr->storage_, conn->GetNamespace());
    rocksdb::Status s = zset_db.InterStore(args_[1], keys_weights_, aggregate_method_, &size);
    if (!s.ok()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    *output = Redis::Integer(size);
    return Status::OK();
  }
};

class CommandGeoBase : public Commander {
 public:
  Status ParseDistanceUnit(const std::string &param) {
    if (Util::ToLower(param) == "m") {
      distance_unit_ = kDistanceMeter;
    } else if (Util::ToLower(param) == "km") {
      distance_unit_ = kDistanceKilometers;
    } else if (Util::ToLower(param) == "ft") {
      distance_unit_ = kDistanceFeet;
    } else if (Util::ToLower(param) == "mi") {
      distance_unit_ = kDistanceMiles;
    } else {
      return Status(Status::RedisParseErr, "distance unit para error");
    }
    return Status::OK();
  }

  Status ParseLongLat(const std::string &longitude_para,
                      const std::string &latitude_para,
                      double *longitude,
                      double *latitude) {
    try {
      *longitude = std::stod(longitude_para);
      *latitude = std::stod(latitude_para);
    } catch (const std::exception &e) {
      return Status(Status::RedisParseErr, "ERR value is not a valid float");
    }
    if (*longitude < GEO_LONG_MIN || *longitude > GEO_LONG_MAX ||
        *latitude < GEO_LAT_MIN || *latitude > GEO_LAT_MAX) {
      return Status(Status::RedisParseErr, "invalid longitude,latitude pair " + longitude_para + "," + latitude_para);
    }
    return Status::OK();
  }

  double GetDistanceByUnit(double distance) {
    return distance / GetUnitConversion();
  }

  double GetRadiusMeters(double radius) {
    return radius * GetUnitConversion();
  }

  double GetUnitConversion() {
    double conversion = 0;
    switch (distance_unit_) {
      case kDistanceMeter:conversion = 1;
        break;
      case kDistanceKilometers:conversion = 1000;
        break;
      case kDistanceFeet:conversion = 0.3048;
        break;
      case kDistanceMiles:conversion = 1609.34;
        break;
    }
    return conversion;
  }

 protected:
  DistanceUnit distance_unit_ = kDistanceMeter;
};

class CommandGeoAdd : public CommandGeoBase {
 public:
  CommandGeoAdd() : CommandGeoBase() {}
  Status Parse(const std::vector<std::string> &args) override {
    if ((args.size() - 5) % 3 != 0) {
      return Status(Status::RedisParseErr, errWrongNumOfArguments);
    }
    for (unsigned i = 2; i < args.size(); i += 3) {
      double longitude = 0, latitude = 0;
      auto s = ParseLongLat(args[i], args[i + 1], &longitude, &latitude);
      if (!s.IsOK()) return s;
      geo_points_.emplace_back(GeoPoint{longitude, latitude, args[i + 2]});
    }
    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    int ret;
    Redis::Geo geo_db(svr->storage_, conn->GetNamespace());
    rocksdb::Status s = geo_db.Add(args_[1], &geo_points_, &ret);
    if (!s.ok()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    *output = Redis::Integer(ret);
    return Status::OK();
  }

 private:
  std::vector<GeoPoint> geo_points_;
};

class CommandGeoDist : public CommandGeoBase {
 public:
  CommandGeoDist() : CommandGeoBase() {}
  Status Parse(const std::vector<std::string> &args) override {
    if (args.size() == 5) {
      auto s = ParseDistanceUnit(args[4]);
      if (!s.IsOK()) return s;
    }
    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    double distance;
    Redis::Geo geo_db(svr->storage_, conn->GetNamespace());
    rocksdb::Status s = geo_db.Dist(args_[1], args_[2], args_[3], &distance);
    if (!s.ok() && !s.IsNotFound()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    if (s.IsNotFound()) {
      *output = Redis::NilString();
    } else {
      *output = Redis::BulkString(Util::Float2String(GetDistanceByUnit(distance)));
    }
    return Status::OK();
  }
};

class CommandGeoHash : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    for (unsigned i = 2; i < args.size(); i++) {
      members_.emplace_back(args[i]);
    }
    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    std::vector<std::string> hashes;
    Redis::Geo geo_db(svr->storage_, conn->GetNamespace());
    rocksdb::Status s = geo_db.Hash(args_[1], members_, &hashes);
    if (!s.ok()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    *output = Redis::MultiBulkString(hashes);
    return Status::OK();
  }

 private:
  std::vector<Slice> members_;
};

class CommandGeoPos : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    for (unsigned i = 2; i < args.size(); i++) {
      members_.emplace_back(args[i]);
    }
    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    std::map<std::string, GeoPoint> geo_points;
    Redis::Geo geo_db(svr->storage_, conn->GetNamespace());
    rocksdb::Status s = geo_db.Pos(args_[1], members_, &geo_points);
    if (!s.ok()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    std::vector<std::string> list;
    for (const auto &member : members_) {
      auto iter = geo_points.find(member.ToString());
      if (iter == geo_points.end()) {
        list.emplace_back(Redis::NilString());
      } else {
        list.emplace_back(Redis::MultiBulkString({Util::Float2String(iter->second.longitude),
                                                  Util::Float2String(iter->second.latitude)}));
      }
    }
    *output = Redis::Array(list);
    return Status::OK();
  }

 private:
  std::vector<Slice> members_;
};

class CommandGeoRadius : public CommandGeoBase {
 public:
  CommandGeoRadius() : CommandGeoBase() {}

  Status Parse(const std::vector<std::string> &args) override {
    auto s = ParseLongLat(args[2], args[3], &longitude_, &latitude_);
    if (!s.IsOK()) return s;
    try {
      radius_ = std::stod(args[4]);
    } catch (const std::exception &e) {
      return Status(Status::RedisParseErr, "ERR value is not a valid float");
    }
    s = ParseDistanceUnit(args[5]);
    if (!s.IsOK()) return s;
    s = ParseRadiusExtraOption();
    if (!s.IsOK()) return s;
    return Commander::Parse(args);
  }

  Status ParseRadiusExtraOption(size_t i = 6) {
    while (i < args_.size()) {
      if (Util::ToLower(args_[i]) == "withcoord") {
        with_coord_ = true;
        i++;
      } else if (Util::ToLower(args_[i]) == "withdist") {
        with_dist_ = true;
        i++;
      } else if (Util::ToLower(args_[i]) == "withhash") {
        with_hash_ = true;
        i++;
      } else if (Util::ToLower(args_[i]) == "asc") {
        sort_ = kSortASC;
        i++;
      } else if (Util::ToLower(args_[i]) == "desc") {
        sort_ = kSortDESC;
        i++;
      } else if (Util::ToLower(args_[i]) == "count" && i + 1 < args_.size()) {
        try {
          count_ = std::stoi(args_[i + 1]);
          i += 2;
        } catch (const std::exception &e) {
          return Status(Status::RedisParseErr, "ERR count is not a valid int");
        }
      } else if (IsWrite() && (Util::ToLower(args_[i]) == "store" || Util::ToLower(args_[i]) == "storedist")
          && i + 1 < args_.size()) {
        store_key_ = args_[i + 1];
        if (Util::ToLower(args_[i]) == "storedist") {
          store_distance_ = true;
        }
        i += 2;
      } else {
        return Status(Status::RedisParseErr, errInvalidSyntax);
      }
    }
    /* Trap options not compatible with STORE and STOREDIST. */
    if (!store_key_.empty() && (with_dist_ || with_hash_ || with_coord_)) {
      return Status(Status::RedisParseErr,
                    "STORE option in GEORADIUS is not compatible with WITHDIST, WITHHASH and WITHCOORDS options");
    }
    /* COUNT without ordering does not make much sense, force ASC
     * ordering if COUNT was specified but no sorting was requested.
     * */
    if (count_ != 0 && sort_ == kSortNone) {
      sort_ = kSortASC;
    }
    return Status::OK();
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    std::vector<GeoPoint> geo_points;
    Redis::Geo geo_db(svr->storage_, conn->GetNamespace());
    rocksdb::Status s = geo_db.Radius(args_[1], longitude_, latitude_, GetRadiusMeters(radius_),
                                      count_,
                                      sort_,
                                      store_key_,
                                      store_distance_, GetUnitConversion(), &geo_points);
    if (!s.ok()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    *output = GenerateOutput(geo_points);
    return Status::OK();
  }

  std::string GenerateOutput(const std::vector<GeoPoint> &geo_points) {
    int result_length = geo_points.size();
    int returned_items_count = (count_ == 0 || result_length < count_) ? result_length : count_;
    std::vector<std::string> list;
    for (int i = 0; i < returned_items_count; i++) {
      auto geo_point = geo_points[i];
      if (!with_coord_ && !with_hash_ && !with_dist_) {
        list.emplace_back(Redis::BulkString(geo_point.member));
      } else {
        std::vector<std::string> one;
        one.emplace_back(Redis::BulkString(geo_point.member));
        if (with_dist_) {
          one.emplace_back(Redis::BulkString(Util::Float2String(GetDistanceByUnit(geo_point.dist))));
        }
        if (with_hash_) {
          one.emplace_back(Redis::BulkString(Util::Float2String(geo_point.score)));
        }
        if (with_coord_) {
          one.emplace_back(Redis::MultiBulkString({Util::Float2String(geo_point.longitude),
                                                   Util::Float2String(geo_point.latitude)}));
        }
        list.emplace_back(Redis::Array(one));
      }
    }
    return Redis::Array(list);
  }

 protected:
  double radius_ = 0;
  bool with_coord_ = false;
  bool with_dist_ = false;
  bool with_hash_ = false;
  int count_ = 0;
  DistanceSort sort_ = kSortNone;
  std::string store_key_;
  bool store_distance_ = false;

 private:
  double longitude_ = 0;
  double latitude_ = 0;
};

class CommandGeoRadiusByMember : public CommandGeoRadius {
 public:
  CommandGeoRadiusByMember() : CommandGeoRadius() {}

  Status Parse(const std::vector<std::string> &args) override {
    try {
      radius_ = std::stod(args[3]);
    } catch (const std::exception &e) {
      return Status(Status::RedisParseErr, "ERR value is not a valid float");
    }
    auto s = ParseDistanceUnit(args[4]);
    if (!s.IsOK()) return s;
    s = ParseRadiusExtraOption(5);
    if (!s.IsOK()) return s;
    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    std::vector<GeoPoint> geo_points;
    Redis::Geo geo_db(svr->storage_, conn->GetNamespace());
    rocksdb::Status s = geo_db.RadiusByMember(args_[1], args_[2], GetRadiusMeters(radius_),
                                              count_,
                                              sort_,
                                              store_key_,
                                              store_distance_, GetUnitConversion(), &geo_points);
    if (!s.ok()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    *output = GenerateOutput(geo_points);
    return Status::OK();
  }
};

class CommandGeoRadiusReadonly : public CommandGeoRadius {
 public:
  CommandGeoRadiusReadonly() : CommandGeoRadius() {}
};

class CommandGeoRadiusByMemberReadonly : public CommandGeoRadiusByMember {
 public:
  CommandGeoRadiusByMemberReadonly() : CommandGeoRadiusByMember() {}
};

class CommandSortedintAdd : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    try {
      for (unsigned i = 2; i < args.size(); i++) {
        auto id = std::stoull(args[i]);
        ids_.emplace_back(id);
      }
    } catch (const std::exception &e) {
      return Status(Status::RedisParseErr, errValueNotInterger);
    }
    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    Redis::Sortedint sortedint_db(svr->storage_, conn->GetNamespace());
    int ret;
    rocksdb::Status s = sortedint_db.Add(args_[1], ids_, &ret);
    if (!s.ok()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    *output = Redis::Integer(ret);
    return Status::OK();
  }

 private:
  std::vector<uint64_t> ids_;
};

class CommandSortedintRem : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    try {
      for (unsigned i = 2; i < args.size(); i++) {
        auto id = std::stoull(args[i]);
        ids_.emplace_back(id);
      }
    } catch (const std::exception &e) {
      return Status(Status::RedisParseErr, errValueNotInterger);
    }
    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    Redis::Sortedint sortedint_db(svr->storage_, conn->GetNamespace());
    int ret;
    rocksdb::Status s = sortedint_db.Remove(args_[1], ids_, &ret);
    if (!s.ok()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    *output = Redis::Integer(ret);
    return Status::OK();
  }

 private:
  std::vector<uint64_t> ids_;
};

class CommandSortedintCard : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    Redis::Sortedint sortedint_db(svr->storage_, conn->GetNamespace());
    int ret;
    rocksdb::Status s = sortedint_db.Card(args_[1], &ret);
    if (!s.ok()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    *output = Redis::Integer(ret);
    return Status::OK();
  }
};

class CommandSortedintExists : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    Redis::Sortedint sortedint_db(svr->storage_, conn->GetNamespace());
    std::vector<uint64_t> ids;
    try {
      for (unsigned int i = 2; i < args_.size(); i++) {
        ids.emplace_back(std::stoull(args_[i]));
      }
    } catch (const std::exception &e) {
      return Status(Status::RedisParseErr, errValueNotInterger);
    }

    std::vector<int> exists;
    rocksdb::Status s = sortedint_db.MExist(args_[1], ids, &exists);
    if (!s.ok() && !s.IsNotFound()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    if (s.IsNotFound()) {
      exists.resize(ids.size(), 0);
    }
    output->append(Redis::MultiLen(exists.size()));
    for (const auto &exist : exists) {
      output->append(Redis::Integer(exist));
    }
    return Status::OK();
  }
};

class CommandSortedintRange : public Commander {
 public:
  explicit CommandSortedintRange(bool reversed = false) {
    reversed_ = reversed;
  }

  Status Parse(const std::vector<std::string> &args) override {
    try {
      offset_ = std::stoi(args[2]);
      limit_ = std::stoi(args[3]);
      if (args.size() == 6) {
        if (Util::ToLower(args[4]) != "cursor") {
          return Status(Status::RedisParseErr, errInvalidSyntax);
        }
        cursor_id_ = std::stoull(args[5]);
      }
    } catch (const std::exception &e) {
      return Status(Status::RedisParseErr, errValueNotInterger);
    }
    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    Redis::Sortedint sortedint_db(svr->storage_, conn->GetNamespace());
    std::vector<uint64_t> ids;
    rocksdb::Status s = sortedint_db.Range(args_[1], cursor_id_, offset_, limit_, reversed_, &ids);
    if (!s.ok()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    output->append(Redis::MultiLen(ids.size()));
    for (const auto id : ids) {
      output->append(Redis::BulkString(std::to_string(id)));
    }
    return Status::OK();
  }

 private:
  uint64_t cursor_id_ = 0;
  uint64_t offset_ = 0;
  uint64_t limit_ = 20;
  bool reversed_ = false;
};

class CommandSortedintRevRange : public CommandSortedintRange {
 public:
  CommandSortedintRevRange() : CommandSortedintRange(true) {}
};

class CommandSortedintRangeByValue : public Commander {
 public:
  explicit CommandSortedintRangeByValue(bool reversed = false) {
    spec_.reversed = reversed;
  }

  Status Parse(const std::vector<std::string> &args) override {
    Status s;
    if (spec_.reversed) {
      s = Redis::Sortedint::ParseRangeSpec(args[3], args[2], &spec_);
    } else {
      s = Redis::Sortedint::ParseRangeSpec(args[2], args[3], &spec_);
    }
    if (!s.IsOK()) {
      return Status(Status::RedisParseErr, s.Msg());
    }
    if (args.size() == 7) {
      if (Util::ToLower(args[4]) != "limit") {
        return Status(Status::RedisParseErr, errInvalidSyntax);
      }
      try {
        spec_.offset = std::stoi(args[5]);
        spec_.count = std::stoi(args[6]);
      } catch (const std::exception &e) {
        return Status(Status::RedisParseErr, errValueNotInterger);
      }
    }
    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    Redis::Sortedint sortedint_db(svr->storage_, conn->GetNamespace());
    std::vector<uint64_t> ids;
    int size;
    rocksdb::Status s = sortedint_db.RangeByValue(args_[1], spec_, &ids, &size);
    if (!s.ok()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    output->append(Redis::MultiLen(ids.size()));
    for (const auto id : ids) {
      output->append(Redis::BulkString(std::to_string(id)));
    }
    return Status::OK();
  }

 private:
  SortedintRangeSpec spec_;
};

class CommandSortedintRevRangeByValue : public CommandSortedintRangeByValue {
 public:
  CommandSortedintRevRangeByValue() : CommandSortedintRangeByValue(true) {}
};

class CommandInfo : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    std::string section = "all";
    if (args_.size() == 2) {
      section = Util::ToLower(args_[1]);
    }
    std::string info;
    svr->GetInfo(conn->GetNamespace(), section, &info);
    *output = Redis::BulkString(info);
    return Status::OK();
  }
};

class CommandRole : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    svr->GetRoleInfo(output);
    return Status::OK();
  }
};

class CommandCompact : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    auto ns = conn->GetNamespace();
    std::string begin_key, end_key;
    if (ns != kDefaultNamespace) {
      Redis::Database redis_db(svr->storage_, conn->GetNamespace());
      std::string prefix;
      ComposeNamespaceKey(ns, "", &prefix);
      auto s = redis_db.FindKeyRangeWithPrefix(prefix, &begin_key, &end_key);
      if (!s.ok()) {
        if (s.IsNotFound()) {
          *output = Redis::SimpleString("OK");
          return Status::OK();
        }
        return Status(Status::RedisExecErr, s.ToString());
      }
    }
    Status s = svr->AsyncCompactDB(begin_key, end_key);
    if (!s.IsOK()) return s;
    *output = Redis::SimpleString("OK");
    LOG(INFO) << "Commpact was triggered by manual with executed success";
    return Status::OK();
  }
};

class CommandBGSave: public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    if (!conn->IsAdmin()) {
      *output = Redis::Error(errAdministorPermissionRequired);
      return Status::OK();
    }
    Status s = svr->AsyncBgsaveDB();
    if (!s.IsOK()) return s;
    *output = Redis::SimpleString("OK");
    LOG(INFO) << "BGSave was triggered by manual with executed success";
    return Status::OK();
  }
};

class CommandFlushBackup : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    if (!conn->IsAdmin()) {
      *output = Redis::Error(errAdministorPermissionRequired);
      return Status::OK();
    }
    Status s = svr->AsyncPurgeOldBackups(0, 0);
    if (!s.IsOK()) return s;
    *output = Redis::SimpleString("OK");
    LOG(INFO) << "flushbackup was triggered by manual with executed success";
    return Status::OK();
  }
};

class CommandDBSize : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    std::string ns = conn->GetNamespace();
    if (args_.size() == 1) {
      KeyNumStats stats;
      svr->GetLastestKeyNumStats(ns, &stats);
      *output = Redis::Integer(stats.n_key);
    } else if (args_.size() == 2 && args_[1] == "scan") {
      Status s = svr->AsyncScanDBSize(ns);
      if (s.IsOK()) {
        *output = Redis::SimpleString("OK");
      } else {
        *output = Redis::Error(s.Msg());
      }
    } else {
      *output = Redis::Error("DBSIZE subcommand only supports scan");
    }
    return Status::OK();
  }
};

class CommandPublish : public Commander {
 public:
  // mark is_write as false here because slave should be able to execute publish command
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    if (!svr->IsSlave()) {
      // Compromise: can't replicate message to sub-replicas in a cascading-like structure.
      // Replication is rely on wal seq, increase the seq on slave will break the replication, hence the compromise
      Redis::PubSub pubsub_db(svr->storage_);
      auto s = pubsub_db.Publish(args_[1], args_[2]);
      if (!s.ok()) {
        return Status(Status::RedisExecErr, s.ToString());
      }
    }

    int receivers = svr->PublishMessage(args_[1], args_[2]);
    *output = Redis::Integer(receivers);
    return Status::OK();
  }
};

void SubscribeCommmandReply(std::string *output, std::string name, std::string sub_name, int num) {
  output->append(Redis::MultiLen(3));
  output->append(Redis::BulkString(name));
  output->append(sub_name.empty() ? Redis::NilString() : Redis::BulkString(sub_name));
  output->append(Redis::Integer(num));
}

class CommandSubscribe : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    for (unsigned i = 1; i < args_.size(); i++) {
      conn->SubscribeChannel(args_[i]);
      SubscribeCommmandReply(output, "subscribe", args_[i],
        conn->SubscriptionsCount() + conn->PSubscriptionsCount());
    }
    return Status::OK();
  }
};

class CommandUnSubscribe : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    if (args_.size() == 1) {
      conn->UnSubscribeAll(std::bind(SubscribeCommmandReply, output, "unsubscribe",
        std::placeholders::_1, std::placeholders::_2));
    } else {
      for (unsigned i = 1; i < args_.size(); i++) {
        conn->UnSubscribeChannel(args_[i]);
        SubscribeCommmandReply(output, "unsubscribe", args_[i],
          conn->SubscriptionsCount() + conn->PSubscriptionsCount());
      }
    }
    return Status::OK();
  }
};

class CommandPSubscribe : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    for (unsigned i = 1; i < args_.size(); i++) {
      conn->PSubscribeChannel(args_[i]);
      SubscribeCommmandReply(output, "psubscribe", args_[i],
        conn->SubscriptionsCount() + conn->PSubscriptionsCount());
    }
    return Status::OK();
  }
};

class CommandPUnSubscribe : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    if (args_.size() == 1) {
      conn->PUnSubscribeAll(std::bind(SubscribeCommmandReply, output, "punsubscribe",
        std::placeholders::_1, std::placeholders::_2));
    } else {
      for (unsigned i = 1; i < args_.size(); i++) {
        conn->PUnSubscribeChannel(args_[i]);
        SubscribeCommmandReply(output, "punsubscribe", args_[i],
          conn->SubscriptionsCount() + conn->PSubscriptionsCount());
      }
    }
    return Status::OK();
  }
};

class CommandPubSub : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    subcommand_ = Util::ToLower(args[1]);
    if (subcommand_ == "numpat" && args.size() == 2) {
      return Status::OK();
    }
    if ((subcommand_ == "numsub") && args.size() >= 2) {
      if (args.size() > 2) {
        channels_ = std::vector<std::string>(args.begin() + 2, args.end());
      }
      return Status::OK();
    }
    if ((subcommand_ == "channels") && args.size() <= 3) {
      if (args.size() == 3) {
        pattern_ = args[2];
      }
      return Status::OK();
    }
    return Status(Status::RedisInvalidCmd,
                  "ERR Unknown subcommand or wrong number of arguments");
  }

  Status Execute(Server *srv, Connection *conn, std::string *output) override {
    if (subcommand_ == "numpat") {
      *output = Redis::Integer(srv->GetPubSubPatternSize());
      return Status::OK();
    } else if (subcommand_ == "numsub") {
      std::vector<ChannelSubscribeNum> channel_subscribe_nums;
      srv->ListChannelSubscribeNum(channels_, &channel_subscribe_nums);
      output->append(Redis::MultiLen(channel_subscribe_nums.size() * 2));
      for (const auto &chan_subscribe_num : channel_subscribe_nums) {
        output->append(Redis::BulkString(chan_subscribe_num.channel));
        output->append(Redis::Integer(chan_subscribe_num.subscribe_num));
      }
      return Status::OK();
    } else if (subcommand_ == "channels") {
      std::vector<std::string> channels;
      srv->GetChannelsByPattern(pattern_, &channels);
      *output = Redis::MultiBulkString(channels);
      return Status::OK();
    }

    return Status(Status::RedisInvalidCmd,
                  "ERR Unknown subcommand or wrong number of arguments");
  }

 private:
  std::string pattern_;
  std::vector<std::string> channels_;
  std::string subcommand_;
};

class CommandSlaveOf : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    host_ = args[1];
    auto port = args[2];
    if (Util::ToLower(host_) == "no" && Util::ToLower(port) == "one") {
      host_.clear();
      return Status::OK();
    }
    try {
      auto p = std::stoul(port);
      if (p > UINT32_MAX) {
        throw std::overflow_error("port out of range");
      }
      port_ = static_cast<uint32_t>(p);
    } catch (const std::exception &e) {
      return Status(Status::RedisParseErr, "port should be number");
    }
    return Commander::Parse(args);
  }
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    if (!conn->IsAdmin()) {
      *output = Redis::Error(errAdministorPermissionRequired);
      return Status::OK();
    }
    Status s;
    if (host_.empty()) {
      s = svr->RemoveMaster();
      if (s.IsOK()) {
        *output = Redis::SimpleString("OK");
        LOG(WARNING) << "MASTER MODE enabled (user request from '" << conn->GetAddr() << "')";
      }
    } else {
      s = svr->AddMaster(host_, port_);
      if (s.IsOK()) {
        *output = Redis::SimpleString("OK");
        LOG(WARNING) << "SLAVE OF " << host_ << ":" << port_
                     << " enabled (user request from '" << conn->GetAddr() << "')";
      } else {
        LOG(ERROR) << "SLAVE OF " << host_ << ":" << port_
                   << " (user request from '" << conn->GetAddr() << "') encounter error: " << s.Msg();
      }
    }
    return s;
  }

 private:
  std::string host_;
  uint32_t port_ = 0;
};

class CommandStats: public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    std::string stats_json = svr->GetRocksDBStatsJson();
    *output = Redis::BulkString(stats_json);
    return Status::OK();
  }
};

class CommandPSync : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    try {
      auto s = std::stoull(args[1]);
      next_repl_seq = static_cast<rocksdb::SequenceNumber>(s);
    } catch (const std::exception &e) {
      return Status(Status::RedisParseErr, "value is not an unsigned long long or out of range");
    }
    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    LOG(INFO) << "Slave " << conn->GetAddr() << " asks for synchronization"
              << " with next sequence: " << next_repl_seq
              << ", and local sequence: " << svr->storage_->LatestSeq();
    if (!checkWALBoundary(svr->storage_, next_repl_seq).IsOK()) {
      svr->stats_.IncrPSyncErrCounter();
      *output = "sequence out of range, please use fullsync";
      return Status(Status::RedisExecErr, *output);
    }

    // Server would spawn a new thread to sync the batch, and connection would
    // be took over, so should never trigger any event in worker thread.
    conn->Detach();
    conn->EnableFlag(Redis::Connection::kSlave);
    Util::SockSetBlocking(conn->GetFD(), 1);

    svr->stats_.IncrPSyncOKCounter();
    Status s = svr->AddSlave(conn, next_repl_seq);
    if (!s.IsOK()) {
      std::string err = "-ERR " + s.Msg() + "\r\n";
      write(conn->GetFD(), err.c_str(), err.length());
      conn->EnableFlag(Redis::Connection::kCloseAsync);
      LOG(WARNING) << "Failed to add salve: "  << conn->GetAddr()
                   << " to start increment syncing";
    } else {
      LOG(INFO) << "New slave: "  << conn->GetAddr()
                << " was added, start increment syncing";
    }
    return Status::OK();
  }

 private:
  rocksdb::SequenceNumber next_repl_seq = 0;

  // Return OK if the seq is in the range of the current WAL
  Status checkWALBoundary(Engine::Storage *storage,
                          rocksdb::SequenceNumber seq) {
    if (seq == storage->LatestSeq() + 1) {
      return Status::OK();
    }
    // Upper bound
    if (seq > storage->LatestSeq() + 1) {
      return Status(Status::NotOK);
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
        return Status(Status::NotOK);
      }
      return Status::OK();
    }
    return Status(Status::NotOK);
  }
};

class CommandPerfLog : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    subcommand_ = Util::ToLower(args[1]);
    if (subcommand_ != "reset" && subcommand_ != "get" && subcommand_ != "len") {
      return Status(Status::NotOK, "PERFLOG subcommand must be one of RESET, LEN, GET");
    }
    if (subcommand_ == "get" && args.size() >= 3) {
      if (args[2] == "*") {
        cnt_ = 0;
      } else {
        Status s = Util::StringToNum(args[2], &cnt_);
        return s;
      }
    }
    return Status::OK();
  }

  Status Execute(Server *srv, Connection *conn, std::string *output) override {
    auto perf_log = srv->GetPerfLog();
    if (subcommand_ == "len") {
      *output = Redis::Integer(static_cast<int64_t>(perf_log->Size()));
    } else if (subcommand_ == "reset") {
      perf_log->Reset();
      *output = Redis::SimpleString("OK");
    } else if (subcommand_ == "get") {
      *output = perf_log->GetLatestEntries(cnt_);
    }
    return Status::OK();
  }

 private:
  std::string subcommand_;
  int64_t cnt_ = 10;
};

class CommandSlowlog : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    subcommand_ = Util::ToLower(args[1]);
    if (subcommand_ != "reset" && subcommand_ != "get" && subcommand_ != "len") {
      return Status(Status::NotOK, "SLOWLOG subcommand must be one of RESET, LEN, GET");
    }
    if (subcommand_ == "get" && args.size() >= 3) {
      if (args[2] == "*") {
        cnt_ = 0;
      } else {
        Status s = Util::StringToNum(args[2], &cnt_);
        return s;
      }
    }
    return Status::OK();
  }

  Status Execute(Server *srv, Connection *conn, std::string *output) override {
    auto slowlog = srv->GetSlowLog();
    if (subcommand_ == "reset") {
      slowlog->Reset();
      *output = Redis::SimpleString("OK");
      return Status::OK();
    } else if (subcommand_ == "len") {
      *output = Redis::Integer(static_cast<int64_t>(slowlog->Size()));
      return Status::OK();
    } else if (subcommand_ == "get") {
      *output = slowlog->GetLatestEntries(cnt_);
      return Status::OK();
    }
    return Status(Status::NotOK, "SLOWLOG subcommand must be one of RESET, LEN, GET");
  }

 private:
  std::string subcommand_;
  int64_t cnt_ = 10;
};

class CommandClient : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    subcommand_ = Util::ToLower(args[1]);
    // subcommand: getname id kill list setname
    if ((subcommand_ == "id" || subcommand_ == "getname" ||  subcommand_ == "list") && args.size() == 2) {
      return Status::OK();
    }
    if ((subcommand_ == "setname") && args.size() == 3) {
      // Check if the charset is ok. We need to do this otherwise
      // CLIENT LIST format will break. You should always be able to
      // split by space to get the different fields.
      for (auto ch : args[2]) {
        if (ch < '!' || ch > '~') {
          return Status(Status::RedisInvalidCmd,
                  "ERR Client names cannot contain spaces, newlines or special characters");
        }
      }
      conn_name_ = args[2];
      return Status::OK();
    }
    if ((subcommand_ == "kill")) {
      if (args.size() == 2) {
        return Status(Status::RedisParseErr, errInvalidSyntax);
      } else if (args.size() == 3) {
        addr_ = args[2];
        new_format_ = false;
        return Status::OK();
      }

      uint i = 2;
      new_format_ = true;
      while (i < args.size()) {
        bool moreargs = i < args.size();
        if (args[i] == "addr" && moreargs) {
          addr_ = args[i+1];
        } else if (args[i] == "id" && moreargs) {
          try {
            id_ = std::stoll(args[i+1]);
          } catch (std::exception &e) {
            return Status(Status::RedisParseErr, errValueNotInterger);
          }
        } else if (args[i] == "skipme" && moreargs) {
          if (args[i+1] == "yes") {
            skipme_ = true;
          } else if (args[i+1] == "no") {
            skipme_ = false;
          } else {
            return Status(Status::RedisParseErr, errInvalidSyntax);
          }
        } else {
          return Status(Status::RedisParseErr, errInvalidSyntax);
        }
        i += 2;
      }
      return Status::OK();
    }
    return Status(Status::RedisInvalidCmd,
                  "Syntax error, try CLIENT LIST|KILL ip:port|GETNAME|SETNAME");
  }

  Status Execute(Server *srv, Connection *conn, std::string *output) override {
    if (subcommand_ == "list") {
      *output = Redis::BulkString(srv->GetClientsStr());
      return Status::OK();
    } else if (subcommand_ == "setname") {
      conn->SetName(conn_name_);
      *output = Redis::SimpleString("OK");
      return Status::OK();
    } else if (subcommand_ == "getname") {
      std::string name = conn->GetName();
      *output = name== ""? Redis::NilString(): Redis::BulkString(name);
      return Status::OK();
    } else if (subcommand_ == "id") {
      *output = Redis::Integer(conn->GetID());
      return Status::OK();
    } else if (subcommand_ == "kill") {
      int64_t killed = 0;
      srv->KillClient(&killed, addr_, id_, skipme_, conn);
      if (new_format_) {
        *output = Redis::Integer(killed);
      } else {
        if (killed == 0)
          *output = Redis::Error("No such client");
        else
          *output = Redis::SimpleString("OK");
      }
      return Status::OK();
    }

    return Status(Status::RedisInvalidCmd,
                  "Syntax error, try CLIENT LIST|KILL ip:port|GETNAME|SETNAME");
  }

 private:
  std::string addr_;
  std::string conn_name_;
  std::string subcommand_;
  bool skipme_ = false;
  uint64_t id_ = 0;
  bool new_format_ = true;
};

class CommandMonitor : public Commander {
 public:
  Status Execute(Server *srv, Connection *conn, std::string *output) override {
    conn->Owner()->BecomeMonitorConn(conn);
    *output = Redis::SimpleString("OK");
    return Status::OK();
  }
};

class CommandShutdown : public Commander {
 public:
  Status Execute(Server *srv, Connection *conn, std::string *output) override {
    if (!conn->IsAdmin()) {
      *output = Redis::Error(errAdministorPermissionRequired);
      return Status::OK();
    }
    if (!srv->IsStopped()) {
      LOG(INFO) << "bye bye";
      srv->Stop();
    }
    return Status::OK();
  }
};

class CommandQuit : public Commander {
 public:
  Status Execute(Server *srv, Connection *conn, std::string *output) override {
    conn->EnableFlag(Redis::Connection::kCloseAfterReply);
    *output = Redis::SimpleString("OK");
    return Status::OK();
  }
};

class CommandDebug : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    subcommand_ = Util::ToLower(args[1]);
    if ((subcommand_ == "sleep") && args.size() == 3) {
      double second = 0.0;
      try {
        second = std::stod(args[2]);
      } catch (const std::exception &e) {
        return Status(Status::RedisParseErr, "ERR invalid debug sleep time");
      }
      microsecond_ = static_cast<uint64_t>(second * 1000 * 1000);
      return Status::OK();
    }
    return Status(Status::RedisInvalidCmd, "Syntax error, DEBUG SLEEP <seconds>");
  }

  Status Execute(Server *srv, Connection *conn, std::string *output) override {
    if (subcommand_ == "sleep") {
      usleep(microsecond_);
    }
    *output = Redis::SimpleString("OK");
    return Status::OK();
  }

 private:
  std::string subcommand_;
  uint64_t microsecond_ = 0;
};

class CommandScanBase : public Commander {
 public:
  Status ParseMatchAndCountParam(const std::string &type, std::string value) {
    if (type == "match") {
      prefix = std::move(value);
      if (!prefix.empty() && prefix[prefix.size() - 1] == '*') {
        prefix = prefix.substr(0, prefix.size() - 1);
        return Status::OK();
      }
      return Status(Status::RedisParseErr, "only keys prefix match was supported");
    } else if (type == "count") {
      try {
        limit = std::stoi(value);
      } catch (const std::exception &e) {
        return Status(Status::RedisParseErr, "ERR count param should be type int");
      }
      if (limit <= 0) {
        return Status(Status::RedisParseErr, errInvalidSyntax);
      }
    }
    return Status::OK();
  }

  void ParseCursor(const std::string &param) {
    cursor = param;
    if (cursor == "0") {
      cursor = std::string();
    }
  }

  std::string GenerateOutput(const std::vector<std::string> &keys) {
    std::vector<std::string> list;
    if (keys.size() == static_cast<size_t>(limit)) {
      list.emplace_back(Redis::BulkString(keys.back()));
    } else {
      list.emplace_back(Redis::BulkString("0"));
    }

    list.emplace_back(Redis::MultiBulkString(keys));

    return Redis::Array(list);
  }

 protected:
  std::string cursor;
  std::string prefix;
  int limit = 20;
};

class CommandSubkeyScanBase : public CommandScanBase {
 public:
  CommandSubkeyScanBase(): CommandScanBase() {}
  Status Parse(const std::vector<std::string> &args) override {
    if (args.size() % 2 == 0) {
      return Status(Status::RedisParseErr, errWrongNumOfArguments);
    }
    key = args[1];
    ParseCursor(args[2]);
    if (args.size() >= 5) {
      Status s = ParseMatchAndCountParam(Util::ToLower(args[3]), args_[4]);
      if (!s.IsOK()) {
        return s;
      }
    }
    if (args.size() >= 7) {
      Status s = ParseMatchAndCountParam(Util::ToLower(args[5]), args_[6]);
      if (!s.IsOK()) {
        return s;
      }
    }
    return Commander::Parse(args);
  }

  std::string GenerateOutput(const std::vector<std::string> &fields, const std::vector<std::string> &values) {
    std::vector<std::string> list;
    auto items_count = fields.size();
    if (items_count == static_cast<size_t>(limit)) {
      list.emplace_back(Redis::BulkString(fields.back()));
    } else {
      list.emplace_back(Redis::BulkString("0"));
    }
    std::vector<std::string> fvs;
    if (items_count > 0) {
      for (size_t i = 0; i < items_count; i++) {
        fvs.emplace_back(fields[i]);
        fvs.emplace_back(values[i]);
      }
    }
    list.emplace_back(Redis::MultiBulkString(fvs, false));
    return Redis::Array(list);
  }

 protected:
  std::string key;
};

class CommandScan : public CommandScanBase {
 public:
  CommandScan() : CommandScanBase() {}
  Status Parse(const std::vector<std::string> &args) override {
    if (args.size() % 2 != 0) {
      return Status(Status::RedisParseErr, errWrongNumOfArguments);
    }

    ParseCursor(args[1]);
    if (args.size() >= 4) {
      Status s = ParseMatchAndCountParam(Util::ToLower(args[2]), args_[3]);
      if (!s.IsOK()) {
        return s;
      }
    }
    if (args.size() >= 6) {
      Status s = ParseMatchAndCountParam(Util::ToLower(args[4]), args_[5]);
      if (!s.IsOK()) {
        return s;
      }
    }
    return Commander::Parse(args);
  }
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    Redis::Database redis_db(svr->storage_, conn->GetNamespace());
    std::vector<std::string> keys;
    auto s = redis_db.Scan(cursor, limit, prefix, &keys);
    if (!s.ok()) {
      return Status(Status::RedisExecErr, s.ToString());
    }

    *output = GenerateOutput(keys);
    return Status::OK();
  }
};

class CommandHScan : public CommandSubkeyScanBase {
 public:
  CommandHScan() : CommandSubkeyScanBase() {}
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    Redis::Hash hash_db(svr->storage_, conn->GetNamespace());
    std::vector<std::string> fields;
    std::vector<std::string> values;
    auto s = hash_db.Scan(key, cursor, limit, prefix, &fields, &values);
    if (!s.ok() && !s.IsNotFound()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    *output = GenerateOutput(fields, values);
    return Status::OK();
  }
};

class CommandSScan : public CommandSubkeyScanBase {
 public:
  CommandSScan() : CommandSubkeyScanBase() {}
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    Redis::Set set_db(svr->storage_, conn->GetNamespace());
    std::vector<std::string> members;
    auto s = set_db.Scan(key, cursor, limit, prefix, &members);
    if (!s.ok() && !s.IsNotFound()) {
      return Status(Status::RedisExecErr, s.ToString());
    }

    *output = CommandScanBase::GenerateOutput(members);
    return Status::OK();
  }
};

class CommandZScan : public CommandSubkeyScanBase {
 public:
  CommandZScan() : CommandSubkeyScanBase() {}
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    Redis::ZSet zset_db(svr->storage_, conn->GetNamespace());
    std::vector<std::string> members;
    std::vector<double> scores;
    auto s = zset_db.Scan(key, cursor, limit, prefix, &members, &scores);
    if (!s.ok() && !s.IsNotFound()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    std::vector<std::string> score_strings;
    for (const auto &score : scores) {
      score_strings.emplace_back(Util::Float2String(score));
    }
    *output = GenerateOutput(members, score_strings);
    return Status::OK();
  }
};

class CommandRandomKey : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    std::string key;
    auto cursor = svr->GetLastRandomKeyCursor();
    Redis::Database redis(svr->storage_, conn->GetNamespace());
    redis.RandomKey(cursor, &key);
    svr->SetLastRandomKeyCursor(key);
    *output = Redis::BulkString(key);
    return Status::OK();
  }
};

class CommandReplConf : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    if (args.size() % 2 == 0) {
      return Status(Status::RedisParseErr, errWrongNumOfArguments);
    }
    if (args.size() >= 3) {
      Status s = ParseParam(Util::ToLower(args[1]), args_[2]);
      if (!s.IsOK()) {
        return s;
      }
    }
    if (args.size() >= 5) {
      Status s = ParseParam(Util::ToLower(args[3]), args_[4]);
      if (!s.IsOK()) {
        return s;
      }
    }
    return Commander::Parse(args);
  }

  Status ParseParam(const std::string &option, const std::string &value) {
    if (option == "listening-port") {
      try {
        auto p = std::stoul(value);
        if (p > UINT32_MAX) {
          throw std::overflow_error("listening-port out of range");
        }
        port_ = static_cast<uint32_t>(p);
      } catch (const std::exception &e) {
        return Status(Status::RedisParseErr, "listening-port should be number");
      }
    } else {
      return Status(Status::RedisParseErr, "unknown option");
    }
    return Status::OK();
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    if (port_ != 0) {
      conn->SetListeningPort(port_);
    }
    *output = Redis::SimpleString("OK");
    return Status::OK();
  }

 private:
  uint32_t port_ = 0;
};

class CommandFetchMeta : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    return Status::OK();
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    int repl_fd = conn->GetFD();
    std::string ip = conn->GetIP();

    Util::SockSetBlocking(repl_fd, 1);
    conn->NeedNotClose();
    conn->EnableFlag(Redis::Connection::kCloseAsync);
    svr->stats_.IncrFullSyncCounter();

    // Feed-replica-meta thread
    std::thread t = std::thread([svr, repl_fd, ip]() {
      Util::ThreadSetName("feed-replica-data-info");
      std::string files;
      auto s = Engine::Storage::ReplDataManager::GetFullReplDataInfo(
          svr->storage_, &files);
      if (!s.IsOK()) {
        const char *message = "-ERR can't create db checkpoint";
        write(repl_fd, message, strlen(message));
        LOG(WARNING) << "[replication] Failed to get full data file info,"
                     << " error: " << s.Msg();
        close(repl_fd);
        return;
      }
      // Send full data file info
      if (Util::SockSend(repl_fd, files+CRLF).IsOK()) {
        LOG(INFO) << "[replication] Succeed sending full data file info to " << ip;
      } else {
        LOG(WARNING) << "[replication] Fail to send full data file info "
                     << ip << ", error: " << strerror(errno);
      }
      svr->storage_->SetCheckpointAccessTime(std::time(nullptr));
      close(repl_fd);
    });
    t.detach();

    return Status::OK();
  }
};

class CommandFetchFile : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    files_str_ = args[1];
    return Status::OK();
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    std::vector<std::string> files;
    Util::Split(files_str_, ",", &files);

    int repl_fd = conn->GetFD();
    std::string ip = conn->GetIP();

    Util::SockSetBlocking(repl_fd, 1);
    conn->NeedNotClose();  // Feed-replica-file thread will close the replica fd
    conn->EnableFlag(Redis::Connection::kCloseAsync);

    std::thread t = std::thread([svr, repl_fd, ip, files]() {
      Util::ThreadSetName("feed-replica-file");
      svr->IncrFetchFileThread();

      for (auto file : files) {
        uint64_t file_size = 0, max_replication_bytes = 0;
        if (svr->GetConfig()->max_replication_mb > 0) {
          max_replication_bytes = (svr->GetConfig()->max_replication_mb*MiB) /
                                   svr->GetFetchFileThreadNum();
        }
        auto start = std::chrono::high_resolution_clock::now();
        auto fd = Engine::Storage::ReplDataManager::OpenDataFile(svr->storage_,
                                                      file, &file_size);
        if (fd < 0) break;

        // Send file size and content
        if (Util::SockSend(repl_fd, std::to_string(file_size)+CRLF).IsOK() &&
            Util::SockSendFile(repl_fd, fd, file_size).IsOK()) {
          LOG(INFO) << "[replication] Succeed sending file " << file << " to "
                    << ip;
        } else {
          LOG(WARNING) << "[replication] Fail to send file " << file << " to "
                       << ip << ", error: " << strerror(errno);
        }
        close(fd);

        // Sleep if the speed of sending file is more than replication speed limit
        auto end = std::chrono::high_resolution_clock::now();
        uint64_t duration = std::chrono::duration_cast<std::chrono::microseconds>
                  (end - start).count();
        uint64_t shortest = static_cast<uint64_t>(static_cast<double>(file_size) /
                                max_replication_bytes * (1000 * 1000));
        if (max_replication_bytes > 0 && duration < shortest) {
          LOG(INFO) << "[replication] Need to sleep "
                     << (shortest - duration) / 1000
                     << " ms since of sending files too quickly";
          usleep(shortest - duration);
        }
      }
      svr->storage_->SetCheckpointAccessTime(std::time(nullptr));
      svr->DecrFetchFileThread();
      close(repl_fd);
    });
    t.detach();

    return Status::OK();
  }

 private:
  std::string files_str_;
};

class CommandDBName : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    return Status::OK();
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    conn->Reply(svr->storage_->GetName() + CRLF);
    return Status::OK();
  }
};

#define ADD_CMD(name, arity, is_write, first_key, last_key, key_step, fn) \
{name, arity, is_write, first_key, last_key, key_step, []() -> std::unique_ptr<Commander> { \
  return std::unique_ptr<Commander>(new fn()); \
}}

CommandAttributes redisCommandTable[] = {
    ADD_CMD("auth", 2, false, 0, 0, 0, CommandAuth),
    ADD_CMD("ping", 1, false, 0, 0, 0, CommandPing),
    ADD_CMD("select", 2, false, 0, 0, 0, CommandSelect),
    ADD_CMD("info", -1, false, 0, 0, 0, CommandInfo),
    ADD_CMD("role", 1, false, 0, 0, 0, CommandRole),
    ADD_CMD("config", -2, false, 0, 0, 0, CommandConfig),
    ADD_CMD("namespace", -3, false, 0, 0, 0, CommandNamespace),
    ADD_CMD("keys", 2, false, 0, 0, 0, CommandKeys),
    ADD_CMD("flushdb", 1, false, 0, 0, 0, CommandFlushDB),
    ADD_CMD("flushall", 1, false, 0, 0, 0, CommandFlushAll),
    ADD_CMD("dbsize", -1, false, 0, 0, 0, CommandDBSize),
    ADD_CMD("slowlog", -2, false, 0, 0, 0, CommandSlowlog),
    ADD_CMD("perflog", -2, false, 0, 0, 0, CommandPerfLog),
    ADD_CMD("client", -2, false, 0, 0, 0, CommandClient),
    ADD_CMD("monitor", 1, false, 0, 0, 0, CommandMonitor),
    ADD_CMD("shutdown", 1, false, 0, 0, 0, CommandShutdown),
    ADD_CMD("quit", 1, false, 0, 0, 0, CommandQuit),
    ADD_CMD("scan", -2, false, 0, 0, 0, CommandScan),
    ADD_CMD("randomkey", 1, false, 0, 0, 0, CommandRandomKey),
    ADD_CMD("debug", -2, false, 0, 0, 0, CommandDebug),

    // key command
    ADD_CMD("ttl", 2, false, 1, 1, 1, CommandTTL),
    ADD_CMD("pttl", 2, false, 1, 1, 1, CommandPTTL),
    ADD_CMD("type", 2, false, 1, 1, 1, CommandType),
    ADD_CMD("object", 3, false, 2, 2, 1, CommandObject),
    ADD_CMD("exists", -2, false, 1, -1, 1, CommandExists),
    ADD_CMD("persist", 2, true, 1, 1, 1, CommandPersist),
    ADD_CMD("expire", 3, true, 1, 1, 1, CommandExpire),
    ADD_CMD("pexpire", 3, true, 1, 1, 1, CommandPExpire),
    ADD_CMD("expireat", 3, true, 1, 1, 1, CommandExpireAt),
    ADD_CMD("pexpireat", 3, true, 1, 1, 1, CommandPExpireAt),
    ADD_CMD("del", -2, true, 1, -1, 1, CommandDel),

    // string command
    ADD_CMD("get", 2, false, 1, 1, 1, CommandGet),
    ADD_CMD("strlen", 2, false, 1, 1, 1, CommandStrlen),
    ADD_CMD("getset", 3, true, 1, 1, 1, CommandGetSet),
    ADD_CMD("getrange", 4, false, 1, 1, 1, CommandGetRange),
    ADD_CMD("setrange", 4, true, 1, 1, 1, CommandSetRange),
    ADD_CMD("mget", -2, false, 1, -1, 1, CommandMGet),
    ADD_CMD("append", 3, true, 1, 1, 1, CommandAppend),
    ADD_CMD("set", -3, true, 1, 1, 1, CommandSet),
    ADD_CMD("setex", 4, true, 1, 1, 1, CommandSetEX),
    ADD_CMD("psetex", 4, true, 1, 1, 1, CommandPSetEX),
    ADD_CMD("setnx", 3, true, 1, 1, 1, CommandSetNX),
    ADD_CMD("msetnx", -3, true, 1, -1, 2, CommandMSetNX),
    ADD_CMD("mset", -3, true, 1, -1, 2, CommandMSet),
    ADD_CMD("incrby", 3, true, 1, 1, 1, CommandIncrBy),
    ADD_CMD("incrbyfloat", 3, true, 1, 1, 1, CommandIncrByFloat),
    ADD_CMD("incr", 2, true, 1, 1, 1, CommandIncr),
    ADD_CMD("decrby", 3, true, 1, 1, 1, CommandDecrBy),
    ADD_CMD("decr", 2, true, 1, 1, 1, CommandDecr),

    // bit command
    ADD_CMD("getbit", 3, false, 1, 1, 1, CommandGetBit),
    ADD_CMD("setbit", 4, true, 1, 1, 1, CommandSetBit),
    ADD_CMD("bitcount", -2, false, 1, 1, 1, CommandBitCount),
    ADD_CMD("bitpos", -3, false, 1, 1, 1, CommandBitPos),

    // hash command
    ADD_CMD("hget", 3, false, 1, 1, 1, CommandHGet),
    ADD_CMD("hincrby", 4, true, 1, 1, 1, CommandHIncrBy),
    ADD_CMD("hincrbyfloat", 4, true, 1, 1, 1, CommandHIncrByFloat),
    ADD_CMD("hset", 4, true, 1, 1, 1, CommandHSet),
    ADD_CMD("hsetnx", 4, true, 1, 1, 1, CommandHSetNX),
    ADD_CMD("hdel", -3, true, 1, 1, 1, CommandHDel),
    ADD_CMD("hstrlen", 3, false, 1, 1, 1, CommandHStrlen),
    ADD_CMD("hexists", 3, false, 1, 1, 1, CommandHExists),
    ADD_CMD("hlen", 2, false, 1, 1, 1, CommandHLen),
    ADD_CMD("hmget", -3, false, 1, 1, 1, CommandHMGet),
    ADD_CMD("hmset", -4, true, 1, 1, 1, CommandHMSet),
    ADD_CMD("hkeys", 2, false, 1, 1, 1, CommandHKeys),
    ADD_CMD("hvals", 2, false, 1, 1, 1, CommandHVals),
    ADD_CMD("hgetall", 2, false, 1, 1, 1, CommandHGetAll),
    ADD_CMD("hscan", -3, false, 1, 1, 1, CommandHScan),

    // list command
    ADD_CMD("lpush", -3, true, 1, 1, 1, CommandLPush),
    ADD_CMD("rpush", -3, true, 1, 1, 1, CommandRPush),
    ADD_CMD("lpushx", -3, true, 1, 1, 1, CommandLPushX),
    ADD_CMD("rpushx", -3, true, 1, 1, 1, CommandRPushX),
    ADD_CMD("lpop", 2, true, 1, 1, 1, CommandLPop),
    ADD_CMD("rpop", 2, true, 1, 1, 1, CommandRPop),
    ADD_CMD("blpop", -3, true, 1, -2, 1, CommandBLPop),
    ADD_CMD("brpop", -3, true, 1, -2, 1, CommandBRPop),
    ADD_CMD("lrem", 4, false, 1, 1, 1, CommandLRem),
    ADD_CMD("linsert", 5, false, 1, 1, 1, CommandLInsert),
    ADD_CMD("lrange", 4, false, 1, 1, 1, CommandLRange),
    ADD_CMD("lindex", 3, false, 1, 1, 1, CommandLIndex),
    ADD_CMD("ltrim", 4, true, 1, 1, 1, CommandLTrim),
    ADD_CMD("llen", 2, false, 1, 1, 1, CommandLLen),
    ADD_CMD("lset", 4, true, 1, 1, 1, CommandLSet),
    ADD_CMD("rpoplpush", 3, true, 1, 2, 1, CommandRPopLPUSH),

    // set command
    ADD_CMD("sadd", -3, true, 1, 1, 1, CommandSAdd),
    ADD_CMD("srem", -3, true, 1, 1, 1, CommandSRem),
    ADD_CMD("scard", 2, false, 1, 1, 1, CommandSCard),
    ADD_CMD("smembers", 2, false, 1, 1, 1, CommandSMembers),
    ADD_CMD("sismember", 3, false, 1, 1, 1, CommandSIsMember),
    ADD_CMD("spop", -2, true, 1, 1, 1, CommandSPop),
    ADD_CMD("srandmember", -2, false, 1, 1, 1, CommandSRandMember),
    ADD_CMD("smove", 4, true, 1, 2, 1, CommandSMove),
    ADD_CMD("sdiff", -2, false, 1, -1, 1, CommandSDiff),
    ADD_CMD("sunion", -2, false, 1, -1, 1, CommandSUnion),
    ADD_CMD("sinter", -2, false, 1, -1, 1, CommandSInter),
    ADD_CMD("sdiffstore", -3, false, 1, -1, 1, CommandSDiffStore),
    ADD_CMD("sunionstore", -3, false, 1, -1, 1, CommandSUnionStore),
    ADD_CMD("sinterstore", -3, false, 1, -1, 1, CommandSInterStore),
    ADD_CMD("sscan", -3, false, 1, 1, 1, CommandSScan),

    // zset command
    ADD_CMD("zadd", -4, true, 1, 1, 1, CommandZAdd),
    ADD_CMD("zcard", 2, false, 1, 1, 1, CommandZCard),
    ADD_CMD("zcount", 4, false, 1, 1, 1, CommandZCount),
    ADD_CMD("zincrby", 4, true, 1, 1, 1, CommandZIncrBy),
    ADD_CMD("zinterstore", -4, true, 1, 1, 1, CommandZInterStore),
    ADD_CMD("zlexcount", 4, true, 1, 1, 1, CommandZLexCount),
    ADD_CMD("zpopmax", -2, true, 1, 1, 1, CommandZPopMax),
    ADD_CMD("zpopmin", -2, true, 1, 1, 1, CommandZPopMin),
    ADD_CMD("zrange", -4, false, 1, 1, 1, CommandZRange),
    ADD_CMD("zrevrange", -4, false, 1, 1, 1, CommandZRevRange),
    ADD_CMD("zrangebylex", -4, false, 1, 1, 1, CommandZRangeByLex),
    ADD_CMD("zrangebyscore", -4, false, 1, 1, 1, CommandZRangeByScore),
    ADD_CMD("zrank", 3, false, 1, 1, 1, CommandZRank),
    ADD_CMD("zrem", -3, true, 1, 1, 1, CommandZRem),
    ADD_CMD("zremrangebyrank", 4, true, 1, 1, 1, CommandZRemRangeByRank),
    ADD_CMD("zremrangebyscore", -4, true, 1, 1, 1, CommandZRemRangeByScore),
    ADD_CMD("zremrangebylex", 4, true, 1, 1, 1, CommandZRemRangeByLex),
    ADD_CMD("zrevrangebyscore", -4, false, 1, 1, 1, CommandZRevRangeByScore),
    ADD_CMD("zrevrank", 3, false, 1, 1, 1, CommandZRevRank),
    ADD_CMD("zscore", 3, false, 1, 1, 1, CommandZScore),
    ADD_CMD("zmscore", -3, false, 1, 1, 1, CommandZMScore),
    ADD_CMD("zscan", -3, false, 1, 1, 1, CommandZScan),
    ADD_CMD("zunionstore", -4, true, 1, 1, 1, CommandZUnionStore),

    // geo command
    ADD_CMD("geoadd", -5, true, 1, 1, 1, CommandGeoAdd),
    ADD_CMD("geodist", -4, false, 1, 1, 1, CommandGeoDist),
    ADD_CMD("geohash", -3, false, 1, 1, 1, CommandGeoHash),
    ADD_CMD("geopos", -3, false, 1, 1, 1, CommandGeoPos),
    ADD_CMD("georadius", -6, true, 1, 1, 1, CommandGeoRadius),
    ADD_CMD("georadiusbymember", -5, true, 1, 1, 1, CommandGeoRadiusByMember),
    ADD_CMD("georadius_ro", -6, false, 1, 1, 1, CommandGeoRadiusReadonly),
    ADD_CMD("georadiusbymember_ro", -5, false, 1, 1, 1, CommandGeoRadiusByMemberReadonly),

    // pub/sub command
    ADD_CMD("publish", 3, false, 0, 0, 0, CommandPublish),
    ADD_CMD("subscribe", -2, false, 0, 0, 0, CommandSubscribe),
    ADD_CMD("unsubscribe", -1, false, 0, 0, 0, CommandUnSubscribe),
    ADD_CMD("psubscribe", -2, false, 0, 0, 0, CommandPSubscribe),
    ADD_CMD("punsubscribe", -1, false, 0, 0, 0, CommandPUnSubscribe),
    ADD_CMD("pubsub", -2, false, 0, 0, 0, CommandPubSub),

    // Sortedint command
    ADD_CMD("siadd", -3, true, 1, 1, 1, CommandSortedintAdd),
    ADD_CMD("sirem", -3, true, 1, 1, 1, CommandSortedintRem),
    ADD_CMD("sicard", 2, false, 1, 1, 1, CommandSortedintCard),
    ADD_CMD("siexists", -3, false, 1, 1, 1, CommandSortedintExists),
    ADD_CMD("sirange", -4, false, 1, 1, 1, CommandSortedintRange),
    ADD_CMD("sirevrange", -4, false, 1, 1, 1, CommandSortedintRevRange),
    ADD_CMD("sirangebyvalue", -4, false, 1, 1, 1, CommandSortedintRangeByValue),
    ADD_CMD("sirevrangebyvalue", -4, false, 1, 1, 1, CommandSortedintRevRangeByValue),

    // internal management cmd
    ADD_CMD("compact", 1, false, 0, 0, 0, CommandCompact),
    ADD_CMD("bgsave", 1, false, 0, 0, 0, CommandBGSave),
    ADD_CMD("flushbackup", 1, false, 0, 0, 0, CommandFlushBackup),
    ADD_CMD("slaveof", 3, false, 0, 0, 0, CommandSlaveOf),
    ADD_CMD("stats", 1, false, 0, 0, 0, CommandStats),

    // Replicaion commands
    ADD_CMD("replconf", -3, false, 0, 0, 0, CommandReplConf),
    ADD_CMD("psync", 2, false, 0, 0, 0, CommandPSync),
    ADD_CMD("_fetch_meta", 1, false, 0, 0, 0, CommandFetchMeta),
    ADD_CMD("_fetch_file", 2, false, 0, 0, 0, CommandFetchFile),
    ADD_CMD("_db_name", 1, false, 0, 0, 0, CommandDBName),
};

std::map<std::string, CommandAttributes*> commands;

void PopulateCommands() {
  int num = sizeof(redisCommandTable)/sizeof(struct CommandAttributes);
  for (int i = 0; i < num; i++) {
    auto commandAttributes = new(CommandAttributes);
    *commandAttributes = redisCommandTable[i];
    commands[redisCommandTable[i].name] = commandAttributes;
  }
}

Status LookupAndCreateCommand(const std::string &cmd_name,
                              std::unique_ptr<Commander> *cmd) {
  if (cmd_name.empty()) return Status(Status::RedisUnknownCmd);
  auto cmd_iter = commands.find(Util::ToLower(cmd_name));
  if (cmd_iter == commands.end()) {
    return Status(Status::RedisUnknownCmd);
  }
  auto redisCmd = cmd_iter->second;
  *cmd = redisCmd->factory();
  (*cmd)->SetAttributes(redisCmd);
  return Status::OK();
}

bool IsCommandExists(const std::string &cmd) {
  return commands.find(cmd) != commands.end();
}

void GetCommandList(std::vector<std::string> *cmds) {
  cmds->clear();
  int num = sizeof(redisCommandTable)/sizeof(struct CommandAttributes);
  for (int i = 0; i < num; i++) {
    cmds->emplace_back(redisCommandTable[i].name);
  }
}
}  // namespace Redis
