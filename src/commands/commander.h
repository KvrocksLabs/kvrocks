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
#include <event2/event.h>
#include <glog/logging.h>
#include <rocksdb/types.h>
#include <rocksdb/utilities/backup_engine.h>

#include <deque>
#include <initializer_list>
#include <iostream>
#include <list>
#include <map>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "cluster/cluster_defs.h"
#include "error_constants.h"
#include "parse_util.h"
#include "server/redis_reply.h"
#include "status.h"
#include "string_util.h"

class Server;

namespace engine {
struct Context;
}

namespace redis {

class Connection;
struct CommandAttributes;

enum CommandFlags : uint64_t {
  // "write" flag, for any command that performs rocksdb writing ops
  kCmdWrite = 1ULL << 0,
  // "read-only" flag, for any command that performs rocksdb reading ops
  // and doesn't perform rocksdb writing ops
  kCmdReadOnly = 1ULL << 1,
  // "ok-loading" flag, for any command that can be executed while
  // the db is in loading phase
  kCmdLoading = 1ULL << 2,
  // "bypass-multi" flag, for commands that can be executed in a MULTI scope,
  // but these commands will NOT be queued and will be executed immediately
  kCmdBypassMulti = 1ULL << 3,
  // "exclusive" flag, for commands that should be executed execlusive globally
  kCmdExclusive = 1ULL << 4,
  // "no-multi" flag, for commands that cannot be executed in MULTI scope
  kCmdNoMulti = 1ULL << 5,
  // "no-script" flag, for commands that cannot be executed in scripting
  kCmdNoScript = 1ULL << 6,
  // "no-dbsize-check" flag, for commands that can ignore the db size checking
  kCmdNoDBSizeCheck = 1ULL << 7,
  // "slow" flag, for commands that run slowly,
  // usually with a non-constant number of rocksdb ops
  kCmdSlow = 1ULL << 8,
  // "blocking" flag, for commands that don't perform db ops immediately,
  // but block and wait for some event to happen before performing db ops
  kCmdBlocking = 1ULL << 9,
  // "auth" flag, for commands used for authentication
  kCmdAuth = 1ULL << 10,
  // "admin" flag, for commands that require admin permission
  kCmdAdmin = 1ULL << 11,
};

enum class CommandCategory : uint8_t {
  Unknown = 0,
  Bit,
  BloomFilter,
  Cluster,
  Function,
  Geo,
  Hash,
  HLL,
  JSON,
  Key,
  List,
  Pubsub,
  Replication,
  Script,
  Search,
  Server,
  Set,
  SortedInt,
  Stream,
  String,
  TDigest,
  Txn,
  ZSet,
};

class Commander {
 public:
  void SetAttributes(const CommandAttributes *attributes) { attributes_ = attributes; }
  const CommandAttributes *GetAttributes() const { return attributes_; }
  void SetArgs(const std::vector<std::string> &args) { args_ = args; }
  virtual Status Parse() { return Parse(args_); }
  virtual Status Parse([[maybe_unused]] const std::vector<std::string> &args) { return Status::OK(); }
  virtual Status Execute([[maybe_unused]] engine::Context &ctx, [[maybe_unused]] Server *srv,
                         [[maybe_unused]] Connection *conn, [[maybe_unused]] std::string *output) {
    return {Status::RedisExecErr, errNotImplemented};
  }

  virtual ~Commander() = default;

 protected:
  std::vector<std::string> args_;
  const CommandAttributes *attributes_ = nullptr;
};

class CommanderWithParseMove : Commander {
 public:
  Status Parse() override { return ParseMove(std::move(args_)); }
  virtual Status ParseMove([[maybe_unused]] std::vector<std::string> &&args) { return Status::OK(); }
};

using CommanderFactory = std::function<std::unique_ptr<Commander>()>;

struct CommandKeyRange {
  // index of the first key in command tokens
  // 0 stands for no key, since the first index of command arguments is command name
  int first_key;

  // index of the last key in command tokens
  // in normal one-key commands, first key and last key index are both 1
  // -n stands for the n-th last index of the sequence, i.e. args.size() - n
  int last_key;

  // step length of key position
  // e.g. key step 2 means "key other key other ..." sequence
  int key_step;

  template <typename F>
  void ForEachKey(F &&f, const std::vector<std::string> &args) const {
    for (size_t i = first_key; last_key > 0 ? i <= size_t(last_key) : i <= args.size() + last_key; i += key_step) {
      if (i >= args.size()) continue;
      std::forward<F>(f)(args[i]);
    }
  }

  template <typename F>
  void ForEachKeyIndex(F &&f, size_t arg_size) const {
    for (size_t i = first_key; last_key > 0 ? i <= size_t(last_key) : i <= arg_size + last_key; i += key_step) {
      if (i >= arg_size) continue;
      std::forward<F>(f)(i);
    }
  }
};

using CommandKeyRangeGen = std::function<CommandKeyRange(const std::vector<std::string> &)>;

using CommandKeyRangeVecGen = std::function<std::vector<CommandKeyRange>(const std::vector<std::string> &)>;

using AdditionalFlagGen = std::function<uint64_t(uint64_t, const std::vector<std::string> &)>;

struct NoKeyInThisCommand {};
static constexpr const NoKeyInThisCommand NO_KEY{};

struct CommandAttributes {
  CommandAttributes(std::string name, int arity, CommandCategory category, uint64_t flags, AdditionalFlagGen flag_gen,
                    NoKeyInThisCommand, CommanderFactory factory)
      : name(std::move(name)),
        arity(arity),
        category(category),
        factory(std::move(factory)),
        flags_(flags),
        flag_gen_(std::move(flag_gen)),
        key_range_{0, 0, 0} {}

  CommandAttributes(std::string name, int arity, CommandCategory category, uint64_t flags, AdditionalFlagGen flag_gen,
                    CommandKeyRange key_range, CommanderFactory factory)
      : name(std::move(name)),
        arity(arity),
        category(category),
        factory(std::move(factory)),
        flags_(flags),
        flag_gen_(std::move(flag_gen)),
        key_range_(key_range) {
    if (key_range.first_key <= 0 || key_range.key_step <= 0 ||
        (key_range.last_key >= 0 && key_range.last_key < key_range.first_key)) {
      std::cout << fmt::format("Encountered invalid key range in command {}", this->name) << std::endl;
      std::abort();
    }
  }

  CommandAttributes(std::string name, int arity, CommandCategory category, uint64_t flags, AdditionalFlagGen flag_gen,
                    CommandKeyRangeGen key_range, CommanderFactory factory)
      : name(std::move(name)),
        arity(arity),
        category(category),
        factory(std::move(factory)),
        flags_(flags),
        flag_gen_(std::move(flag_gen)),
        key_range_{-1, 0, 0},
        key_range_gen_(std::move(key_range)) {}

  CommandAttributes(std::string name, int arity, CommandCategory category, uint64_t flags, AdditionalFlagGen flag_gen,
                    CommandKeyRangeVecGen key_range, CommanderFactory factory)
      : name(std::move(name)),
        arity(arity),
        category(category),
        factory(std::move(factory)),
        flags_(flags),
        flag_gen_(std::move(flag_gen)),
        key_range_{-2, 0, 0},
        key_range_vec_gen_(std::move(key_range)) {}

  // command name
  std::string name;

  // number of command arguments
  // positive number n means number of arguments is equal to n
  // negative number -n means number of arguments is equal to or large than n
  int arity;

  // category of this command, e.g. key, string, hash
  CommandCategory category;

  // commander object generator
  CommanderFactory factory;

  uint64_t InitialFlags() const { return flags_; }

  auto GenerateFlags(const std::vector<std::string> &args) const {
    uint64_t res = flags_;
    if (flag_gen_) res = flag_gen_(res, args);
    return res;
  }

  bool CheckArity(int cmd_size) const {
    return !((arity > 0 && cmd_size != arity) || (arity < 0 && cmd_size < -arity));
  }

  StatusOr<CommandKeyRange> InitialKeyRange() const {
    if (key_range_.first_key >= 0) return key_range_;
    return {Status::NotOK, "key range is unavailable without command arguments"};
  }

  // the command arguments must be parsed and in valid syntax
  // before this method is called, otherwise the behavior is UNDEFINED
  template <typename F, typename G>
  void ForEachKeyRange(F &&f, const std::vector<std::string> &args, G &&g) const {
    if (key_range_.first_key > 0) {
      std::forward<F>(f)(args, key_range_);
    } else if (key_range_.first_key == -1) {
      redis::CommandKeyRange range = key_range_gen_(args);

      if (range.first_key > 0) {
        std::forward<F>(f)(args, range);
      }
    } else if (key_range_.first_key == -2) {
      std::vector<redis::CommandKeyRange> vec_range = key_range_vec_gen_(args);

      for (const auto &range : vec_range) {
        if (range.first_key > 0) {
          std::forward<F>(f)(args, range);
        }
      }
    } else if (key_range_.first_key == 0) {
      // otherwise, if there's no key inside the command arguments
      // e.g. FLUSHALL, with "write" flag but no key specified
      std::forward<G>(g)(args);
    }
  }

  template <typename F>
  void ForEachKeyRange(F &&f, const std::vector<std::string> &args) const {
    ForEachKeyRange(std::forward<F>(f), args, [](const auto &) {});
  }

 private:
  // bitmap of enum CommandFlags
  uint64_t flags_;

  // additional flags regarding to dynamic command arguments
  AdditionalFlagGen flag_gen_;

  // static determined key range
  // if key_range.first_key == 0, there's no key in this command args
  CommandKeyRange key_range_;

  // if key_range.first_key == -1, key_range_gen is used instead
  CommandKeyRangeGen key_range_gen_;

  // if key_range.first_key == -2, key_range_vec_gen is used instead
  CommandKeyRangeVecGen key_range_vec_gen_;
};

using CommandMap = std::map<std::string, const CommandAttributes *>;

inline uint64_t ParseCommandFlags(const std::string &description, const std::string &cmd_name) {
  uint64_t flags = 0;

  for (const auto &flag : util::Split(description, " ")) {
    if (flag == "write")
      flags |= kCmdWrite;
    else if (flag == "read-only")
      flags |= kCmdReadOnly;
    else if (flag == "ok-loading")
      flags |= kCmdLoading;
    else if (flag == "exclusive")
      flags |= kCmdExclusive;
    else if (flag == "bypass-multi")
      flags |= kCmdBypassMulti;
    else if (flag == "no-multi")
      flags |= kCmdNoMulti;
    else if (flag == "no-script")
      flags |= kCmdNoScript;
    else if (flag == "no-dbsize-check")
      flags |= kCmdNoDBSizeCheck;
    else if (flag == "slow")
      flags |= kCmdSlow;
    else if (flag == "auth")
      flags |= kCmdAuth;
    else if (flag == "blocking")
      flags |= kCmdBlocking;
    else if (flag == "admin")
      flags |= kCmdAdmin;
    else {
      std::cout << fmt::format("Encountered non-existent flag '{}' in command {} in command attribute parsing", flag,
                               cmd_name)
                << std::endl;
      std::abort();
    }
  }

  return flags;
}

template <typename T>
auto MakeCmdAttr(const std::string &name, int arity, const std::string &description, NoKeyInThisCommand no_key,
                 const AdditionalFlagGen &flag_gen = {}) {
  CommandAttributes attr(name, arity, CommandCategory::Unknown, ParseCommandFlags(description, name), flag_gen, no_key,
                         []() -> std::unique_ptr<Commander> { return std::unique_ptr<Commander>(new T()); });

  return attr;
}

template <typename T>
auto MakeCmdAttr(const std::string &name, int arity, const std::string &description, int first_key, int last_key,
                 int key_step = 1, const AdditionalFlagGen &flag_gen = {}) {
  CommandAttributes attr(name, arity, CommandCategory::Unknown, ParseCommandFlags(description, name), flag_gen,
                         {first_key, last_key, key_step},
                         []() -> std::unique_ptr<Commander> { return std::unique_ptr<Commander>(new T()); });

  return attr;
}

template <typename T>
auto MakeCmdAttr(const std::string &name, int arity, const std::string &description, const CommandKeyRangeGen &gen,
                 const AdditionalFlagGen &flag_gen = {}) {
  CommandAttributes attr{name,
                         arity,
                         CommandCategory::Unknown,
                         ParseCommandFlags(description, name),
                         flag_gen,
                         gen,
                         []() -> std::unique_ptr<Commander> { return std::unique_ptr<Commander>(new T()); }};

  return attr;
}

template <typename T>
auto MakeCmdAttr(const std::string &name, int arity, const std::string &description,
                 const CommandKeyRangeVecGen &vec_gen, const AdditionalFlagGen &flag_gen = {}) {
  CommandAttributes attr{name,
                         arity,
                         CommandCategory::Unknown,
                         ParseCommandFlags(description, name),
                         flag_gen,
                         vec_gen,
                         []() -> std::unique_ptr<Commander> { return std::unique_ptr<Commander>(new T()); }};

  return attr;
}

struct RegisterToCommandTable {
  RegisterToCommandTable(CommandCategory category, std::initializer_list<CommandAttributes> list);
};

struct CommandTable {
 public:
  CommandTable() = delete;

  static CommandMap *Get();
  static const CommandMap *GetOriginal();
  static void Reset();

  static void GetAllCommandsInfo(std::string *info);
  static void GetCommandsInfo(std::string *info, const std::vector<std::string> &cmd_names);
  static std::string GetCommandInfo(const CommandAttributes *command_attributes);
  static StatusOr<std::vector<int>> GetKeysFromCommand(const CommandAttributes *attributes,
                                                       const std::vector<std::string> &cmd_tokens);

  static size_t Size();
  static bool IsExists(const std::string &name);

  static Status ParseSlotRanges(const std::string &slots_str, std::vector<SlotRange> &slots);

 private:
  static inline std::deque<CommandAttributes> redis_command_table;

  // Original Command table before rename-command directive
  static inline CommandMap original_commands;

  // Command table after rename-command directive
  static inline CommandMap commands;

  friend struct RegisterToCommandTable;
};

#define KVROCKS_CONCAT(a, b) a##b                   // NOLINT
#define KVROCKS_CONCAT2(a, b) KVROCKS_CONCAT(a, b)  // NOLINT

// NOLINTNEXTLINE
#define REDIS_REGISTER_COMMANDS(cat, ...)                                                                   \
  static RegisterToCommandTable KVROCKS_CONCAT2(register_to_command_table_, __LINE__)(CommandCategory::cat, \
                                                                                      {__VA_ARGS__});

}  // namespace redis
