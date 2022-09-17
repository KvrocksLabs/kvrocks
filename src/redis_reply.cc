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

#include "redis_reply.h"
#include <numeric>

namespace Redis {

void Reply(evbuffer *output, const std::string &data) {
  evbuffer_add(output, data.c_str(), data.length());
}

std::string SimpleString(const std::string &data) { return "+" + data + CRLF; }

std::string Error(const std::string &err) { return "-" + err + CRLF; }

std::string Integer(int64_t data) { return ":" + std::to_string(data) + CRLF; }

std::string BulkString(const std::string &data) {
  return "$" + std::to_string(data.length()) + CRLF + data + CRLF;
}

std::string NilString() {
  return "$-1\r\n";
}

std::string MultiLen(int64_t len) {
  return "*"+std::to_string(len)+"\r\n";
}

std::string MultiBulkString(const std::vector<std::string>& values, bool output_nil_for_empty_string) {
  std::string result = "*" + std::to_string(values.size()) + CRLF;
  for (size_t i = 0; i < values.size(); i++) {
    if (values[i].empty() && output_nil_for_empty_string) {
      result += NilString();
    }  else {
      result += BulkString(values[i]);
    }
  }
  return result;
}


std::string MultiBulkString(const std::vector<std::string>& values, const std::vector<rocksdb::Status> &statuses) {
  std::string result = "*" + std::to_string(values.size()) + CRLF;
  for (size_t i = 0; i < values.size(); i++) {
    if (i < statuses.size() && !statuses[i].ok()) {
      result += NilString();
    } else {
      result += BulkString(values[i]);
    }
  }
  return result;
}

std::string Array(const std::vector<std::string>& list) {
  size_t n = std::accumulate(
      list.begin(), list.end(), 0, [] (size_t n, const std::string &s) { return n + s.size(); });
  std::string result = "*" + std::to_string(list.size()) + CRLF;
  std::string::size_type final_size = result.size() + n;
  result.reserve(final_size);
  for(const auto& i : list) result += i;
  return result;
}

std::string Command2RESP(const std::vector<std::string> &cmd_args) {
  std::string output;
  output.append("*" + std::to_string(cmd_args.size()) + CRLF);
  for (const auto &arg : cmd_args) {
    output.append("$" + std::to_string(arg.size()) + CRLF);
    output.append(arg + CRLF);
  }
  return output;
}

}  // namespace Redis
