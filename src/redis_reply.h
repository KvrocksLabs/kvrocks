#pragma once

#include <string>
#include <vector>
#include <event2/buffer.h>

#define CRLF "\r\n"

namespace Redis {
void Reply(evbuffer *output, const std::string &data);
std::string SimpleString(const std::string &data);
std::string Error(const std::string &err);
std::string Integer(int64_t data);
std::string BulkString(const std::string &data, bool output_nil_for_empty_string = true);
std::string NilString();
std::string MultiLen(int64_t len);
std::string Array(std::vector<std::string> list);
std::string MultiBulkString(std::vector<std::string> list);
std::string ParseSimpleString(evbuffer *input);
}  // namespace Redis
