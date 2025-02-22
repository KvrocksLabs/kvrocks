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

namespace redis {

inline constexpr const char *errNotImplemented = "not implemented";
inline constexpr const char *errInvalidSyntax = "syntax error";
inline constexpr const char *errInvalidExpireTime = "invalid expire time";
inline constexpr const char *errWrongNumOfArguments = "wrong number of arguments";
inline constexpr const char *errInvalidNumOfAttributes = "number of attributes is not as required";
inline constexpr const char *errValueNotInteger = "value is not an integer or out of range";
inline constexpr const char *errAdminPermissionRequired = "admin permission required to perform the command";
inline constexpr const char *errValueMustBePositive = "value is out of range, must be positive";
inline constexpr const char *errNoSuchKey = "no such key";
inline constexpr const char *errUnbalancedStreamList =
    "Unbalanced XREAD list of streams: for each stream key an ID or '$' must be specified.";
inline constexpr const char *errTimeoutIsNegative = "timeout is negative";
inline constexpr const char *errTimeoutIsNotFloat = "timeout is not a float or out of range";
inline constexpr const char *errLimitIsNegative = "LIMIT can't be negative";
inline constexpr const char *errLimitOptionNotAllowed =
    "syntax error, LIMIT cannot be used without the special ~ option";
inline constexpr const char *errZSetLTGTNX = "GT, LT, and/or NX options at the same time are not compatible";
inline constexpr const char *errScoreIsNotValidFloat = "score is not a valid float";
inline constexpr const char *errValueIsNotFloat = "value is not a valid float";
inline constexpr const char *errNoMatchingScript = "No matching script. Please use EVAL";
inline constexpr const char *errUnknownOption = "unknown option";
inline constexpr const char *errUnknownSubcommandOrWrongArguments = "Unknown subcommand or wrong number of arguments";
inline constexpr const char *errRestoringBackup = "kvrocks is restoring the db from backup";
}  // namespace redis
