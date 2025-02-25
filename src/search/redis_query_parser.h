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

#include <tao/pegtl.hpp>

#include "common_parser.h"

namespace kqir {

namespace redis_query {

using namespace peg;

struct VectorRangeToken : string<'V', 'E', 'C', 'T', 'O', 'R', '_', 'R', 'A', 'N', 'G', 'E'> {};
struct KnnToken : string<'K', 'N', 'N'> {};
struct ArrowOp : string<'=', '>'> {};
struct Wildcard : one<'*'> {};

struct Field : seq<one<'@'>, Identifier> {};

struct Param : seq<one<'$'>, Identifier> {};

struct Tag : sor<Identifier, StringL, Param> {};
struct TagList : seq<one<'{'>, WSPad<Tag>, star<seq<one<'|'>, WSPad<Tag>>>, one<'}'>> {};

struct NumberOrParam : sor<Number, Param> {};
struct UintOrParam : sor<UnsignedInteger, Param> {};

struct Inf : seq<opt<one<'+', '-'>>, string<'i', 'n', 'f'>> {};
struct ExclusiveNumber : seq<one<'('>, NumberOrParam> {};
struct NumericRangePart : sor<Inf, ExclusiveNumber, NumberOrParam> {};
struct NumericRange : seq<one<'['>, WSPad<NumericRangePart>, WSPad<NumericRangePart>, one<']'>> {};

struct KnnSearch : seq<one<'['>, WSPad<KnnToken>, WSPad<UintOrParam>, WSPad<Field>, WSPad<Param>, one<']'>> {};
struct VectorRange : seq<one<'['>, WSPad<VectorRangeToken>, WSPad<NumberOrParam>, WSPad<Param>, one<']'>> {};

struct TextContains : sor<Identifier, StringL, Param> {};

struct FieldQuery : seq<WSPad<Field>, one<':'>, WSPad<sor<VectorRange, TagList, NumericRange, TextContains>>> {};

struct QueryExpr;

struct ParenExpr : WSPad<seq<one<'('>, QueryExpr, one<')'>>> {};

struct NotExpr;

struct BooleanExpr : sor<FieldQuery, ParenExpr, NotExpr, WSPad<Wildcard>> {};

struct NotExpr : seq<WSPad<one<'-'>>, BooleanExpr> {};

struct AndExpr : seq<BooleanExpr, plus<seq<BooleanExpr>>> {};
struct AndExprP : sor<AndExpr, BooleanExpr> {};

struct OrExpr : seq<AndExprP, plus<seq<one<'|'>, AndExprP>>> {};
struct OrExprP : sor<OrExpr, AndExprP> {};

struct PrefilterExpr : seq<WSPad<Wildcard>, ArrowOp, WSPad<KnnSearch>> {};

struct QueryP : sor<PrefilterExpr, OrExprP> {};

struct QueryExpr : seq<QueryP> {};

}  // namespace redis_query

}  // namespace kqir
