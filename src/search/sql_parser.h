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

namespace sql {

using namespace peg;

struct Param : seq<one<'@'>, Identifier> {};
struct StringOrParam : sor<StringL, Param> {};
struct NumberOrParam : sor<Number, Param> {};

struct HasTag : string<'h', 'a', 's', 't', 'a', 'g'> {};
struct Tagged : string<'t', 'a', 'g', 'g', 'e', 'd'> {};
struct HasTagExpr : WSPad<seq<Identifier, WSPad<sor<HasTag, Tagged>>, StringOrParam>> {};

struct NumericAtomExpr : WSPad<sor<NumberOrParam, Identifier>> {};
struct NumericCompareOp : sor<string<'!', '='>, string<'<', '='>, string<'>', '='>, one<'=', '<', '>'>> {};
struct NumericCompareExpr : seq<NumericAtomExpr, NumericCompareOp, NumericAtomExpr> {};

struct VectorCompareOp : string<'<', '-', '>'> {};
struct VectorLiteral : seq<WSPad<one<'['>>, Number, star<seq<WSPad<one<','>>>, Number>, WSPad<one<']'>>> {};
struct VectorCompareExpr : seq<WSPad<Identifier>, VectorCompareOp, WSPad<VectorLiteral>> {};
struct VectorRangeExpr : seq<VectorCompareExpr, one<'<'>, WSPad<NumberOrParam>> {};

struct TextContains : string<'c', 'o', 'n', 't', 'a', 'i', 'n', 's'> {};
struct TextContainsExpr : seq<WSPad<Identifier>, TextContains, WSPad<StringOrParam>> {};

struct BooleanAtomExpr : sor<HasTagExpr, NumericCompareExpr, VectorRangeExpr, WSPad<Boolean>> {};

struct QueryExpr;

struct ParenExpr : WSPad<seq<one<'('>, QueryExpr, one<')'>>> {};

struct NotExpr;

struct BooleanExpr : sor<BooleanAtomExpr, ParenExpr, NotExpr> {};

struct Not : string<'n', 'o', 't'> {};
struct NotExpr : seq<WSPad<Not>, BooleanExpr> {};

struct And : string<'a', 'n', 'd'> {};
// left recursion elimination
// struct AndExpr : sor<seq<AndExpr, And, BooleanExpr>, BooleanExpr> {};
struct AndExpr : seq<BooleanExpr, plus<seq<And, BooleanExpr>>> {};
struct AndExprP : sor<AndExpr, BooleanExpr> {};

struct Or : string<'o', 'r'> {};
// left recursion elimination
// struct OrExpr : sor<seq<OrExpr, Or, AndExpr>, AndExpr> {};
struct OrExpr : seq<AndExprP, plus<seq<Or, AndExprP>>> {};
struct OrExprP : sor<OrExpr, AndExprP> {};

struct QueryExpr : seq<OrExprP> {};

struct Select : string<'s', 'e', 'l', 'e', 'c', 't'> {};
struct From : string<'f', 'r', 'o', 'm'> {};

struct Wildcard : one<'*'> {};
struct IdentifierList : seq<Identifier, star<WSPad<one<','>>, Identifier>> {};
struct SelectExpr : WSPad<sor<Wildcard, IdentifierList>> {};
struct FromExpr : WSPad<Identifier> {};

struct Where : string<'w', 'h', 'e', 'r', 'e'> {};
struct OrderBy : seq<string<'o', 'r', 'd', 'e', 'r'>, plus<WhiteSpace>, string<'b', 'y'>> {};
struct Asc : string<'a', 's', 'c'> {};
struct Desc : string<'d', 'e', 's', 'c'> {};
struct Limit : string<'l', 'i', 'm', 'i', 't'> {};

struct WhereClause : seq<Where, QueryExpr> {};
struct AscOrDesc : sor<Asc, Desc> {};
struct SortableFieldExpr : seq<WSPad<Identifier>, opt<AscOrDesc>> {};
struct OrderByExpr : sor<WSPad<VectorCompareExpr>, WSPad<SortableFieldExpr>> {};
struct OrderByClause : seq<OrderBy, OrderByExpr> {};
struct LimitClause : seq<Limit, opt<seq<WSPad<UnsignedInteger>, one<','>>>, WSPad<UnsignedInteger>> {};

struct SearchStmt
    : WSPad<seq<Select, SelectExpr, From, FromExpr, opt<WhereClause>, opt<OrderByClause>, opt<LimitClause>>> {};

}  // namespace sql

}  // namespace kqir
