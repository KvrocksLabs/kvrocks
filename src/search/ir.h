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

#include <fmt/format.h>

#include <initializer_list>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

#include "fmt/core.h"
#include "ir_iterator.h"
#include "search/index_info.h"
#include "string_util.h"
#include "type_util.h"

// kqir stands for Kvrocks Query Intermediate Representation
namespace kqir {

struct Node {
  virtual std::string Dump() const = 0;
  virtual std::string_view Name() const = 0;
  virtual std::string Content() const { return {}; }

  virtual NodeIterator ChildBegin() { return {}; };
  virtual NodeIterator ChildEnd() { return {}; };

  virtual std::unique_ptr<Node> Clone() const = 0;

  template <typename T>
  std::unique_ptr<T> CloneAs() const {
    return Node::MustAs<T>(Clone());
  }

  virtual ~Node() = default;

  template <typename T, typename U = Node, typename... Args>
  static std::unique_ptr<U> Create(Args &&...args) {
    return std::unique_ptr<U>(new T(std::forward<Args>(args)...));
  }

  template <typename T, typename U>
  static std::unique_ptr<T> MustAs(std::unique_ptr<U> &&original) {
    auto casted = As<T>(std::move(original));
    CHECK(casted != nullptr);
    return casted;
  }

  template <typename T, typename U>
  static std::unique_ptr<T> As(std::unique_ptr<U> &&original) {
    auto casted = dynamic_cast<T *>(original.get());
    if (casted) original.release();
    return std::unique_ptr<T>(casted);
  }

  template <typename T = Node, typename... Args>
  static std::vector<std::unique_ptr<T>> List(std::unique_ptr<Args>... args) {
    std::vector<std::unique_ptr<T>> result;
    result.reserve(sizeof...(Args));
    (result.push_back(std::move(args)), ...);
    return result;
  }
};

struct Ref : Node {};

struct FieldRef : Ref {
  std::string name;
  const FieldInfo *info = nullptr;

  explicit FieldRef(std::string name) : name(std::move(name)) {}
  FieldRef(std::string name, const FieldInfo *info) : name(std::move(name)), info(info) {}

  std::string_view Name() const override { return "FieldRef"; }
  std::string Dump() const override { return name; }
  std::string Content() const override { return Dump(); }

  std::unique_ptr<Node> Clone() const override { return std::make_unique<FieldRef>(*this); }
};

struct Literal : virtual Node {};

struct StringLiteral : Literal {
  std::string val;

  explicit StringLiteral(std::string val) : val(std::move(val)) {}

  std::string_view Name() const override { return "StringLiteral"; }
  std::string Dump() const override { return fmt::format("\"{}\"", util::EscapeString(val)); }
  std::string Content() const override { return Dump(); }

  std::unique_ptr<Node> Clone() const override { return std::make_unique<StringLiteral>(*this); }
};

struct QueryExpr : virtual Node {};

struct BoolAtomExpr : QueryExpr {};

struct TagContainExpr : BoolAtomExpr {
  std::unique_ptr<FieldRef> field;
  std::unique_ptr<StringLiteral> tag;

  TagContainExpr(std::unique_ptr<FieldRef> &&field, std::unique_ptr<StringLiteral> &&tag)
      : field(std::move(field)), tag(std::move(tag)) {}

  std::string_view Name() const override { return "TagContainExpr"; }
  std::string Dump() const override { return fmt::format("{} hastag {}", field->Dump(), tag->Dump()); }

  NodeIterator ChildBegin() override { return {field.get(), tag.get()}; };
  NodeIterator ChildEnd() override { return {}; };

  std::unique_ptr<Node> Clone() const override {
    return std::make_unique<TagContainExpr>(Node::MustAs<FieldRef>(field->Clone()),
                                            Node::MustAs<StringLiteral>(tag->Clone()));
  }
};

struct TextContainExpr : BoolAtomExpr {
  std::unique_ptr<FieldRef> field;
  std::unique_ptr<StringLiteral> word;

  TextContainExpr(std::unique_ptr<FieldRef> &&field, std::unique_ptr<StringLiteral> &&word)
      : field(std::move(field)), word(std::move(word)) {}

  std::string_view Name() const override { return "TextContainExpr"; }
  std::string Dump() const override { return fmt::format("{} contains {}", field->Dump(), word->Dump()); }

  NodeIterator ChildBegin() override { return {field.get(), word.get()}; };
  NodeIterator ChildEnd() override { return {}; };

  std::unique_ptr<Node> Clone() const override {
    return std::make_unique<TagContainExpr>(Node::MustAs<FieldRef>(field->Clone()),
                                            Node::MustAs<StringLiteral>(word->Clone()));
  }
};

struct NumericLiteral : Literal {
  double val;

  explicit NumericLiteral(double val) : val(val) {}

  std::string_view Name() const override { return "NumericLiteral"; }
  std::string Dump() const override { return fmt::format("{}", val); }
  std::string Content() const override { return Dump(); }

  std::unique_ptr<Node> Clone() const override { return std::make_unique<NumericLiteral>(*this); }
};

// NOLINTNEXTLINE
#define KQIR_NUMERIC_COMPARE_OPS(X) \
  X(EQ, =, NE, EQ) X(NE, !=, EQ, NE) X(LT, <, GET, GT) X(LET, <=, GT, GET) X(GT, >, LET, LT) X(GET, >=, LT, LET)

struct NumericCompareExpr : BoolAtomExpr {
  enum Op {
#define X(n, s, o, f) n,  // NOLINT
    KQIR_NUMERIC_COMPARE_OPS(X)
#undef X
  } op;
  std::unique_ptr<FieldRef> field;
  std::unique_ptr<NumericLiteral> num;

  NumericCompareExpr(Op op, std::unique_ptr<FieldRef> &&field, std::unique_ptr<NumericLiteral> &&num)
      : op(op), field(std::move(field)), num(std::move(num)) {}

  static constexpr const char *ToOperator(Op op) {
    switch (op) {
// NOLINTNEXTLINE
#define X(n, s, o, f) \
  case n:             \
    return #s;
      KQIR_NUMERIC_COMPARE_OPS(X)
#undef X
    }

    return nullptr;
  }

  static constexpr std::optional<Op> FromOperator(std::string_view op) {
// NOLINTNEXTLINE
#define X(n, s, o, f) \
  if (op == #s) return n;
    KQIR_NUMERIC_COMPARE_OPS(X)
#undef X

    return std::nullopt;
  }

  static constexpr Op Negative(Op op) {
    switch (op) {
// NOLINTNEXTLINE
#define X(n, s, o, f) \
  case n:             \
    return o;
      KQIR_NUMERIC_COMPARE_OPS(X)
#undef X
    }

    __builtin_unreachable();
  }

  static constexpr Op Flip(Op op) {
    switch (op) {
// NOLINTNEXTLINE
#define X(n, s, o, f) \
  case n:             \
    return f;
      KQIR_NUMERIC_COMPARE_OPS(X)
#undef X
    }

    __builtin_unreachable();
  }

  std::string_view Name() const override { return "NumericCompareExpr"; }
  std::string Dump() const override { return fmt::format("{} {} {}", field->Dump(), ToOperator(op), num->Dump()); };
  std::string Content() const override { return ToOperator(op); }

  NodeIterator ChildBegin() override { return {field.get(), num.get()}; };
  NodeIterator ChildEnd() override { return {}; };

  std::unique_ptr<Node> Clone() const override {
    return std::make_unique<NumericCompareExpr>(op, Node::MustAs<FieldRef>(field->Clone()),
                                                Node::MustAs<NumericLiteral>(num->Clone()));
  }
};

struct VectorLiteral : Literal {
  std::vector<double> values;

  explicit VectorLiteral(std::vector<double> &&values) : values(std::move(values)){};

  std::string_view Name() const override { return "VectorLiteral"; }
  std::string Dump() const override {
    return fmt::format("[{}]", util::StringJoin(values, [](auto v) { return std::to_string(v); }));
  }
  std::string Content() const override { return Dump(); }

  std::unique_ptr<Node> Clone() const override { return std::make_unique<VectorLiteral>(*this); }
};

struct VectorRangeExpr : BoolAtomExpr {
  std::unique_ptr<FieldRef> field;
  std::unique_ptr<NumericLiteral> range;
  std::unique_ptr<VectorLiteral> vector;

  VectorRangeExpr(std::unique_ptr<FieldRef> &&field, std::unique_ptr<NumericLiteral> &&range,
                  std::unique_ptr<VectorLiteral> &&vector)
      : field(std::move(field)), range(std::move(range)), vector(std::move(vector)) {}

  std::string_view Name() const override { return "VectorRangeExpr"; }
  std::string Dump() const override {
    return fmt::format("{} <-> {} < {}", field->Dump(), vector->Dump(), range->Dump());
  }

  std::unique_ptr<Node> Clone() const override {
    return std::make_unique<VectorRangeExpr>(Node::MustAs<FieldRef>(field->Clone()),
                                             Node::MustAs<NumericLiteral>(range->Clone()),
                                             Node::MustAs<VectorLiteral>(vector->Clone()));
  }
};

struct VectorKnnExpr : BoolAtomExpr {
  std::unique_ptr<FieldRef> field;
  std::unique_ptr<VectorLiteral> vector;
  size_t k;

  VectorKnnExpr(std::unique_ptr<FieldRef> &&field, std::unique_ptr<VectorLiteral> &&vector, size_t k)
      : field(std::move(field)), vector(std::move(vector)), k(k) {}

  std::string_view Name() const override { return "VectorKnnExpr"; }
  std::string Dump() const override { return fmt::format("KNN k={}, {} <-> {}", k, field->Dump(), vector->Dump()); }

  std::unique_ptr<Node> Clone() const override {
    return std::make_unique<VectorKnnExpr>(Node::MustAs<FieldRef>(field->Clone()),
                                           Node::MustAs<VectorLiteral>(vector->Clone()), k);
  }
};

struct BoolLiteral : BoolAtomExpr, Literal {
  bool val;

  explicit BoolLiteral(bool val) : val(val) {}

  std::string_view Name() const override { return "BoolLiteral"; }
  std::string Dump() const override { return val ? "true" : "false"; }
  std::string Content() const override { return Dump(); }

  std::unique_ptr<Node> Clone() const override { return std::make_unique<BoolLiteral>(*this); }
};

struct QueryExpr;

struct NotExpr : QueryExpr {
  std::unique_ptr<QueryExpr> inner;

  explicit NotExpr(std::unique_ptr<QueryExpr> &&inner) : inner(std::move(inner)) {}

  std::string_view Name() const override { return "NotExpr"; }
  std::string Dump() const override { return fmt::format("not {}", inner->Dump()); }

  NodeIterator ChildBegin() override { return NodeIterator{inner.get()}; };
  NodeIterator ChildEnd() override { return {}; };

  std::unique_ptr<Node> Clone() const override {
    return std::make_unique<NotExpr>(Node::MustAs<QueryExpr>(inner->Clone()));
  }
};

struct AndExpr : QueryExpr {
  std::vector<std::unique_ptr<QueryExpr>> inners;

  explicit AndExpr(std::vector<std::unique_ptr<QueryExpr>> &&inners) : inners(std::move(inners)) {}

  static std::unique_ptr<QueryExpr> Create(std::vector<std::unique_ptr<QueryExpr>> &&exprs) {
    CHECK(!exprs.empty());

    if (exprs.size() == 1) {
      return std::move(exprs.front());
    }

    return std::make_unique<AndExpr>(std::move(exprs));
  }

  std::string_view Name() const override { return "AndExpr"; }
  std::string Dump() const override {
    return fmt::format("(and {})", util::StringJoin(inners, [](const auto &v) { return v->Dump(); }));
  }

  NodeIterator ChildBegin() override { return NodeIterator(inners.begin()); };
  NodeIterator ChildEnd() override { return NodeIterator(inners.end()); };

  std::unique_ptr<Node> Clone() const override {
    std::vector<std::unique_ptr<QueryExpr>> res;
    res.reserve(inners.size());
    for (const auto &n : inners) {
      res.push_back(Node::MustAs<QueryExpr>(n->Clone()));
    }
    return std::make_unique<AndExpr>(std::move(res));
  }
};

struct OrExpr : QueryExpr {
  std::vector<std::unique_ptr<QueryExpr>> inners;

  explicit OrExpr(std::vector<std::unique_ptr<QueryExpr>> &&inners) : inners(std::move(inners)) {}

  static std::unique_ptr<QueryExpr> Create(std::vector<std::unique_ptr<QueryExpr>> &&exprs) {
    CHECK(!exprs.empty());

    if (exprs.size() == 1) {
      return std::move(exprs.front());
    }

    return std::make_unique<OrExpr>(std::move(exprs));
  }

  std::string_view Name() const override { return "OrExpr"; }
  std::string Dump() const override {
    return fmt::format("(or {})", util::StringJoin(inners, [](const auto &v) { return v->Dump(); }));
  }

  NodeIterator ChildBegin() override { return NodeIterator(inners.begin()); };
  NodeIterator ChildEnd() override { return NodeIterator(inners.end()); };

  std::unique_ptr<Node> Clone() const override {
    std::vector<std::unique_ptr<QueryExpr>> res;
    res.reserve(inners.size());
    for (const auto &n : inners) {
      res.push_back(Node::MustAs<QueryExpr>(n->Clone()));
    }
    return std::make_unique<OrExpr>(std::move(res));
  }
};

struct LimitClause : Node {
  size_t offset = 0;
  size_t count = std::numeric_limits<size_t>::max();

  LimitClause(size_t offset, size_t count) : offset(offset), count(count) {}

  std::string_view Name() const override { return "LimitClause"; }
  std::string Dump() const override { return fmt::format("limit {}, {}", offset, count); }
  std::string Content() const override { return fmt::format("{}, {}", offset, count); }

  std::unique_ptr<Node> Clone() const override { return std::make_unique<LimitClause>(*this); }
  size_t Offset() const { return offset; }

  size_t Count() const { return count; }
};

struct SortByClause : Node {
  enum Order { ASC, DESC } order = ASC;
  std::unique_ptr<FieldRef> field;
  std::unique_ptr<VectorLiteral> vector = nullptr;

  SortByClause(Order order, std::unique_ptr<FieldRef> &&field) : order(order), field(std::move(field)) {}
  SortByClause(std::unique_ptr<FieldRef> &&field, std::unique_ptr<VectorLiteral> &&vector)
      : field(std::move(field)), vector(std::move(vector)) {}

  static constexpr const char *OrderToString(Order order) { return order == ASC ? "asc" : "desc"; }
  bool IsVectorField() const { return vector != nullptr; }

  std::string_view Name() const override { return "SortByClause"; }
  std::string Dump() const override {
    if (!IsVectorField()) {
      return fmt::format("sortby {}, {}", field->Dump(), OrderToString(order));
    }
    return fmt::format("sortby {} <-> {}", field->Dump(), vector->Dump());
  }
  std::string Content() const override { return OrderToString(order); }

  NodeIterator ChildBegin() override { return NodeIterator(field.get()); };
  NodeIterator ChildEnd() override { return {}; };

  std::unique_ptr<Node> Clone() const override {
    return std::make_unique<SortByClause>(order, Node::MustAs<FieldRef>(field->Clone()));
  }

  std::unique_ptr<FieldRef> TakeFieldRef() { return std::move(field); }

  std::unique_ptr<VectorLiteral> TakeVectorLiteral() { return std::move(vector); }
};

struct SelectClause : Node {
  std::vector<std::unique_ptr<FieldRef>> fields;

  explicit SelectClause(std::vector<std::unique_ptr<FieldRef>> &&fields) : fields(std::move(fields)) {}

  std::string_view Name() const override { return "SelectClause"; }
  std::string Dump() const override {
    if (fields.empty()) return "select *";
    return fmt::format("select {}", util::StringJoin(fields, [](const auto &v) { return v->Dump(); }));
  }

  NodeIterator ChildBegin() override { return NodeIterator(fields.begin()); };
  NodeIterator ChildEnd() override { return NodeIterator(fields.end()); };

  std::unique_ptr<Node> Clone() const override {
    std::vector<std::unique_ptr<FieldRef>> res;
    res.reserve(fields.size());
    for (const auto &f : fields) {
      res.push_back(Node::MustAs<FieldRef>(f->Clone()));
    }
    return std::make_unique<SelectClause>(std::move(res));
  }
};

struct IndexRef : Ref {
  std::string name;
  const IndexInfo *info = nullptr;

  explicit IndexRef(std::string name) : name(std::move(name)) {}
  explicit IndexRef(std::string name, const IndexInfo *info) : name(std::move(name)), info(info) {}

  std::string_view Name() const override { return "IndexRef"; }
  std::string Dump() const override { return name; }
  std::string Content() const override { return Dump(); }

  std::unique_ptr<Node> Clone() const override { return std::make_unique<IndexRef>(*this); }
};

struct SearchExpr : Node {
  std::unique_ptr<SelectClause> select;
  std::unique_ptr<IndexRef> index;
  std::unique_ptr<QueryExpr> query_expr;
  std::unique_ptr<LimitClause> limit;     // optional
  std::unique_ptr<SortByClause> sort_by;  // optional

  SearchExpr(std::unique_ptr<IndexRef> &&index, std::unique_ptr<QueryExpr> &&query_expr,
             std::unique_ptr<LimitClause> &&limit, std::unique_ptr<SortByClause> &&sort_by,
             std::unique_ptr<SelectClause> &&select)
      : select(std::move(select)),
        index(std::move(index)),
        query_expr(std::move(query_expr)),
        limit(std::move(limit)),
        sort_by(std::move(sort_by)) {}

  std::string_view Name() const override { return "SearchExpr"; }
  std::string Dump() const override {
    std::string opt;
    if (sort_by) opt += " " + sort_by->Dump();
    if (limit) opt += " " + limit->Dump();
    return fmt::format("{} from {} where {}{}", select->Dump(), index->Dump(), query_expr->Dump(), opt);
  }

  static inline const std::vector<std::function<Node *(Node *)>> ChildMap = {
      NodeIterator::MemFn<&SearchExpr::select>,     NodeIterator::MemFn<&SearchExpr::index>,
      NodeIterator::MemFn<&SearchExpr::query_expr>, NodeIterator::MemFn<&SearchExpr::limit>,
      NodeIterator::MemFn<&SearchExpr::sort_by>,
  };

  NodeIterator ChildBegin() override { return NodeIterator(this, ChildMap.begin()); };
  NodeIterator ChildEnd() override { return NodeIterator(this, ChildMap.end()); };

  std::unique_ptr<Node> Clone() const override {
    return std::make_unique<SearchExpr>(
        Node::MustAs<IndexRef>(index->Clone()), Node::MustAs<QueryExpr>(query_expr->Clone()),
        limit ? Node::MustAs<LimitClause>(limit->Clone()) : nullptr,
        sort_by ? Node::MustAs<SortByClause>(sort_by->Clone()) : nullptr, Node::MustAs<SelectClause>(select->Clone()));
  }
};

}  // namespace kqir
