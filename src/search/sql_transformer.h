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

#include <limits>
#include <memory>
#include <variant>

#include "common_transformer.h"
#include "fmt/format.h"
#include "ir.h"
#include "parse_util.h"
#include "sql_parser.h"

namespace kqir {

namespace sql {

namespace ir = kqir;

template <typename Rule>
using TreeSelector = parse_tree::selector<
    Rule,
    parse_tree::store_content::on<Boolean, Number, StringL, Param, Identifier, NumericCompareOp, AscOrDesc,
                                  UnsignedInteger>,
    parse_tree::remove_content::on<HasTagExpr, NumericCompareExpr, TextContainsExpr, VectorCompareOp, VectorLiteral,
                                   VectorCompareExpr, VectorRangeExpr, NotExpr, AndExpr, OrExpr, Wildcard, SelectExpr,
                                   FromExpr, WhereClause, OrderByClause, OrderByExpr, LimitClause, SearchStmt>>;

template <typename Input>
StatusOr<std::unique_ptr<parse_tree::node>> ParseToTree(Input&& in) {
  if (auto root = parse_tree::parse<seq<SearchStmt, eof>, TreeSelector>(std::forward<Input>(in))) {
    return root;
  } else {
    // TODO: improve parse error message, with source location
    return {Status::NotOK, "invalid syntax"};
  }
}

struct Transformer : ir::TreeTransformer {
  explicit Transformer(const ParamMap& param_map) : TreeTransformer(param_map) {}

  auto Transform(const TreeNode& node) -> StatusOr<std::unique_ptr<Node>> {
    auto number_or_param = [this](const TreeNode& node) -> StatusOr<std::unique_ptr<NumericLiteral>> {
      if (Is<Number>(node)) {
        return Node::MustAs<ir::NumericLiteral>(GET_OR_RET(Transform(node)));
      } else if (Is<Param>(node)) {
        auto val = GET_OR_RET(ParseFloat(GET_OR_RET(GetParam(node)))
                                  .Prefixed(fmt::format("parameter {} is not a number", node->string_view())));

        return std::make_unique<ir::NumericLiteral>(val);
      } else {
        return {Status::NotOK,
                fmt::format("expected a number or a parameter in numeric comparison but got {}", node->type)};
      }
    };

    if (Is<Boolean>(node)) {
      return Node::Create<ir::BoolLiteral>(node->string_view() == "true");
    } else if (Is<Number>(node)) {
      return Node::Create<ir::NumericLiteral>(*ParseFloat(node->string()));
    } else if (Is<StringL>(node)) {
      return Node::Create<ir::StringLiteral>(GET_OR_RET(UnescapeString(node->string_view())));
    } else if (Is<VectorLiteral>(node)) {
      std::vector<double> values;
      for (const auto& child : node->children) {
        values.push_back(*ParseFloat(child->string()));
      }
      return Node::Create<ir::VectorLiteral>(std::move(values));
    } else if (Is<HasTagExpr>(node)) {
      CHECK(node->children.size() == 2);

      const auto& tag = node->children[1];
      std::unique_ptr<ir::StringLiteral> res;
      if (Is<StringL>(tag)) {
        res = Node::MustAs<ir::StringLiteral>(GET_OR_RET(Transform(tag)));
      } else if (Is<Param>(tag)) {
        res = std::make_unique<ir::StringLiteral>(GET_OR_RET(GetParam(tag)));
      } else {
        return {Status::NotOK, "encountered invalid tag"};
      }

      return Node::Create<ir::TagContainExpr>(std::make_unique<ir::FieldRef>(node->children[0]->string()),
                                              std::move(res));
    } else if (Is<NumericCompareExpr>(node)) {
      CHECK(node->children.size() == 3);

      const auto& lhs = node->children[0];
      const auto& rhs = node->children[2];

      auto op = ir::NumericCompareExpr::FromOperator(node->children[1]->string_view()).value();
      if (Is<Identifier>(lhs) && (Is<Number>(rhs) || Is<Param>(rhs))) {
        return Node::Create<ir::NumericCompareExpr>(op, std::make_unique<ir::FieldRef>(lhs->string()),
                                                    GET_OR_RET(number_or_param(rhs)));
      } else if ((Is<Number>(lhs) || Is<Param>(lhs)) && Is<Identifier>(rhs)) {
        return Node::Create<ir::NumericCompareExpr>(ir::NumericCompareExpr::Flip(op),
                                                    std::make_unique<ir::FieldRef>(rhs->string()),
                                                    GET_OR_RET(number_or_param(lhs)));
      } else {
        return {Status::NotOK, "the left and right side of numeric comparison should be an identifier and a number"};
      }
    } else if (Is<VectorRangeExpr>(node)) {
      CHECK(node->children.size() == 2);
      const auto& vector_comp_expr = node->children[0];
      CHECK(vector_comp_expr->children.size() == 3);

      return Node::Create<ir::VectorRangeExpr>(
          std::make_unique<ir::FieldRef>(vector_comp_expr->children[0]->string()),
          GET_OR_RET(number_or_param(node->children[1])),
          Node::MustAs<ir::VectorLiteral>(GET_OR_RET(Transform(vector_comp_expr->children[2]))));
    } else if (Is<TextContainsExpr>(node)) {
      CHECK(node->children.size() == 2);

      const auto& word = node->children[1];
      std::unique_ptr<ir::StringLiteral> res;
      if (Is<StringL>(word)) {
        res = Node::MustAs<ir::StringLiteral>(GET_OR_RET(Transform(word)));
      } else if (Is<Param>(word)) {
        res = std::make_unique<ir::StringLiteral>(GET_OR_RET(GetParam(word)));
      } else {
        return {Status::NotOK, "encountered invalid word"};
      }

      return Node::Create<ir::TextContainExpr>(std::make_unique<ir::FieldRef>(node->children[0]->string()),
                                               std::move(res));
    } else if (Is<NotExpr>(node)) {
      CHECK(node->children.size() == 1);

      return Node::Create<ir::NotExpr>(Node::MustAs<ir::QueryExpr>(GET_OR_RET(Transform(node->children[0]))));
    } else if (Is<AndExpr>(node)) {
      std::vector<std::unique_ptr<ir::QueryExpr>> exprs;

      for (const auto& child : node->children) {
        exprs.push_back(Node::MustAs<ir::QueryExpr>(GET_OR_RET(Transform(child))));
      }

      return Node::Create<ir::AndExpr>(std::move(exprs));
    } else if (Is<OrExpr>(node)) {
      std::vector<std::unique_ptr<ir::QueryExpr>> exprs;

      for (const auto& child : node->children) {
        exprs.push_back(Node::MustAs<ir::QueryExpr>(GET_OR_RET(Transform(child))));
      }

      return Node::Create<ir::OrExpr>(std::move(exprs));
    } else if (Is<SelectExpr>(node)) {
      std::vector<std::unique_ptr<ir::FieldRef>> fields;

      if (node->children.size() == 1 && Is<Wildcard>(node->children[0])) {
        return Node::Create<ir::SelectClause>(std::move(fields));
      }

      for (const auto& child : node->children) {
        fields.push_back(std::make_unique<ir::FieldRef>(child->string()));
      }

      return Node::Create<ir::SelectClause>(std::move(fields));
    } else if (Is<FromExpr>(node)) {
      CHECK(node->children.size() == 1);
      return Node::Create<ir::IndexRef>(node->children[0]->string());
    } else if (Is<WhereClause>(node)) {
      CHECK(node->children.size() == 1);
      return Transform(node->children[0]);
    } else if (Is<LimitClause>(node)) {
      CHECK(node->children.size() == 1 || node->children.size() == 2);

      size_t offset = 0, count = std::numeric_limits<size_t>::max();
      if (node->children.size() == 1) {
        count = *ParseInt(node->children[0]->string());
      } else {
        offset = *ParseInt(node->children[0]->string());
        count = *ParseInt(node->children[1]->string());
      }

      return Node::Create<ir::LimitClause>(offset, count);
    } else if (Is<OrderByClause>(node)) {
      CHECK(node->children.size() == 1);
      const auto& order_by_expr = node->children[0];
      CHECK(order_by_expr->children.size() == 1 || order_by_expr->children.size() == 2);

      if (Is<VectorCompareExpr>(order_by_expr->children[0])) {
        const auto& vector_compare_expr = order_by_expr->children[0];
        CHECK(vector_compare_expr->children.size() == 3);
        auto field = std::make_unique<FieldRef>(vector_compare_expr->children[0]->string());
        return Node::Create<SortByClause>(
            std::move(field), Node::MustAs<ir::VectorLiteral>(GET_OR_RET(Transform(vector_compare_expr->children[2]))));
      } else {
        auto field = std::make_unique<FieldRef>(order_by_expr->children[0]->string());
        auto order = SortByClause::Order::ASC;
        if (order_by_expr->children.size() == 2 && order_by_expr->children[1]->string_view() == "desc") {
          order = SortByClause::Order::DESC;
        }
        return Node::Create<SortByClause>(order, std::move(field));
      }
    } else if (Is<SearchStmt>(node)) {  // root node
      CHECK(node->children.size() >= 2 && node->children.size() <= 5);

      auto index = Node::MustAs<ir::IndexRef>(GET_OR_RET(Transform(node->children[1])));
      auto select = Node::MustAs<ir::SelectClause>(GET_OR_RET(Transform(node->children[0])));

      std::unique_ptr<ir::QueryExpr> query_expr;
      std::unique_ptr<ir::LimitClause> limit;
      std::unique_ptr<ir::SortByClause> sort_by;

      for (size_t i = 2; i < node->children.size(); ++i) {
        if (Is<WhereClause>(node->children[i])) {
          query_expr = Node::MustAs<ir::QueryExpr>(GET_OR_RET(Transform(node->children[i])));
        } else if (Is<LimitClause>(node->children[i])) {
          limit = Node::MustAs<ir::LimitClause>(GET_OR_RET(Transform(node->children[i])));
        } else if (Is<OrderByClause>(node->children[i])) {
          sort_by = Node::MustAs<ir::SortByClause>(GET_OR_RET(Transform(node->children[i])));
        }
      }

      if (!query_expr) {
        query_expr = std::make_unique<BoolLiteral>(true);
      }

      return Node::Create<ir::SearchExpr>(std::move(index), std::move(query_expr), std::move(limit), std::move(sort_by),
                                          std::move(select));
    } else if (IsRoot(node)) {
      CHECK(node->children.size() == 1);

      return Transform(node->children[0]);
    } else {
      // UNREACHABLE CODE, just for debugging here
      return {Status::NotOK, fmt::format("encountered invalid node type: {}", node->type)};
    }
  }
};

template <typename Input>
StatusOr<std::unique_ptr<ir::Node>> ParseToIR(Input&& in, const ParamMap& param_map = {}) {
  Transformer transformer{param_map};
  return transformer.Transform(GET_OR_RET(ParseToTree(std::forward<Input>(in))));
}

}  // namespace sql

}  // namespace kqir
