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

#include <memory>

#include "common_transformer.h"
#include "ir.h"
#include "parse_util.h"
#include "redis_query_parser.h"
#include "search/common_parser.h"

namespace kqir {

namespace redis_query {

namespace ir = kqir;

template <typename Rule>
using TreeSelector =
    parse_tree::selector<Rule, parse_tree::store_content::on<Number, UnsignedInteger, StringL, Param, Identifier, Inf>,
                         parse_tree::remove_content::on<
                             TagList, NumericRange, TextContains, VectorRange, ExclusiveNumber, FieldQuery, NotExpr,
                             AndExpr, OrExpr, PrefilterExpr, KnnSearch, Wildcard, VectorRangeToken, KnnToken, ArrowOp>>;

template <typename Input>
StatusOr<std::unique_ptr<parse_tree::node>> ParseToTree(Input&& in) {
  if (auto root = parse_tree::parse<seq<QueryExpr, eof>, TreeSelector>(std::forward<Input>(in))) {
    return root;
  } else {
    // TODO: improve parse error message, with source location
    return {Status::NotOK, "invalid syntax"};
  }
}

struct Transformer : ir::TreeTransformer {
  explicit Transformer(const ParamMap& param_map) : TreeTransformer(param_map) {}

  StatusOr<std::unique_ptr<VectorLiteral>> Transform2Vector(const TreeNode& node) {
    std::string vector_str = GET_OR_RET(GetParam(node));

    std::vector<double> values = GET_OR_RET(Binary2Vector<double>(vector_str));
    if (values.empty()) {
      return {Status::NotOK, "empty vector is invalid"};
    }
    return std::make_unique<ir::VectorLiteral>(std::move(values));
  };

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

    auto ident_string_or_param = [this](const TreeNode& node) -> StatusOr<std::string> {
      if (Is<Identifier>(node)) {
        return node->string();
      } else if (Is<StringL>(node)) {
        return GET_OR_RET(UnescapeString(node->string()));
      } else if (Is<Param>(node)) {
        return GET_OR_RET(GetParam(node));
      } else {
        return {Status::NotOK, "encountered invalid identifier, string or parameter"};
      }
    };

    if (Is<Number>(node)) {
      return Node::Create<ir::NumericLiteral>(*ParseFloat(node->string()));
    } else if (Is<Wildcard>(node)) {
      return Node::Create<ir::BoolLiteral>(true);
    } else if (Is<FieldQuery>(node)) {
      CHECK(node->children.size() == 2);

      auto field = node->children[0]->string();
      const auto& query = node->children[1];

      if (Is<TagList>(query)) {
        std::vector<std::unique_ptr<ir::QueryExpr>> exprs;

        for (const auto& tag : query->children) {
          auto tag_str = GET_OR_RET(ident_string_or_param(tag));

          exprs.push_back(std::make_unique<ir::TagContainExpr>(std::make_unique<FieldRef>(field),
                                                               std::make_unique<StringLiteral>(tag_str)));
        }

        if (exprs.size() == 1) {
          return std::move(exprs[0]);
        } else {
          return std::make_unique<ir::OrExpr>(std::move(exprs));
        }
      } else if (Is<NumericRange>(query)) {
        std::vector<std::unique_ptr<ir::QueryExpr>> exprs;

        const auto& lhs = query->children[0];
        const auto& rhs = query->children[1];

        if (Is<ExclusiveNumber>(lhs)) {
          exprs.push_back(std::make_unique<NumericCompareExpr>(NumericCompareExpr::GT,
                                                               std::make_unique<FieldRef>(field),
                                                               GET_OR_RET(number_or_param(lhs->children[0]))));
        } else if (Is<Number>(lhs) || Is<Param>(lhs)) {
          exprs.push_back(std::make_unique<NumericCompareExpr>(
              NumericCompareExpr::GET, std::make_unique<FieldRef>(field), GET_OR_RET(number_or_param(lhs))));
        } else {  // Inf
          if (lhs->string_view() == "+inf") {
            return {Status::NotOK, "it's not allowed to set the lower bound as positive infinity"};
          }
        }

        if (Is<ExclusiveNumber>(rhs)) {
          exprs.push_back(std::make_unique<NumericCompareExpr>(NumericCompareExpr::LT,
                                                               std::make_unique<FieldRef>(field),
                                                               GET_OR_RET(number_or_param(rhs->children[0]))));
        } else if (Is<Number>(rhs) || Is<Param>(rhs)) {
          exprs.push_back(std::make_unique<NumericCompareExpr>(
              NumericCompareExpr::LET, std::make_unique<FieldRef>(field), GET_OR_RET(number_or_param(rhs))));
        } else {  // Inf
          if (rhs->string_view() == "-inf") {
            return {Status::NotOK, "it's not allowed to set the upper bound as negative infinity"};
          }
        }

        if (exprs.empty()) {
          return std::make_unique<BoolLiteral>(true);
        } else if (exprs.size() == 1) {
          return std::move(exprs[0]);
        } else {
          return std::make_unique<ir::AndExpr>(std::move(exprs));
        }
      } else if (Is<VectorRange>(query)) {
        return std::make_unique<VectorRangeExpr>(std::make_unique<FieldRef>(field),
                                                 GET_OR_RET(number_or_param(query->children[1])),
                                                 GET_OR_RET(Transform2Vector(query->children[2])));
      } else if (Is<TextContains>(query)) {
        auto text = GET_OR_RET(ident_string_or_param(query->children[1]));
        return std::make_unique<TextContainExpr>(std::make_unique<FieldRef>(field),
                                                 std::make_unique<StringLiteral>(text));
      }
    } else if (Is<NotExpr>(node)) {
      CHECK(node->children.size() == 1);

      return Node::Create<ir::NotExpr>(Node::MustAs<ir::QueryExpr>(GET_OR_RET(Transform(node->children[0]))));
    } else if (Is<PrefilterExpr>(node)) {
      // TODO: allow hybrid query
      CHECK(node->children.size() == 3);

      const auto& knn_search = node->children[2];
      CHECK(knn_search->children.size() == 4);

      size_t k = 0;
      if (Is<UnsignedInteger>(knn_search->children[1])) {
        k = *ParseInt(knn_search->children[1]->string());
      } else {
        k = *ParseInt(GET_OR_RET(GetParam(knn_search->children[1])));
      }

      return std::make_unique<VectorKnnExpr>(std::make_unique<FieldRef>(knn_search->children[2]->string()),
                                             GET_OR_RET(Transform2Vector(knn_search->children[3])), k);
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
    } else if (IsRoot(node)) {
      CHECK(node->children.size() == 1);

      return Transform(node->children[0]);
    } else {
      // UNREACHABLE CODE, just for debugging here
      return {Status::NotOK, fmt::format("encountered invalid node type: {}", node->type)};
    }
  }  // NOLINT
};

template <typename Input>
StatusOr<std::unique_ptr<ir::Node>> ParseToIR(Input&& in, const ParamMap& param_map = {}) {
  Transformer transformer(param_map);
  return transformer.Transform(GET_OR_RET(ParseToTree(std::forward<Input>(in))));
}

}  // namespace redis_query

}  // namespace kqir
