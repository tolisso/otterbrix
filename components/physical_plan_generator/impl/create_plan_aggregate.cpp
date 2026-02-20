#include "create_plan_aggregate.hpp"
#include "create_plan_match.hpp"

#include <components/expressions/aggregate_expression.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/physical_plan/collection/operators/aggregation.hpp>
#include <components/physical_plan/table/operators/aggregation.hpp>
#include <components/physical_plan_generator/create_plan.hpp>
#include <set>
#include <string>

namespace services::collection::planner::impl {

    using components::logical_plan::node_type;

    components::collection::operators::operator_ptr
    create_plan_aggregate(const context_storage_t& context,
                          const components::logical_plan::node_ptr& node,
                          components::logical_plan::limit_t limit) {
        auto op = boost::intrusive_ptr(
            new components::collection::operators::aggregation(context.at(node->collection_full_name())));
        for (const components::logical_plan::node_ptr& child : node->children()) {
            switch (child->type()) {
                case node_type::match_t:
                    op->set_match(create_plan(context, child, limit));
                    break;
                case node_type::group_t:
                    op->set_group(create_plan(context, child, limit));
                    break;
                case node_type::sort_t:
                    op->set_sort(create_plan(context, child, limit));
                    break;
                default:
                    op->set_children(create_plan(context, child, limit));
                    break;
            }
        }
        return op;
    }

} // namespace services::collection::planner::impl

namespace services::table::planner::impl {

    using components::logical_plan::node_type;
    using components::expressions::expression_group;

    namespace {
        // Collect column names referenced in a compare expression (for WHERE projection)
        void collect_compare_columns(const components::expressions::compare_expression_ptr& expr,
                                     std::set<std::string>& columns) {
            if (!expr) return;
            if (expr->primary_key().is_string()) {
                columns.insert(expr->primary_key().as_string());
            }
            for (const auto& child : expr->children()) {
                if (child->group() == expression_group::compare) {
                    auto child_expr = static_cast<components::expressions::compare_expression_t*>(child.get());
                    collect_compare_columns(
                        components::expressions::compare_expression_ptr(child_expr), columns);
                }
            }
        }

        // Extract projection columns from aggregate node's GROUP BY and WHERE children
        std::vector<std::string> extract_projection(const components::logical_plan::node_ptr& node) {
            std::set<std::string> columns;
            for (const auto& child : node->children()) {
                if (child->type() == node_type::group_t) {
                    for (const auto& expr : child->expressions()) {
                        if (expr->group() == expression_group::scalar) {
                            auto scalar = static_cast<const components::expressions::scalar_expression_t*>(expr.get());
                            // Actual column is in params[0], alias is in key
                            if (!scalar->params().empty()) {
                                const auto& param = scalar->params()[0];
                                if (std::holds_alternative<components::expressions::key_t>(param)) {
                                    const auto& key = std::get<components::expressions::key_t>(param);
                                    if (key.is_string()) columns.insert(key.as_string());
                                }
                            } else if (scalar->key().is_string()) {
                                columns.insert(scalar->key().as_string());
                            }
                        } else if (expr->group() == expression_group::aggregate) {
                            auto agg = static_cast<const components::expressions::aggregate_expression_t*>(expr.get());
                            for (const auto& param : agg->params()) {
                                if (std::holds_alternative<components::expressions::key_t>(param)) {
                                    const auto& key = std::get<components::expressions::key_t>(param);
                                    if (key.is_string() && !key.as_string().empty() && key.as_string() != "*") {
                                        columns.insert(key.as_string());
                                    }
                                }
                            }
                        }
                    }
                } else if (child->type() == node_type::match_t) {
                    if (!child->expressions().empty()) {
                        auto expr = reinterpret_cast<const components::expressions::compare_expression_ptr*>(
                            &child->expressions()[0]);
                        collect_compare_columns(*expr, columns);
                    }
                }
            }
            return {columns.begin(), columns.end()};
        }
    } // anonymous namespace

    components::base::operators::operator_ptr create_plan_aggregate(const context_storage_t& context,
                                                                    const components::logical_plan::node_ptr& node,
                                                                    components::logical_plan::limit_t limit) {
        auto op = boost::intrusive_ptr(
            new components::table::operators::aggregation(context.at(node->collection_full_name())));

        auto projection = extract_projection(node);

        for (const components::logical_plan::node_ptr& child : node->children()) {
            switch (child->type()) {
                case node_type::match_t:
                    // Pass projection so full_scan reads only needed columns
                    op->set_match(create_plan_match(context, child, limit, projection));
                    break;
                case node_type::group_t:
                    op->set_group(create_plan(context, child, limit));
                    break;
                case node_type::sort_t:
                    op->set_sort(create_plan(context, child, limit));
                    break;
                default:
                    op->set_children(create_plan(context, child, limit));
                    break;
            }
        }
        return op;
    }

} // namespace services::table::planner::impl
