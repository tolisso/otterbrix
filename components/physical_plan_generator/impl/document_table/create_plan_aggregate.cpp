#include "create_plan_aggregate.hpp"
#include "create_plan_match.hpp"
#include <components/physical_plan/document_table/operators/aggregation.hpp>
#include <components/physical_plan/document_table/operators/scan/full_scan.hpp>
#include <components/physical_plan_generator/create_plan.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/expressions/aggregate_expression.hpp>
#include <components/expressions/compare_expression.hpp>
#include <services/collection/collection.hpp>
#include <set>

namespace services::document_table::planner::impl {

    using components::logical_plan::node_type;
    using components::expressions::expression_group;

    // Helper to check if context uses document_table storage
    inline bool is_document_table_storage(collection::context_collection_t* ctx) {
        return ctx && ctx->storage_type() == collection::storage_type_t::DOCUMENT_TABLE;
    }

    // Extract columns from compare expression (WHERE clause)
    void extract_columns_from_compare(const components::expressions::compare_expression_ptr& expr,
                                      std::set<std::string>& columns) {
        if (!expr) return;

        // Get the primary key (left side of comparison)
        if (expr->primary_key().is_string()) {
            columns.insert(expr->primary_key().as_string());
        }

        // Recursively process children (for AND/OR expressions)
        for (const auto& child : expr->children()) {
            if (child->group() == expression_group::compare) {
                auto child_expr = static_cast<components::expressions::compare_expression_t*>(child.get());
                extract_columns_from_compare(
                    components::expressions::compare_expression_ptr(child_expr), columns);
            }
        }
    }

    // Extract column names from expressions (GROUP BY keys, aggregate function arguments, and WHERE)
    std::vector<std::string> extract_projection_columns(const components::logical_plan::node_ptr& node) {
        std::set<std::string> columns;

        for (const auto& child : node->children()) {
            if (child->type() == node_type::group_t) {
                // Extract columns from GROUP BY expressions
                for (const auto& expr : child->expressions()) {
                    if (expr->group() == expression_group::scalar) {
                        auto scalar = static_cast<components::expressions::scalar_expression_t*>(expr.get());

                        // For scalar expressions like "$commit_dot_collection AS event"
                        // The key is the alias, but params[0] contains the actual column name
                        if (!scalar->params().empty()) {
                            const auto& param = scalar->params()[0];
                            if (std::holds_alternative<components::expressions::key_t>(param)) {
                                const auto& key = std::get<components::expressions::key_t>(param);
                                if (key.is_string()) {
                                    columns.insert(key.as_string());
                                }
                            }
                        } else if (scalar->key().is_string()) {
                            columns.insert(scalar->key().as_string());
                        }
                    } else if (expr->group() == expression_group::aggregate) {
                        // For aggregate functions like COUNT(column), extract the column
                        auto agg = static_cast<components::expressions::aggregate_expression_t*>(expr.get());
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
                // Extract columns from WHERE clause
                if (!child->expressions().empty()) {
                    auto expr = reinterpret_cast<const components::expressions::compare_expression_ptr*>(
                        &child->expressions()[0]);
                    extract_columns_from_compare(*expr, columns);
                }
            }
        }

        std::vector<std::string> result(columns.begin(), columns.end());
        std::cout << "[DEBUG PLANNER] extract_projection_columns: found " << result.size() << " columns: ";
        for (const auto& col : result) {
            std::cout << col << " ";
        }
        std::cout << std::endl;

        return result;
    }

    components::base::operators::operator_ptr
    create_plan_aggregate(const context_storage_t& context,
                          const components::logical_plan::node_ptr& node,
                          components::logical_plan::limit_t limit) {
        std::cout << "[DEBUG PLANNER] create_plan_aggregate for document_table" << std::endl << std::flush;

        auto ctx = context.at(node->collection_full_name());

        // Проверяем что коллекция использует document_table storage
        if (!is_document_table_storage(ctx)) {
            std::cout << "[DEBUG PLANNER] ERROR: not document_table storage!" << std::endl << std::flush;
            throw std::runtime_error(
                "create_plan_aggregate called for non-document_table collection: " +
                node->collection_full_name().to_string());
        }

        std::cout << "[DEBUG PLANNER] Creating aggregation operator" << std::endl << std::flush;

        // Для document_table используем специализированный aggregation
        // который автоматически создаёт full_scan вместо transfer_scan
        auto op = boost::intrusive_ptr(new components::document_table::operators::aggregation(ctx));

        // Extract projection columns from GROUP BY, aggregate expressions, and WHERE
        auto projection_columns = extract_projection_columns(node);
        if (!projection_columns.empty()) {
            op->set_projection(projection_columns);
        }

        for (const components::logical_plan::node_ptr& child : node->children()) {
            switch (child->type()) {
                case node_type::match_t:
                    // Используем наш create_plan_match для document_table с projection
                    op->set_match(create_plan_match(context, child, limit, projection_columns));
                    break;
                case node_type::group_t:
                    // GROUP BY работает с data_chunk - используем table planner
                    op->set_group(services::table::planner::create_plan(context, child, limit));
                    break;
                case node_type::sort_t:
                    // ORDER BY работает с data_chunk - используем table planner
                    op->set_sort(services::table::planner::create_plan(context, child, limit));
                    break;
                default:
                    // Для остальных операций используем table planner
                    op->set_children(services::table::planner::create_plan(context, child, limit));
                    break;
            }
        }

        return op;
    }

} // namespace services::document_table::planner::impl
