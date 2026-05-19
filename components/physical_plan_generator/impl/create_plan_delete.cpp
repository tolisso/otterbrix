#include "create_plan_delete.hpp"
#include "create_plan_match.hpp"
#include <components/expressions/compare_expression.hpp>
#include <components/logical_plan/node_delete.hpp>
#include <components/logical_plan/node_limit.hpp>
#include <components/physical_plan/operators/operator_delete.hpp>
#include <components/physical_plan/operators/operator_delete_computing.hpp>
#include <components/physical_plan/operators/operator_match.hpp>
#include <components/physical_plan/operators/scan/full_scan.hpp>
#include <components/physical_plan/operators/scan/scan_computing_table.hpp>

#include "create_plan_data.hpp"

namespace services::planner::impl {

    components::operators::operator_ptr create_plan_delete(const context_storage_t& context,
                                                           const components::logical_plan::node_ptr& node) {
        const auto* node_delete = static_cast<const components::logical_plan::node_delete_t*>(node.get());

        components::logical_plan::node_ptr node_match = nullptr;
        components::logical_plan::node_ptr node_limit = nullptr;
        components::logical_plan::node_ptr node_raw_data = nullptr;
        for (auto child : node_delete->children()) {
            if (child->type() == components::logical_plan::node_type::match_t) {
                node_match = child;
            } else if (child->type() == components::logical_plan::node_type::limit_t) {
                node_limit = child;
            } else if (child->type() == components::logical_plan::node_type::data_t) {
                node_raw_data = child;
            }
        }
        auto limit = static_cast<components::logical_plan::node_limit_t*>(node_limit.get())->limit();
        auto coll_name = node->collection_full_name();

        // Sparse computing-schema DELETE: build resolve pipeline
        //   operator_delete_computing
        //     └── operator_match_t (post-filter, only if WHERE present)
        //          └── scan_computing_table
        // intercept_dml_io_::case remove_computing drives the multi-collection
        // storage_delete_rows + WAL + commit under one txn_id.
        if (node_delete->is_computing_table()) {
            const auto& sides = node_delete->computing_sides();
            std::pmr::vector<components::types::complex_logical_type> virtual_types(context.resource);
            std::pmr::vector<collection_full_name_t> side_names(context.resource);
            virtual_types.reserve(sides.size());
            side_names.reserve(sides.size());
            for (const auto& s : sides) {
                virtual_types.push_back(s.field_type);
                side_names.push_back(s.collection);
            }
            // No column_pruning here (we need to surface row_id; scan_computing_table
            // populates chunk.row_ids regardless of projected_cols).
            std::vector<size_t> projected_cols;
            auto scan = boost::intrusive_ptr(
                new components::operators::scan_computing_table(context.resource,
                                                                coll_name,
                                                                std::move(virtual_types),
                                                                std::move(side_names),
                                                                projected_cols,
                                                                components::logical_plan::limit_t::unlimit()));

            components::operators::operator_ptr resolve_op = std::move(scan);
            if (node_match && !node_match->expressions().empty()) {
                auto post_match = boost::intrusive_ptr(
                    new components::operators::operator_match_t(context.resource,
                                                                context.log.clone(),
                                                                node_match->expressions()[0],
                                                                limit));
                post_match->set_children(std::move(resolve_op));
                resolve_op = std::move(post_match);
            }

            auto del_op = boost::intrusive_ptr(
                new components::operators::operator_delete_computing(context.resource,
                                                                     context.log.clone(),
                                                                     coll_name,
                                                                     node_delete->computing_sides()));
            del_op->set_children(std::move(resolve_op));
            return del_op;
        }

        if (node_delete->collection_from().empty() && !node_raw_data) {
            auto plan = boost::intrusive_ptr(
                new components::operators::operator_delete(context.resource, context.log.clone(), coll_name));
            plan->set_children(create_plan_match(context, node_match, limit));

            return plan;
        } else {
            auto expr =
                reinterpret_cast<const components::expressions::compare_expression_ptr*>(&node_match->expressions()[0]);

            auto plan = boost::intrusive_ptr(
                new components::operators::operator_delete(context.resource, context.log.clone(), coll_name, *expr));
            if (node_raw_data) {
                plan->set_children(boost::intrusive_ptr(new components::operators::full_scan(context.resource,
                                                                                             context.log.clone(),
                                                                                             coll_name,
                                                                                             nullptr,
                                                                                             limit)),
                                   create_plan_data(node_raw_data));
            } else {
                auto coll_from = node_delete->collection_from();
                plan->set_children(boost::intrusive_ptr(new components::operators::full_scan(context.resource,
                                                                                             context.log.clone(),
                                                                                             coll_name,
                                                                                             nullptr,
                                                                                             limit)),
                                   boost::intrusive_ptr(new components::operators::full_scan(context.resource,
                                                                                             context.log.clone(),
                                                                                             coll_from,
                                                                                             nullptr,
                                                                                             limit)));
            }

            return plan;
        }
    }

} // namespace services::planner::impl
