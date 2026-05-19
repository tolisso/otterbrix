#include "create_plan_update.hpp"
#include "create_plan_match.hpp"
#include <components/logical_plan/node_limit.hpp>
#include <components/logical_plan/node_update.hpp>
#include <components/physical_plan/operators/operator_match.hpp>
#include <components/physical_plan/operators/operator_update.hpp>
#include <components/physical_plan/operators/operator_update_computing.hpp>
#include <components/physical_plan/operators/scan/full_scan.hpp>
#include <components/physical_plan/operators/scan/scan_computing_table.hpp>

#include "create_plan_data.hpp"

namespace services::planner::impl {

    components::operators::operator_ptr create_plan_update(const context_storage_t& context,
                                                           const components::logical_plan::node_ptr& node) {
        const auto* node_update = static_cast<const components::logical_plan::node_update_t*>(node.get());

        components::logical_plan::node_ptr node_match = nullptr;
        components::logical_plan::node_ptr node_limit = nullptr;
        components::logical_plan::node_ptr node_raw_data = nullptr;
        for (auto child : node_update->children()) {
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

        // Sparse computing-schema UPDATE: build re-alloc resolve pipeline
        //   operator_update_computing
        //     └── operator_match_t (post-filter)
        //          └── scan_computing_table  (full virtual schema + row_ids)
        if (node_update->is_computing_table()) {
            const auto& sides = node_update->computing_sides();
            std::pmr::vector<components::types::complex_logical_type> virtual_types(context.resource);
            std::pmr::vector<collection_full_name_t> side_names(context.resource);
            virtual_types.reserve(sides.size());
            side_names.reserve(sides.size());
            for (const auto& s : sides) {
                virtual_types.push_back(s.field_type);
                side_names.push_back(s.collection);
            }
            // We need every virtual column populated (re-alloc copies the full row),
            // so no projected_cols pruning.
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

            auto upd_op = boost::intrusive_ptr(
                new components::operators::operator_update_computing(context.resource,
                                                                     context.log.clone(),
                                                                     coll_name,
                                                                     node_update->computing_sides(),
                                                                     node_update->updates()));
            upd_op->set_children(std::move(resolve_op));
            return upd_op;
        }

        if (node_update->collection_from().empty() && !node_raw_data) {
            auto plan = boost::intrusive_ptr(new components::operators::operator_update(context.resource,
                                                                                        context.log.clone(),
                                                                                        coll_name,
                                                                                        node_update->updates(),
                                                                                        node_update->upsert()));
            plan->set_children(create_plan_match(context, node_match, limit));

            return plan;
        } else {
            auto plan = boost::intrusive_ptr(new components::operators::operator_update(context.resource,
                                                                                        context.log.clone(),
                                                                                        coll_name,
                                                                                        node_update->updates(),
                                                                                        node_update->upsert(),
                                                                                        node_match->expressions()[0]));
            if (node_raw_data) {
                plan->set_children(boost::intrusive_ptr(new components::operators::full_scan(context.resource,
                                                                                             context.log.clone(),
                                                                                             coll_name,
                                                                                             nullptr,
                                                                                             limit)),
                                   create_plan_data(node_raw_data));
            } else {
                auto coll_from = node_update->collection_from();
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
