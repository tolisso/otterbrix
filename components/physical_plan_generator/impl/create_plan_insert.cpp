#include "create_plan_insert.hpp"

#include <components/logical_plan/node_insert.hpp>
#include <components/physical_plan/operators/operator_insert.hpp>
#include <components/physical_plan/operators/operator_insert_computing.hpp>
#include <components/physical_plan_generator/create_plan.hpp>

namespace services::planner::impl {

    components::operators::operator_ptr
    create_plan_insert(const context_storage_t& context,
                       const components::compute::function_registry_t& function_registry,
                       const components::logical_plan::node_ptr& node,
                       components::logical_plan::limit_t limit,
                       const components::logical_plan::storage_parameters* params) {
        const auto* ins = static_cast<const components::logical_plan::node_insert_t*>(node.get());

        components::operators::operator_ptr plan;
        if (ins->is_computing_table()) {
            // Sparse computing-schema INSERT — fan-out to N+1 storages happens
            // inside operator_insert_computing + intercept_dml_io_.
            plan = boost::intrusive_ptr(
                new components::operators::operator_insert_computing(context.resource,
                                                                     context.log.clone(),
                                                                     node->collection_full_name(),
                                                                     ins->computing_sides()));
        } else {
            // TODO: figure out key translation
            plan = boost::intrusive_ptr(
                new components::operators::operator_insert(context.resource,
                                                           context.log.clone(),
                                                           node->collection_full_name()));
        }
        plan->set_children(create_plan(context, function_registry, node->children().front(), std::move(limit), params));

        return plan;
    }

} // namespace services::planner::impl