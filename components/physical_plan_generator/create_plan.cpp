#include "create_plan.hpp"

#include <components/logical_plan/node_data.hpp>
#include "impl/create_plan_add_index.hpp"
#include "impl/create_plan_aggregate.hpp"
#include "impl/create_plan_data.hpp"
#include "impl/create_plan_delete.hpp"
#include "impl/create_plan_drop_index.hpp"
#include "impl/create_plan_group.hpp"
#include "impl/create_plan_insert.hpp"
#include "impl/create_plan_join.hpp"
#include "impl/create_plan_match.hpp"
#include "impl/create_plan_sort.hpp"
#include "impl/create_plan_update.hpp"

// document_table specialized planners
#include "impl/document_table/create_plan_aggregate.hpp"
#include "impl/document_table/create_plan_match.hpp"
#include "impl/document_table/create_plan_insert.hpp"
#include "impl/document_table/create_plan_delete.hpp"
#include "impl/document_table/create_plan_update.hpp"

namespace services::collection::planner {

    using components::logical_plan::node_type;

    components::base::operators::operator_ptr create_plan(const context_storage_t& context,
                                                          const components::logical_plan::node_ptr& node,
                                                          components::logical_plan::limit_t limit) {
        switch (node->type()) {
            case node_type::aggregate_t:
                return impl::create_plan_aggregate(context, node, std::move(limit));
            case node_type::data_t:
                return impl::create_plan_data(node);
            case node_type::delete_t:
                return impl::create_plan_delete(context, node);
            case node_type::insert_t:
                return impl::create_plan_insert(context, node, std::move(limit));
            case node_type::match_t:
                return impl::create_plan_match(context, node, std::move(limit));
            case node_type::group_t:
                return impl::create_plan_group(context, node);
            case node_type::sort_t:
                return impl::create_plan_sort(context, node);
            case node_type::update_t:
                return impl::create_plan_update(context, node);
            case node_type::join_t:
                return impl::create_plan_join(context, node, std::move(limit));
            case node_type::create_index_t:
                return impl::create_plan_add_index(context, node);
            case node_type::drop_index_t:
                return impl::create_plan_drop_index(context, node);
            default:
                break;
        }
        return nullptr;
    }

} // namespace services::collection::planner

namespace services::table::planner {

    using components::logical_plan::node_type;

    components::base::operators::operator_ptr create_plan(const context_storage_t& context,
                                                          const components::logical_plan::node_ptr& node,
                                                          components::logical_plan::limit_t limit) {
        switch (node->type()) {
            case node_type::aggregate_t:
                return impl::create_plan_aggregate(context, node, std::move(limit));
            case node_type::data_t:
                return impl::create_plan_data(node);
            case node_type::delete_t:
                return impl::create_plan_delete(context, node);
            case node_type::insert_t:
                return impl::create_plan_insert(context, node, std::move(limit));
            case node_type::match_t:
                return impl::create_plan_match(context, node, std::move(limit));
            case node_type::group_t:
                return impl::create_plan_group(context, node);
            case node_type::sort_t:
                return impl::create_plan_sort(context, node);
            case node_type::update_t:
                return impl::create_plan_update(context, node);
            case node_type::join_t:
                return impl::create_plan_join(context, node, std::move(limit));
            case node_type::create_index_t:
                return impl::create_plan_add_index(context, node);
            case node_type::drop_index_t:
                return impl::create_plan_drop_index(context, node);
            default:
                break;
        }
        return nullptr;
    }

} // namespace services::table::planner

namespace services::document_table::planner {

    using components::logical_plan::node_type;

    // Специализированный планировщик для document_table
    // Использует колоночное хранилище с динамической схемой
    //
    // TODO: Оптимизация производительности запросов
    // 1. Реализовать primary_key_scan для быстрого findOne({_id: "..."}) - O(1) вместо O(N)
    // 2. Реализовать index_scan для поиска по индексированным полям - O(log N) вместо O(N)
    //
    // Текущее состояние:
    // - INSERT/UPDATE/DELETE используют document_table::operators::* с поддержкой schema evolution
    // - SELECT использует full_scan (O(N)) - индексы не поддерживаются
    // - GROUP BY/ORDER BY/JOIN используют table::operators::* (переиспользуются напрямую)
    components::base::operators::operator_ptr create_plan(const context_storage_t& context,
                                                          const components::logical_plan::node_ptr& node,
                                                          components::logical_plan::limit_t limit) {
        switch (node->type()) {
            case node_type::aggregate_t:
                // Используем специализированный планировщик для document_table
                return impl::create_plan_aggregate(context, node, std::move(limit));
            case node_type::data_t: {
                // Check if node contains data_chunk (from SQL) or documents (from API)
                const auto* data = static_cast<const components::logical_plan::node_data_t*>(node.get());
                if (data->uses_data_chunk()) {
                    return table::planner::impl::create_plan_data(node);
                }
                return collection::planner::impl::create_plan_data(node);
            }
            case node_type::delete_t:
                // Используем специализированный планировщик для document_table
                return impl::create_plan_delete(context, node);
            case node_type::insert_t:
                // Используем специализированный планировщик для document_table
                return impl::create_plan_insert(context, node, std::move(limit));
            case node_type::match_t:
                // Используем специализированный планировщик для document_table
                return impl::create_plan_match(context, node, std::move(limit));
            case node_type::group_t:
                // GROUP BY работает с data_chunk - используем table planner
                return table::planner::impl::create_plan_group(context, node);
            case node_type::sort_t:
                // ORDER BY работает с data_chunk - используем table planner
                return table::planner::impl::create_plan_sort(context, node);
            case node_type::update_t:
                // Используем специализированный планировщик для document_table
                return impl::create_plan_update(context, node);
            case node_type::join_t:
                // JOIN работает с data_chunk - используем table planner
                return table::planner::impl::create_plan_join(context, node, std::move(limit));
            case node_type::create_index_t:
                return collection::planner::impl::create_plan_add_index(context, node);
            case node_type::drop_index_t:
                return collection::planner::impl::create_plan_drop_index(context, node);
            default:
                break;
        }
        return nullptr;
    }

} // namespace services::document_table::planner
