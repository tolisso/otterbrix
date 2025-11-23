#include "../create_plan_insert.hpp"
#include <components/physical_plan/document_table/operators/operator_insert.hpp>
#include <components/physical_plan_generator/create_plan.hpp>
#include <services/collection/collection.hpp>

namespace services::document_table::planner::impl {

    // Helper to check if context uses document_table storage
    inline bool is_document_table_storage(collection::context_collection_t* ctx) {
        return ctx && ctx->storage_type() == collection::storage_type_t::DOCUMENT_TABLE;
    }

    components::base::operators::operator_ptr
    create_plan_insert(const context_storage_t& context,
                       const components::logical_plan::node_ptr& node,
                       components::logical_plan::limit_t limit) {
        // Проверяем что коллекция использует document_table storage
        auto ctx = context.at(node->collection_full_name());
        if (!is_document_table_storage(ctx)) {
            throw std::runtime_error(
                "create_plan_insert called for non-document_table collection: " +
                node->collection_full_name().to_string());
        }

        // Для document_table используем наш оператор вставки с поддержкой schema evolution
        auto plan = boost::intrusive_ptr(
            new components::document_table::operators::operator_insert(ctx));

        // Подключаем дочерний план (обычно operator_raw_data с документами)
        plan->set_children(services::document_table::planner::create_plan(
            context, node->children().front(), std::move(limit)));

        return plan;
    }

} // namespace services::document_table::planner::impl
