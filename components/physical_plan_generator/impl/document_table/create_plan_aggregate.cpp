#include "create_plan_aggregate.hpp"
#include "create_plan_match.hpp"
#include <components/physical_plan/document_table/operators/aggregation.hpp>
#include <components/physical_plan/document_table/operators/scan/full_scan.hpp>
#include <components/physical_plan_generator/create_plan.hpp>
#include <services/collection/collection.hpp>

namespace services::document_table::planner::impl {

    using components::logical_plan::node_type;

    // Helper to check if context uses document_table storage
    inline bool is_document_table_storage(collection::context_collection_t* ctx) {
        return ctx && ctx->storage_type() == collection::storage_type_t::DOCUMENT_TABLE;
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

        for (const components::logical_plan::node_ptr& child : node->children()) {
            switch (child->type()) {
                case node_type::match_t:
                    // Используем наш create_plan_match для document_table
                    op->set_match(create_plan_match(context, child, limit));
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
