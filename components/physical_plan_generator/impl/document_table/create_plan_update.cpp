#include "../create_plan_update.hpp"
#include "create_plan_match.hpp"
#include <components/logical_plan/node_limit.hpp>
#include <components/logical_plan/node_update.hpp>
#include <components/physical_plan/document_table/operators/operator_update.hpp>
#include <components/physical_plan/document_table/operators/scan/full_scan.hpp>
#include <components/physical_plan_generator/impl/create_plan_data.hpp>

namespace services::document_table::planner::impl {

    // Helper to check if context uses document_table storage
    inline bool is_document_table_storage(collection::context_collection_t* ctx) {
        return ctx && ctx->storage_type() == collection::storage_type_t::DOCUMENT_TABLE;
    }

    components::base::operators::operator_ptr
    create_plan_update(const context_storage_t& context,
                       const components::logical_plan::node_ptr& node) {
        const auto* node_update = static_cast<const components::logical_plan::node_update_t*>(node.get());

        // Проверяем что основная коллекция использует document_table storage
        auto ctx = context.at(node->collection_full_name());
        if (!is_document_table_storage(ctx)) {
            throw std::runtime_error(
                "create_plan_update called for non-document_table collection: " +
                node->collection_full_name().to_string());
        }

        // Ищем дочерние узлы
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

        auto limit = node_limit
            ? static_cast<components::logical_plan::node_limit_t*>(node_limit.get())->limit()
            : components::logical_plan::limit_t::unlimit();

        // Простой случай: update с match (без JOIN)
        if (node_update->collection_from().empty() && !node_raw_data) {
            auto plan = boost::intrusive_ptr(
                new components::document_table::operators::operator_update(
                    context.at(node->collection_full_name()),
                    node_update->updates(),
                    node_update->upsert(),
                    nullptr));

            // Подключаем scan с фильтром
            plan->set_children(create_plan_match(context, node_match, limit));

            return plan;
        } else {
            // Сложный случай: update с JOIN или raw data
            components::expressions::compare_expression_ptr expr = nullptr;
            if (node_match && !node_match->expressions().empty()) {
                expr = *reinterpret_cast<const components::expressions::compare_expression_ptr*>(
                    &node_match->expressions()[0]);
            }

            auto plan = boost::intrusive_ptr(
                new components::document_table::operators::operator_update(
                    context.at(node->collection_full_name()),
                    node_update->updates(),
                    node_update->upsert(),
                    std::move(expr)));

            if (node_raw_data) {
                // UPDATE с raw data
                plan->set_children(
                    boost::intrusive_ptr(new components::document_table::operators::full_scan(
                        context.at(node->collection_full_name()), nullptr, limit)),
                    services::collection::planner::impl::create_plan_data(node_raw_data));
            } else {
                // UPDATE с JOIN
                auto join_ctx = context.at(node_update->collection_from());

                // Для JOIN второго ребёнка создаём через правильный планировщик
                boost::intrusive_ptr<components::base::operators::read_only_operator_t> join_child;
                if (is_document_table_storage(join_ctx)) {
                    join_child = boost::intrusive_ptr(
                        new components::document_table::operators::full_scan(join_ctx, nullptr, limit));
                } else {
                    // Вторая коллекция - не document_table, используем match планировщик
                    // TODO: нужно создать правильный node или использовать table::planner
                    throw std::runtime_error(
                        "UPDATE JOIN with non-document_table collection not yet supported");
                }

                plan->set_children(
                    boost::intrusive_ptr(new components::document_table::operators::full_scan(
                        context.at(node->collection_full_name()), nullptr, limit)),
                    join_child);
            }

            return plan;
        }
    }

} // namespace services::document_table::planner::impl
