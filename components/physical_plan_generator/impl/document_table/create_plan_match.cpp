#include "../create_plan_match.hpp"
#include <components/physical_plan/document_table/operators/scan/full_scan.hpp>
#include <components/physical_plan/document_table/operators/scan/primary_key_scan.hpp>
#include <components/physical_plan/table/operators/operator_match.hpp>
#include <services/collection/collection.hpp>

namespace services::document_table::planner::impl {

    // Helper to check if context uses document_table storage
    inline bool is_document_table_storage(collection::context_collection_t* ctx) {
        return ctx && ctx->storage_type() == collection::storage_type_t::DOCUMENT_TABLE;
    }

    // TODO: Добавить поддержку индексов для оптимизации запросов
    // bool is_can_index_find_by_predicate(components::expressions::compare_type compare) {
    //     using components::expressions::compare_type;
    //     return compare == compare_type::eq || compare == compare_type::ne ||
    //            compare == compare_type::gt || compare == compare_type::lt ||
    //            compare == compare_type::gte || compare == compare_type::lte;
    // }

    // Поддержка быстрого поиска по _id
    bool is_can_primary_key_find_by_predicate(components::expressions::compare_type compare) {
        using components::expressions::compare_type;
        return compare == compare_type::eq;
    }

    components::base::operators::operator_ptr
    create_plan_match_(collection::context_collection_t* context_,
                       const components::expressions::compare_expression_ptr& expr,
                       components::logical_plan::limit_t limit) {
        // Реализация primary_key_scan для быстрого findOne по _id
        if (expr && is_can_primary_key_find_by_predicate(expr->type()) && 
            expr->key_left().as_string() == "_id") {
            // Создаем primary_key_scan оператор с expression
            // Значение _id будет извлечено из expression во время выполнения
            return boost::intrusive_ptr(
                new components::document_table::operators::primary_key_scan(context_, expr));
        }

        // TODO: Реализовать index_scan для быстрого поиска по индексированным полям
        // if (context_) {
        //     if (is_can_index_find_by_predicate(expr->type()) &&
        //         components::index::search_index(context_->index_engine(), {expr->key_left()})) {
        //         return boost::intrusive_ptr(
        //             new components::document_table::operators::index_scan(context_, expr, limit));
        //     }
        // }

        // Для document_table используем full_scan как fallback
        if (is_document_table_storage(context_)) {
            return boost::intrusive_ptr(
                new components::document_table::operators::full_scan(context_, expr, limit));
        } else {
            // Если это не document_table - используем table::operator_match для фильтрации data_chunk
            return boost::intrusive_ptr(
                new components::table::operators::operator_match_t(context_, expr, limit));
        }
    }

    components::base::operators::operator_ptr
    create_plan_match(const context_storage_t& context,
                      const components::logical_plan::node_ptr& node,
                      components::logical_plan::limit_t limit) {
        auto ctx = context.at(node->collection_full_name());

        // Проверяем что коллекция использует document_table storage
        if (!is_document_table_storage(ctx)) {
            throw std::runtime_error(
                "create_plan_match called for non-document_table collection: " +
                node->collection_full_name().to_string());
        }

        if (node->expressions().empty()) {
            // Scan без фильтра
            return boost::intrusive_ptr(
                new components::document_table::operators::full_scan(ctx, nullptr, limit));
        } else {
            auto expr = reinterpret_cast<const components::expressions::compare_expression_ptr*>(
                &node->expressions()[0]);
            return create_plan_match_(ctx, *expr, limit);
        }
    }

} // namespace services::document_table::planner::impl
