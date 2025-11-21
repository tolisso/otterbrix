#include "../create_plan_match.hpp"
#include <components/physical_plan/document_table/operators/scan/full_scan.hpp>
#include <components/physical_plan/table/operators/operator_match.hpp>
#include <services/collection/collection.hpp>

namespace services::document_table::planner::impl {

    // TODO: Добавить поддержку индексов для оптимизации запросов
    // bool is_can_index_find_by_predicate(components::expressions::compare_type compare) {
    //     using components::expressions::compare_type;
    //     return compare == compare_type::eq || compare == compare_type::ne ||
    //            compare == compare_type::gt || compare == compare_type::lt ||
    //            compare == compare_type::gte || compare == compare_type::lte;
    // }

    // TODO: Добавить поддержку быстрого поиска по _id
    // bool is_can_primary_key_find_by_predicate(components::expressions::compare_type compare) {
    //     using components::expressions::compare_type;
    //     return compare == compare_type::eq;
    // }

    components::base::operators::operator_ptr
    create_plan_match_(collection::context_collection_t* context_,
                       const components::expressions::compare_expression_ptr& expr,
                       components::logical_plan::limit_t limit) {
        // TODO: Реализовать primary_key_scan для быстрого findOne по _id
        // if (is_can_primary_key_find_by_predicate(expr->type()) && expr->key().as_string() == "_id") {
        //     return boost::intrusive_ptr(
        //         new components::document_table::operators::primary_key_scan(context_));
        // }

        // TODO: Реализовать index_scan для быстрого поиска по индексированным полям
        // if (context_) {
        //     if (is_can_index_find_by_predicate(expr->type()) &&
        //         components::index::search_index(context_->index_engine(), {expr->key_left()})) {
        //         return boost::intrusive_ptr(
        //             new components::document_table::operators::index_scan(context_, expr, limit));
        //     }
        // }

        // Для document_table всегда используем full_scan
        // Пока индексы не поддерживаются - все запросы делают полное сканирование (O(N))
        if (context_) {
            return boost::intrusive_ptr(
                new components::document_table::operators::full_scan(context_, expr, limit));
        } else {
            // Если нет контекста - используем table::operator_match для фильтрации data_chunk
            return boost::intrusive_ptr(
                new components::table::operators::operator_match_t(context_, expr, limit));
        }
    }

    components::base::operators::operator_ptr
    create_plan_match(const context_storage_t& context,
                      const components::logical_plan::node_ptr& node,
                      components::logical_plan::limit_t limit) {
        if (node->expressions().empty()) {
            // Scan без фильтра
            return boost::intrusive_ptr(
                new components::document_table::operators::full_scan(
                    context.at(node->collection_full_name()), nullptr, limit));
        } else {
            auto expr = reinterpret_cast<const components::expressions::compare_expression_ptr*>(
                &node->expressions()[0]);
            return create_plan_match_(context.at(node->collection_full_name()), *expr, limit);
        }
    }

} // namespace services::document_table::planner::impl
