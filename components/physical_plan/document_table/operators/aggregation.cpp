#include "aggregation.hpp"
#include <components/physical_plan/document_table/operators/scan/full_scan.hpp>

namespace components::document_table::operators {

    aggregation::aggregation(services::collection::context_collection_t* context)
        : read_only_operator_t(context, operator_type::aggregate) {
        std::cout << "[DEBUG] aggregation::aggregation - constructor" << std::endl << std::flush;
    }

    void aggregation::set_match(operator_ptr&& match) {
        std::cout << "[DEBUG] aggregation::set_match" << std::endl;
        match_ = std::move(match);
    }

    void aggregation::set_group(operator_ptr&& group) { group_ = std::move(group); }

    void aggregation::set_sort(operator_ptr&& sort) { sort_ = std::move(sort); }

    void aggregation::on_execute_impl(pipeline::context_t*) {
        std::cout << "[DEBUG] aggregation::on_execute_impl" << std::endl;
        // Берём output от последнего оператора в цепочке
        take_output(left_);
    }

    void aggregation::on_prepare_impl() {
        // Строим цепочку операторов: match -> group -> sort
        operator_ptr executor = nullptr;

        // DEBUG: проверим что происходит
        std::cout << "[DEBUG] aggregation::on_prepare_impl - left_=" << (left_ ? "YES" : "NO")
                  << ", match_=" << (match_ ? "YES" : "NO") << std::endl;

        if (left_) {
            // Если есть левый ребёнок (например, INSERT или другой оператор)
            executor = std::move(left_);
            if (match_) {
                match_->set_children(std::move(executor));
                executor = std::move(match_);
            }
        } else {
            // Если нет левого ребёнка, используем match или создаём full_scan
            if (match_) {
                executor = std::move(match_);
            } else {
                // Создаём full_scan для document_table (аналог transfer_scan)
                executor = static_cast<operator_ptr>(boost::intrusive_ptr(
                    new full_scan(context_, nullptr, logical_plan::limit_t::unlimit())));
            }
        }

        // Добавляем GROUP BY если есть
        if (group_) {
            group_->set_children(std::move(executor));
            executor = std::move(group_);
        }

        // Добавляем ORDER BY если есть
        if (sort_) {
            sort_->set_children(std::move(executor));
            executor = std::move(sort_);
        }

        // Устанавливаем итоговую цепочку как дочерний оператор
        set_children(std::move(executor));
    }

} // namespace components::document_table::operators
