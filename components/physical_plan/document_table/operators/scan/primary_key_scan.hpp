#pragma once

#include <components/document/document_id.hpp>
#include <components/logical_plan/node_limit.hpp>
#include <components/physical_plan/base/operators/operator.hpp>
#include <components/vector/vector.hpp>
#include <expressions/compare_expression.hpp>

namespace components::document_table::operators {

    class primary_key_scan final : public base::operators::read_only_operator_t {
    public:
        primary_key_scan(services::collection::context_collection_t* context,
                        const expressions::compare_expression_ptr& expression);

        // Добавление document_id для поиска (для программного API)
        void append(const document::document_id_t& id);

    private:
        void on_execute_impl(pipeline::context_t* pipeline_context) final;

        // Expression для извлечения значения _id
        expressions::compare_expression_ptr expression_;
        
        // Список document_id для поиска
        std::pmr::vector<document::document_id_t> document_ids_;
    };

} // namespace components::document_table::operators

