#pragma once

#include <components/physical_plan/base/operators/operator.hpp>
#include <components/physical_plan/document_table/operators/base_operator.hpp>
#include <expressions/compare_expression.hpp>

namespace components::document_table::operators {

    /**
     * @brief Delete operator for document_table
     *
     * Delegates most work to the underlying data_table.
     * Handles document ID mappings for document_table_storage.
     */
    class operator_delete final : public base::operators::read_write_operator_t,
                                   public base_helper_t {
    public:
        explicit operator_delete(
            services::collection::context_collection_t* context,
            expressions::compare_expression_ptr expr = nullptr
        );

    private:
        void on_execute_impl(pipeline::context_t* pipeline_context) final;

        expressions::compare_expression_ptr compare_expression_;
    };

} // namespace components::document_table::operators
