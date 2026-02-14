#pragma once

#include <components/expressions/compare_expression.hpp>
#include <components/expressions/update_expression.hpp>
#include <components/physical_plan/base/operators/operator.hpp>
#include <components/physical_plan/document_table/operators/base_operator.hpp>

namespace components::document_table::operators {

    /**
     * @brief Update operator for document_table
     *
     * Delegates most work to the underlying data_table.
     * Supports both regular UPDATE and UPSERT operations.
     */
    class operator_update final : public base::operators::read_write_operator_t,
                                   public base_helper_t {
    public:
        operator_update(services::collection::context_collection_t* context,
                        std::pmr::vector<expressions::update_expr_ptr> updates,
                        bool upsert,
                        expressions::compare_expression_ptr comp_expr = nullptr);

    private:
        void on_execute_impl(pipeline::context_t* pipeline_context) final;

        std::pmr::vector<expressions::update_expr_ptr> updates_;
        expressions::compare_expression_ptr comp_expr_;
        bool upsert_;
    };

} // namespace components::document_table::operators
