#pragma once

#include <components/logical_plan/node_limit.hpp>
#include <components/physical_plan/base/operators/operator.hpp>
#include <components/table/column_state.hpp>
#include <expressions/compare_expression.hpp>

namespace components::document_table::operators {

    class full_scan final : public base::operators::read_only_operator_t {
    public:
        full_scan(services::collection::context_collection_t* context,
                  const expressions::compare_expression_ptr& expression,
                  logical_plan::limit_t limit);

    private:
        void on_execute_impl(pipeline::context_t* pipeline_context) final;

        expressions::compare_expression_ptr expression_;
        const logical_plan::limit_t limit_;
    };

} // namespace components::document_table::operators
