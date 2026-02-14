#pragma once

#include <components/logical_plan/node_limit.hpp>
#include <components/physical_plan/base/operators/operator.hpp>
#include <components/table/column_state.hpp>
#include <expressions/compare_expression.hpp>
#include <string>
#include <vector>

namespace components::table::operators {

    std::unique_ptr<table::table_filter_t> transform_predicate(const expressions::compare_expression_ptr& expression);

    class full_scan final : public read_only_operator_t {
    public:
        full_scan(services::collection::context_collection_t* collection,
                  const expressions::compare_expression_ptr& expression,
                  logical_plan::limit_t limit);

        // Set projection - list of column names to read (empty = all columns)
        void set_projection(std::vector<std::string> columns);

    private:
        void on_execute_impl(pipeline::context_t* pipeline_context) final;

        expressions::compare_expression_ptr expression_;
        const logical_plan::limit_t limit_;
        std::vector<std::string> projection_columns_;  // Empty = all columns
    };

} // namespace components::table::operators
