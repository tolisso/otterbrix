#pragma once

#include <components/physical_plan/base/operators/operator.hpp>
#include <vector>
#include <string>

namespace components::document_table::operators {

    using components::base::operators::operator_ptr;
    using components::base::operators::operator_type;
    using components::base::operators::read_only_operator_t;
    using components::pipeline::context_t;

    class aggregation final : public read_only_operator_t {
    public:
        explicit aggregation(services::collection::context_collection_t* context);

        void set_match(operator_ptr&& match);
        void set_group(operator_ptr&& group);
        void set_sort(operator_ptr&& sort);

        // Set projection columns - columns needed for GROUP BY and aggregations
        void set_projection(std::vector<std::string> columns);

    private:
        operator_ptr match_{nullptr};
        operator_ptr group_{nullptr};
        operator_ptr sort_{nullptr};
        std::vector<std::string> projection_columns_;

        void on_execute_impl(context_t* pipeline_context) final;
        void on_prepare_impl() final;
    };

} // namespace components::document_table::operators
