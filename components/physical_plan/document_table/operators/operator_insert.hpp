#pragma once

#include <components/physical_plan/base/operators/operator.hpp>
#include <components/vector/data_chunk.hpp>

namespace components::document_table::operators {

    class operator_insert final : public base::operators::read_write_operator_t {
    public:
        explicit operator_insert(services::collection::context_collection_t* context);

    private:
        void on_execute_impl(pipeline::context_t* pipeline_context) final;
    };

} // namespace components::document_table::operators
