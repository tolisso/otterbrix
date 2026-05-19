#include "operator_delete_computing.hpp"

namespace components::operators {

    operator_delete_computing::operator_delete_computing(
        std::pmr::memory_resource* resource,
        log_t log,
        collection_full_name_t main,
        std::vector<logical_plan::computing_side_t> sides)
        : read_write_operator_t(resource, std::move(log), operator_type::remove_computing)
        , main_(std::move(main))
        , sides_(std::move(sides)) {}

    void operator_delete_computing::on_execute_impl(pipeline::context_t* /*ctx*/) {
        // Propagate the resolve pipeline's output so intercept_dml_io_ can read
        // logical row_ids out of waiting_op->output()->data_chunk().row_ids.
        if (left_ && left_->output()) {
            output_ = left_->output();
        }
        async_wait();
    }

} // namespace components::operators
