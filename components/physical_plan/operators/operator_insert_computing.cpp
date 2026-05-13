#include "operator_insert_computing.hpp"

namespace components::operators {

    operator_insert_computing::operator_insert_computing(
        std::pmr::memory_resource* resource,
        log_t log,
        collection_full_name_t main,
        std::vector<logical_plan::computing_side_t> sides)
        : read_write_operator_t(resource, std::move(log), operator_type::insert_computing)
        , main_(std::move(main))
        , sides_(std::move(sides)) {}

    void operator_insert_computing::on_execute_impl(pipeline::context_t* /*ctx*/) {
        // Mirror the user data from the child operator_data into our own output —
        // intercept_dml_io_ reads waiting_op->output() to find the input chunk.
        if (left_ && left_->output()) {
            output_ = left_->output();
        }
        if (output_ && output_->size() > 0) {
            async_wait();
        }
    }

} // namespace components::operators
