#include "operator_update_computing.hpp"

namespace components::operators {

    operator_update_computing::operator_update_computing(
        std::pmr::memory_resource* resource,
        log_t log,
        collection_full_name_t main,
        std::vector<logical_plan::computing_side_t> sides,
        std::pmr::vector<expressions::update_expr_ptr> updates)
        : read_write_operator_t(resource, std::move(log), operator_type::update_computing)
        , main_(std::move(main))
        , sides_(std::move(sides))
        , updates_(std::move(updates)) {}

    void operator_update_computing::on_execute_impl(pipeline::context_t* /*ctx*/) {
        // Propagate the resolve pipeline's output (virtual schema chunk +
        // chunk.row_ids = old logical row_ids) so intercept_dml_io_ can read
        // both values and old row_ids from waiting_op->output().
        if (left_ && left_->output()) {
            output_ = left_->output();
        }
        async_wait();
    }

} // namespace components::operators
