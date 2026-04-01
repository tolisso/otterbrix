#include "operator_raw_data.hpp"

namespace components::operators {

    operator_raw_data_t::operator_raw_data_t(vector::data_chunk_t&& chunk)
        : read_only_operator_t(nullptr, log_t{}, operator_type::raw_data) {
        output_ = make_operator_data(chunk.resource(), std::move(chunk));
    }

    operator_raw_data_t::operator_raw_data_t(const vector::data_chunk_t& chunk)
        : read_only_operator_t(nullptr, log_t{}, operator_type::raw_data) {
        vector::data_chunk_t copy(chunk.resource(), chunk.types(), chunk.size());
        chunk.copy(copy, 0);
        output_ = make_operator_data(chunk.resource(), std::move(copy));
    }

    std::pmr::memory_resource* operator_raw_data_t::resource() const noexcept { return output_->resource(); }

    void operator_raw_data_t::on_execute_impl(pipeline::context_t*) {}

} // namespace components::operators
