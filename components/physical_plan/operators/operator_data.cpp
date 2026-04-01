#include "operator_data.hpp"
#include <components/vector/vector_operations.hpp>

namespace components::operators {

    operator_data_t::operator_data_t(std::pmr::memory_resource* resource,
                                     std::pmr::vector<types::complex_logical_type> types)
        : resource_(resource)
        , types_(std::move(types))
        , chunks_(resource) {}

    operator_data_t::operator_data_t(std::pmr::memory_resource* resource, vector::data_chunk_t&& chunk)
        : resource_(resource)
        , types_(chunk.types())
        , chunks_(resource) {
        chunks_.push_back(std::move(chunk));
    }

    operator_data_t::ptr operator_data_t::copy() const {
        auto result = make_operator_data(resource_, types_);
        for (const auto& chunk : chunks_) {
            vector::data_chunk_t new_chunk(resource_, types_, chunk.size());
            chunk.copy(new_chunk, 0);
            result->add_chunk(std::move(new_chunk));
        }
        return result;
    }

    std::size_t operator_data_t::size() const {
        std::size_t total = 0;
        for (const auto& chunk : chunks_) {
            total += chunk.size();
        }
        return total;
    }

    vector::data_chunk_t operator_data_t::merged() const {
        uint64_t total = static_cast<uint64_t>(size());
        uint64_t cap = total > 0 ? total : 1;
        vector::data_chunk_t result(resource_, types_, cap);
        for (const auto& chunk : chunks_) {
            uint64_t dst_offset = result.size();
            result.append(chunk);
            vector::vector_ops::copy(chunk.row_ids, result.row_ids, chunk.size(), 0, dst_offset);
        }
        return result;
    }

} // namespace components::operators
