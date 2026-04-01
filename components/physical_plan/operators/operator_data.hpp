#pragma once

#include <boost/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>
#include <memory_resource>
#include <vector/data_chunk.hpp>

namespace components::operators {

    class operator_data_t : public boost::intrusive_ref_counter<operator_data_t> {
    public:
        using ptr = boost::intrusive_ptr<operator_data_t>;
        using chunk_list = std::pmr::vector<vector::data_chunk_t>;

        // Empty data with known types (no chunks yet)
        operator_data_t(std::pmr::memory_resource* resource,
                        std::pmr::vector<types::complex_logical_type> types);

        // Wraps a single chunk (types taken from chunk)
        operator_data_t(std::pmr::memory_resource* resource, vector::data_chunk_t&& chunk);

        ptr copy() const;

        // Total row count across all chunks
        std::size_t size() const;

        chunk_list& chunks() { return chunks_; }
        const chunk_list& chunks() const { return chunks_; }
        void add_chunk(vector::data_chunk_t chunk) { chunks_.push_back(std::move(chunk)); }

        // Column types (stored at construction time)
        const std::pmr::vector<types::complex_logical_type>& types() const { return types_; }

        std::pmr::memory_resource* resource() const { return resource_; }

        // Merge all chunks into one new chunk. O(n) with pre-allocated capacity.
        vector::data_chunk_t merged() const;

    private:
        std::pmr::memory_resource* resource_;
        std::pmr::vector<types::complex_logical_type> types_;
        chunk_list chunks_;
    };

    using operator_data_ptr = operator_data_t::ptr;

    // Create empty output with known types
    inline operator_data_ptr make_operator_data(std::pmr::memory_resource* resource,
                                                std::pmr::vector<types::complex_logical_type> types) {
        return {new operator_data_t(resource, std::move(types))};
    }

    // Wrap a single chunk
    inline operator_data_ptr make_operator_data(std::pmr::memory_resource* resource, vector::data_chunk_t&& chunk) {
        return {new operator_data_t(resource, std::move(chunk))};
    }

} // namespace components::operators
