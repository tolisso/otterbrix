#pragma once
#include <components/types/logical_value.hpp>
#include <components/vector/data_chunk.hpp>
#include <functional>

namespace components::sort {

    using types::compare_t;

    enum class order
    {
        descending = -1,
        ascending = 1
    };

    class columnar_sorter_t {
        struct sort_key {
            std::pmr::vector<size_t> col_path;
            order order_ = order::ascending;
            const vector::vector_t* vec = nullptr; // cached pointer set in set_chunk()
        };

    public:
        explicit columnar_sorter_t() = default;
        explicit columnar_sorter_t(size_t index, order order_ = order::ascending);

        void add(size_t index, order order_ = order::ascending);
        void add(const std::pmr::vector<size_t>& col_path, order order_ = order::ascending);

        // Single-chunk mode: cache vector pointers for fast comparison
        void set_chunk(const vector::data_chunk_t& chunk);

        // Multi-chunk mode: compare across a list of chunks
        using chunk_list = std::vector<const vector::data_chunk_t*>;
        void set_chunks(const chunk_list& chunks);

        bool operator()(size_t row_a, size_t row_b) const;

    private:
        static int compare_raw(const vector::vector_t& vec, size_t a, size_t b);
        static int compare_cross(const vector::vector_t& vec_a, size_t a,
                                 const vector::vector_t& vec_b, size_t b);

        // Locate (chunk_idx, local_row) for a global row index
        std::pair<size_t, size_t> locate(size_t global_idx) const;

        std::vector<sort_key> keys_;
        const vector::data_chunk_t* chunk_ = nullptr;

        // Multi-chunk state
        chunk_list multi_chunks_;
        std::vector<uint64_t> chunk_start_rows_; // prefix sums of chunk sizes
    };

} // namespace components::sort
