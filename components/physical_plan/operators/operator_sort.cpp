#include "operator_sort.hpp"
#include <components/vector/vector_buffer.hpp>
#include <chrono>
#include <iostream>
#include <numeric>

namespace components::operators {

    namespace {

        // Copy a single value from src[src_row] to dst[dst_row] using raw typed access.
        // Avoids logical_value_t for primitive types.
        void scatter_value(const vector::vector_t& src, size_t src_row,
                           vector::vector_t& dst, size_t dst_row) {
            using PT = types::physical_type;
            bool is_null = src.is_null(src_row);
            dst.validity().set(dst_row, !is_null);
            if (is_null) return;
            switch (src.type().to_physical_type()) {
                case PT::BOOL:
                case PT::INT8:    dst.data<int8_t>()[dst_row]   = src.data<int8_t>()[src_row];   break;
                case PT::INT16:   dst.data<int16_t>()[dst_row]  = src.data<int16_t>()[src_row];  break;
                case PT::INT32:   dst.data<int32_t>()[dst_row]  = src.data<int32_t>()[src_row];  break;
                case PT::INT64:   dst.data<int64_t>()[dst_row]  = src.data<int64_t>()[src_row];  break;
                case PT::UINT8:   dst.data<uint8_t>()[dst_row]  = src.data<uint8_t>()[src_row];  break;
                case PT::UINT16:  dst.data<uint16_t>()[dst_row] = src.data<uint16_t>()[src_row]; break;
                case PT::UINT32:  dst.data<uint32_t>()[dst_row] = src.data<uint32_t>()[src_row]; break;
                case PT::UINT64:  dst.data<uint64_t>()[dst_row] = src.data<uint64_t>()[src_row]; break;
                case PT::INT128:  dst.data<types::int128_t>()[dst_row]  = src.data<types::int128_t>()[src_row];  break;
                case PT::UINT128: dst.data<types::uint128_t>()[dst_row] = src.data<types::uint128_t>()[src_row]; break;
                case PT::FLOAT:   dst.data<float>()[dst_row]  = src.data<float>()[src_row];  break;
                case PT::DOUBLE:  dst.data<double>()[dst_row] = src.data<double>()[src_row]; break;
                case PT::STRING: {
                    auto aux = dst.auxiliary();
                    if (!aux) {
                        aux = std::make_shared<vector::string_vector_buffer_t>(dst.resource());
                        dst.set_auxiliary(aux);
                    }
                    auto src_sv = src.data<std::string_view>()[src_row];
                    auto* buf = static_cast<vector::string_vector_buffer_t*>(aux.get());
                    dst.data<std::string_view>()[dst_row] =
                        std::string_view(reinterpret_cast<char*>(buf->insert(src_sv)), src_sv.size());
                    break;
                }
                default:
                    // Complex types (STRUCT, LIST, ARRAY): fall back to set_value
                    dst.set_value(dst_row, src.value(src_row));
                    break;
            }
        }

    } // anonymous namespace

    operator_sort_t::operator_sort_t(std::pmr::memory_resource* resource, log_t log)
        : read_only_operator_t(resource, log, operator_type::sort) {}

    void operator_sort_t::add(size_t index, operator_sort_t::order order_) { sorter_.add(index, order_); }

    void operator_sort_t::add(const std::pmr::vector<size_t>& col_path, order order_) { sorter_.add(col_path, order_); }

    void operator_sort_t::on_execute_impl(pipeline::context_t*) {
        if (!left_ || !left_->output()) {
            return;
        }

        const auto& chunks = left_->output()->chunks();
        if (chunks.empty()) return;

        // DEBUG
        {
            size_t total_in = 0;
            for (const auto& c : chunks) total_in += c.size();
            std::cout << "[SORT_OP] input: " << total_in << " rows, "
                      << chunks.size() << " chunk(s)\n";
        }
        auto dbg_t0 = std::chrono::steady_clock::now();

        // Fast path: single chunk — use indexing sort + indexed copy (no allocation of intermediate)
        if (chunks.size() == 1) {
            const auto& chunk = chunks[0];
            auto num_rows = chunk.size();
            if (num_rows == 0) {
                output_ = operators::make_operator_data(left_->output()->resource(),
                                                        vector::data_chunk_t(resource_, chunk.types(), 0));
                return;
            }
            vector::indexing_vector_t indexing(resource_, uint64_t(0), num_rows);
            sorter_.set_chunk(chunk);
            std::sort(indexing.data(), indexing.data() + num_rows, std::ref(sorter_));

            vector::data_chunk_t result(resource_, chunk.types(), num_rows);
            chunk.copy(result, indexing, num_rows, 0);

            if (expected_output_count_ > 0 && result.data.size() > expected_output_count_) {
                result.data.erase(result.data.begin() + static_cast<ptrdiff_t>(expected_output_count_),
                                  result.data.end());
            }
            std::cout << "[SORT_OP] single-chunk sort done: "
                      << std::chrono::duration<double,std::milli>(
                             std::chrono::steady_clock::now() - dbg_t0).count()
                      << " ms  output=" << result.size() << " rows\n";
            output_ = operators::make_operator_data(left_->output()->resource(), std::move(result));
            return;
        }

        // Multi-chunk path: sort global index array cross-chunk, then scatter-gather into result.
        // No intermediate merged chunk allocated.

        // 1. Compute total rows and chunk offset table
        size_t num_rows = 0;
        std::vector<size_t> chunk_offsets;
        chunk_offsets.reserve(chunks.size());
        for (const auto& c : chunks) {
            chunk_offsets.push_back(num_rows);
            num_rows += c.size();
        }
        if (num_rows == 0) {
            output_ = operators::make_operator_data(left_->output()->resource(),
                                                    vector::data_chunk_t(resource_, chunks[0].types(), 0));
            return;
        }

        // 2. Sort global indices [0..N-1] using cross-chunk comparison
        std::vector<size_t> global_order(num_rows);
        std::iota(global_order.begin(), global_order.end(), size_t(0));

        sort::columnar_sorter_t::chunk_list chunk_ptrs;
        chunk_ptrs.reserve(chunks.size());
        for (const auto& c : chunks) chunk_ptrs.push_back(&c);
        sorter_.set_chunks(chunk_ptrs);
        std::sort(global_order.begin(), global_order.end(), std::ref(sorter_));

        // 3. Precompute row_map[sorted_pos] = (chunk_idx, local_row)
        //    We linearise it as two parallel arrays to avoid struct overhead.
        std::vector<uint32_t> map_chunk(num_rows);
        std::vector<uint32_t> map_local(num_rows);
        {
            // Build a lookup: for each global index, find its chunk
            // chunk_offsets is sorted ascending — use it as a lookup table
            size_t ci = 0;
            for (size_t g = 0; g < num_rows; g++) {
                // advance ci to the owning chunk
                while (ci + 1 < chunks.size() && chunk_offsets[ci + 1] <= g) ++ci;
                map_chunk[g] = static_cast<uint32_t>(ci);
                map_local[g] = static_cast<uint32_t>(g - chunk_offsets[ci]);
            }
        }

        // 4. Scatter-gather: write rows into result in sorted order
        //    Column-wise loop avoids repeated type dispatch per cell.
        auto types = left_->output()->types();
        vector::data_chunk_t result(resource_, types, num_rows);

        size_t ncols = result.column_count();
        for (size_t col = 0; col < ncols; col++) {
            auto& dst_vec = result.data[col];
            for (size_t dst = 0; dst < num_rows; dst++) {
                size_t g = global_order[dst];
                const auto& src_vec = chunks[map_chunk[g]].data[col];
                scatter_value(src_vec, map_local[g], dst_vec, dst);
            }
        }
        result.set_cardinality(num_rows);

        if (expected_output_count_ > 0 && result.data.size() > expected_output_count_) {
            result.data.erase(result.data.begin() + static_cast<ptrdiff_t>(expected_output_count_),
                              result.data.end());
        }

        std::cout << "[SORT_OP] multi-chunk sort done: "
                  << std::chrono::duration<double,std::milli>(
                         std::chrono::steady_clock::now() - dbg_t0).count()
                  << " ms  output=" << result.size() << " rows\n";
        output_ = operators::make_operator_data(left_->output()->resource(), std::move(result));
    }

} // namespace components::operators
