#include "scan_computing_table.hpp"

#include <services/disk/manager_disk.hpp>

#include <algorithm>
#include <unordered_map>

namespace components::operators {

    scan_computing_table::scan_computing_table(std::pmr::memory_resource* resource,
                                               collection_full_name_t main,
                                               std::pmr::vector<types::complex_logical_type> virtual_types,
                                               std::pmr::vector<collection_full_name_t> side_names,
                                               std::vector<size_t> projected_cols,
                                               logical_plan::limit_t limit)
        : read_only_operator_t(resource, log_t{}, operator_type::scan_computing_table)
        , main_(std::move(main))
        , virtual_types_(std::move(virtual_types))
        , side_names_(std::move(side_names))
        , projected_cols_(std::move(projected_cols))
        , limit_(limit) {}

    void scan_computing_table::on_execute_impl(pipeline::context_t* /*ctx*/) { async_wait(); }

    actor_zeta::unique_future<void> scan_computing_table::await_async_and_resume(pipeline::context_t* ctx) {
        const int64_t offset_val = limit_.offset();
        const int64_t limit_val = limit_.limit();
        const int64_t main_scan_limit = (limit_val < 0) ? limit_val : limit_val + offset_val;

        // Which side indices we actually need data from. Empty projected_cols means
        // "no pruning — read everything" (column_pruning didn't run, e.g. for tests).
        std::vector<bool> col_needed(virtual_types_.size(), projected_cols_.empty());
        for (size_t idx : projected_cols_) {
            if (idx < virtual_types_.size()) {
                col_needed[idx] = true;
            }
        }
        std::vector<size_t> needed_sides;
        needed_sides.reserve(virtual_types_.size());
        for (size_t i = 0; i < virtual_types_.size(); ++i) {
            if (col_needed[i] && i < side_names_.size() && !side_names_[i].collection.empty()) {
                needed_sides.push_back(i);
            }
        }

        // 1) Fire main + side scans in parallel.
        auto [_m, mf] = actor_zeta::send(ctx->disk_address,
                                         &services::disk::manager_disk_t::storage_scan_batched,
                                         ctx->session,
                                         main_,
                                         std::unique_ptr<table::table_filter_t>(nullptr),
                                         main_scan_limit,
                                         std::vector<size_t>{},
                                         ctx->txn);

        std::vector<actor_zeta::unique_future<std::pmr::vector<vector::data_chunk_t>>> side_futs;
        side_futs.reserve(needed_sides.size());
        for (size_t side_idx : needed_sides) {
            auto [_s, sf] = actor_zeta::send(ctx->disk_address,
                                             &services::disk::manager_disk_t::storage_scan_batched,
                                             ctx->session,
                                             side_names_[side_idx],
                                             std::unique_ptr<table::table_filter_t>(nullptr),
                                             int64_t{-1},
                                             std::vector<size_t>{},
                                             ctx->txn);
            side_futs.push_back(std::move(sf));
        }

        // 2) Await main; collect live row_ids, applying offset.
        auto main_chunks = co_await std::move(mf);
        uint64_t skip = static_cast<uint64_t>(std::max<int64_t>(offset_val, 0));
        std::vector<int64_t> row_ids;
        size_t total_main_rows = 0;
        for (const auto& c : main_chunks) total_main_rows += c.size();
        row_ids.reserve(total_main_rows);
        for (const auto& c : main_chunks) {
            if (c.column_count() == 0 || c.size() == 0) continue;
            const auto& rid_col = c.data[0];
            for (uint64_t r = 0; r < c.size(); ++r) {
                if (!rid_col.validity().row_is_valid(r)) continue;
                if (skip > 0) {
                    --skip;
                    continue;
                }
                row_ids.push_back(rid_col.value(r).value<int64_t>());
            }
        }
        const uint64_t n_out = row_ids.size();

        // 3) row_id → global output position.
        std::unordered_map<int64_t, uint64_t> rid_to_pos;
        rid_to_pos.reserve(n_out * 2 + 1);
        for (uint64_t i = 0; i < n_out; ++i) {
            rid_to_pos.emplace(row_ids[i], i);
        }

        // 4) Allocate output chunks, ≤ DEFAULT_VECTOR_CAPACITY each.
        const uint64_t cap = vector::DEFAULT_VECTOR_CAPACITY;
        chunks_vector_t out_chunks(resource_);
        const bool no_pruning = projected_cols_.empty();
        if (n_out == 0) {
            vector::data_chunk_t empty =
                no_pruning
                    ? vector::data_chunk_t(resource_, virtual_types_, uint64_t{0})
                    : vector::data_chunk_t(resource_, virtual_types_, projected_cols_, uint64_t{0});
            empty.set_cardinality(0);
            out_chunks.emplace_back(std::move(empty));
        } else {
            const uint64_t n_chunks = (n_out + cap - 1) / cap;
            out_chunks.reserve(n_chunks);
            for (uint64_t off = 0; off < n_out; off += cap) {
                uint64_t this_cap = std::min<uint64_t>(cap, n_out - off);
                vector::data_chunk_t chunk =
                    no_pruning ? vector::data_chunk_t(resource_, virtual_types_, this_cap)
                               : vector::data_chunk_t(resource_, virtual_types_, projected_cols_, this_cap);
                chunk.set_cardinality(this_cap);
                for (size_t i = 0; i < virtual_types_.size(); ++i) {
                    if (!col_needed[i]) continue;
                    chunk.data[i].validity().set_all_invalid(this_cap);
                }
                out_chunks.emplace_back(std::move(chunk));
            }
        }

        // 5) Scatter side values into output positions.
        for (size_t k = 0; k < needed_sides.size(); ++k) {
            const size_t side_idx = needed_sides[k];
            auto side_chunks = co_await std::move(side_futs[k]);
            for (const auto& sc : side_chunks) {
                if (sc.column_count() < 2 || sc.size() == 0) continue;
                const auto& rid_col = sc.data[0];
                const auto& val_col = sc.data[1];
                for (uint64_t r = 0; r < sc.size(); ++r) {
                    if (!rid_col.validity().row_is_valid(r)) continue;
                    const int64_t rid = rid_col.value(r).value<int64_t>();
                    auto it = rid_to_pos.find(rid);
                    if (it == rid_to_pos.end()) continue;
                    const uint64_t global_pos = it->second;
                    const uint64_t chunk_idx = global_pos / cap;
                    const uint64_t in_chunk = global_pos % cap;
                    auto& dst = out_chunks[chunk_idx].data[side_idx];
                    if (!val_col.validity().row_is_valid(r)) {
                        continue; // leave NULL
                    }
                    dst.set_value(in_chunk, val_col.value(r));
                    dst.validity().set_valid(in_chunk);
                }
            }
        }

        output_ = make_operator_data(resource_, std::move(out_chunks));
        mark_executed();
        co_return;
    }

} // namespace components::operators
