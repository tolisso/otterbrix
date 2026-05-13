#pragma once

#include <components/logical_plan/node_aggregate.hpp>  // for computing_side_t
#include <components/physical_plan/operators/operator.hpp>

namespace components::operators {

    // INSERT into a sparse computing table.
    //
    // Lightweight DML operator — only carries the main collection + the parallel
    // side list (one entry per chunk column). All async work (storage_total_rows
    // for next row_id, splitting the user chunk into per-side `(row_id, value)`
    // tuples plus a main `(row_id)` chunk, storage_append × (N+1), WAL physicals,
    // txn_manager_->commit, storage_commit_append × (N+1), WAL commit-marker) is
    // driven inline by intercept_dml_io_::case insert_computing under one
    // transaction id, so the post-DML loop in execute_plan has nothing to do.
    class operator_insert_computing final : public read_write_operator_t {
    public:
        operator_insert_computing(std::pmr::memory_resource* resource,
                                  log_t log,
                                  collection_full_name_t main,
                                  std::vector<logical_plan::computing_side_t> sides);

        const collection_full_name_t& main_collection() const noexcept { return main_; }
        const std::vector<logical_plan::computing_side_t>& sides() const noexcept { return sides_; }

    private:
        void on_execute_impl(pipeline::context_t* ctx) override;

        collection_full_name_t main_;
        std::vector<logical_plan::computing_side_t> sides_;
    };

} // namespace components::operators
