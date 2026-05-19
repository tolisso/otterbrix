#pragma once

#include <components/expressions/update_expression.hpp>
#include <components/logical_plan/node_aggregate.hpp>  // for computing_side_t
#include <components/physical_plan/operators/operator.hpp>

namespace components::operators {

    // UPDATE on a sparse computing table — re-alloc with full copying.
    //
    // Left child = scan_computing_table [+ operator_match_t] producing chunks
    // with the full virtual schema and chunk.row_ids = OLD logical row_ids.
    //
    // intercept_dml_io_::case update_computing then:
    //   1. Collect OLD logical row_ids from chunk.row_ids.
    //   2. Apply every SET expression in-place on the chunk (mutates user
    //      fields; unchanged fields stay as-is so the full row carries over).
    //   3. Allocate a fresh logical row_id range via storage_total_rows(main),
    //      split the mutated chunk into per-side `(row_id, value)` + main
    //      `(row_id)` chunks, storage_append × (N+1) with txn_id.
    //   4. For every target (main + every side), scan to find PHYSICAL
    //      positions where row_id-column ∈ OLD row_ids; storage_delete_rows
    //      × (N+1) with txn_id.
    //   5. WAL physical_insert × inserts, WAL physical_delete × deletes.
    //   6. txn_manager_->commit → commit_id; storage_commit_append × inserts,
    //      storage_commit_delete × deletes.
    //   7. Single WAL COMMIT marker.
    class operator_update_computing final : public read_write_operator_t {
    public:
        operator_update_computing(std::pmr::memory_resource* resource,
                                  log_t log,
                                  collection_full_name_t main,
                                  std::vector<logical_plan::computing_side_t> sides,
                                  std::pmr::vector<expressions::update_expr_ptr> updates);

        const collection_full_name_t& main_collection() const noexcept { return main_; }
        const std::vector<logical_plan::computing_side_t>& sides() const noexcept { return sides_; }
        const std::pmr::vector<expressions::update_expr_ptr>& updates() const noexcept { return updates_; }

    private:
        void on_execute_impl(pipeline::context_t* ctx) override;

        collection_full_name_t main_;
        std::vector<logical_plan::computing_side_t> sides_;
        std::pmr::vector<expressions::update_expr_ptr> updates_;
    };

} // namespace components::operators
