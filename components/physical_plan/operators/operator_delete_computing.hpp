#pragma once

#include <components/logical_plan/node_aggregate.hpp>  // for computing_side_t
#include <components/physical_plan/operators/operator.hpp>

namespace components::operators {

    // DELETE on a sparse computing table.
    //
    // Holds the multi-collection target (main + every side) so intercept_dml_io_
    // can drive the full DML lifecycle inline. Its left child is the row-id
    // resolve pipeline — scan_computing_table + (optional) operator_match_t
    // producing chunks whose `chunk.row_ids` carry the LOGICAL row_ids of rows
    // that match the user WHERE.
    //
    // intercept_dml_io_::case remove_computing then:
    //   1. Collects logical row_ids from the operator's output chunk.row_ids.
    //   2. For each target (main + every side) scans storage to find the
    //      PHYSICAL positions whose row_id-column value lives in our set.
    //   3. storage_delete_rows × N+1 with the same txn_id.
    //   4. WAL physical_delete × N+1.
    //   5. txn_manager_->commit → commit_id; storage_commit_delete × N+1.
    //   6. WAL COMMIT marker.
    //
    // sides_ lists every side at DELETE time — main DELETE is implicit (main_ is
    // always the first target).
    class operator_delete_computing final : public read_write_operator_t {
    public:
        operator_delete_computing(std::pmr::memory_resource* resource,
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
