#pragma once

#include <components/logical_plan/node_limit.hpp>
#include <components/physical_plan/operators/operator.hpp>
#include <components/types/types.hpp>

namespace components::operators {

    // Source operator for `aggregate(t)` where `t` is a sparse computing table.
    //
    // Storage layout:
    //   - main physical table = single column `row_id BIGINT`, listing live rows.
    //   - per (field, type) side table `_dyn_<t>__<field>__<tid>(row_id, value)` —
    //     holds only rows that have a non-NULL value for that field/type.
    //
    // Execution:
    //   1. Scan main + every needed side in parallel via storage_scan_batched.
    //   2. Walk main chunks to gather alive row_ids in their natural order (the
    //      canonical row ordering for the virtual table) and build a
    //      row_id → global-output-position map.
    //   3. Allocate output chunks (≤ DEFAULT_VECTOR_CAPACITY) matching the
    //      virtual schema. Projected columns get real buffers initialized to
    //      all-NULL; non-projected columns get placeholder vectors (no buffer).
    //   4. For each side row (rid, val), look up rid → position and write val at
    //      that position. Missing rows stay NULL.
    //
    // row_ids may be non-monotonic (DELETE + INSERT can reuse positions in
    // main), so we hash-gather instead of merge-joining.
    class scan_computing_table final : public read_only_operator_t {
    public:
        scan_computing_table(std::pmr::memory_resource* resource,
                             collection_full_name_t main,
                             std::pmr::vector<types::complex_logical_type> virtual_types,
                             std::pmr::vector<collection_full_name_t> side_names,
                             std::vector<size_t> projected_cols,
                             logical_plan::limit_t limit);

        actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx) override;

    private:
        void on_execute_impl(pipeline::context_t* ctx) override;

        collection_full_name_t main_;
        std::pmr::vector<types::complex_logical_type> virtual_types_;
        std::pmr::vector<collection_full_name_t> side_names_;
        std::vector<size_t> projected_cols_;
        logical_plan::limit_t limit_;
    };

} // namespace components::operators
