#pragma once

#include <components/catalog/catalog.hpp>
#include <components/logical_plan/node.hpp>
#include <memory_resource>

namespace components::planner {

    // Replaces every `aggregate(t)` where `t` is a computing table with a
    // subquery-based plan that LEFT JOINs `t`'s side tables (`_dyn_t__field__N`).
    // The subquery exposes columns with `result_alias = t.collection`, so the
    // outer plan's `t.field` references resolve unchanged.
    //
    // Only side tables whose fields are actually referenced in the original
    // aggregate's children are joined — fields not used pay no JOIN cost.
    //
    // If a multi-type field (e.g. `val` exists as both BIGINT and STRING) is
    // referenced WITHOUT an explicit cast (`val::string`/`val::bigint`), the
    // rewrite is ambiguous and `*out_error` is set to `schema_error` /
    // `field_not_exists`. The caller should propagate this error up.
    //
    // Idempotent: aggregates marked `is_raw_computing_scan()` are skipped (raw
    // scans are how the rewrite refers back to the main physical table).
    logical_plan::node_ptr expand_computing_tables(std::pmr::memory_resource* resource,
                                                    logical_plan::node_ptr root,
                                                    const catalog::catalog& cat,
                                                    core::error_t* out_error = nullptr);

    // Walk the logical plan; for every `aggregate(t)` where `t` is a computing
    // table, stamp `computing_sides_` (parallel to the virtual schema's
    // child_types) and `is_computing_=true` on the aggregate node. This is the
    // physical-plan-driven path: the validator continues to see the virtual
    // schema unchanged, and create_plan_aggregate uses the stamped metadata to
    // build a scan_computing_table operator instead of a transfer_scan.
    //
    // Skips aggregates already marked `is_raw_computing_scan()` (those are the
    // legacy DELETE-path raw-scan markers that target main directly).
    void annotate_computing_aggregates(std::pmr::memory_resource* resource,
                                       logical_plan::node_ptr root,
                                       const catalog::catalog& cat);

    // Post-validate fixup: rebase outer-level expression `key.path()` indices through
    // the projection inside `is_computing_subquery_wrapper()` aggregates.
    //
    // The validator resolves all keys in one pass against the deepest scope (here,
    // the JOIN-internal schema with row_id+all-side-cols). But our subquery_agg
    // exposes only user-fields via my_select_node, so an outer WHERE key whose
    // validator-resolved path points at, say, JOIN-column 2, must be rebased to
    // the matching position in the projection (which might be 1).
    void fixup_computing_paths(logical_plan::node_ptr root);

    // Build a `SELECT row_id FROM <expanded virtual t> [WHERE where_expr]` plan that
    // exposes the main table's row_id as the only projected column. Used by the
    // computing-DELETE path to enumerate affected row_ids.
    //
    // Differs from expand_computing_tables: there's no subquery wrapper that hides
    // row_id; instead the JOIN chain is the direct child of the outer aggregate, and
    // the select projects [main.row_id] explicitly.
    //
    // `where_expr` may be null → no WHERE filter (returns all row_ids).
    logical_plan::node_ptr build_select_row_ids(std::pmr::memory_resource* resource,
                                                const catalog::catalog& cat,
                                                const collection_full_name_t& main_full,
                                                const expressions::expression_ptr& where_expr);

} // namespace components::planner
