#pragma once

#include <components/catalog/catalog.hpp>
#include <components/logical_plan/node.hpp>
#include <memory_resource>

namespace components::planner {

    // Walk the logical plan; for every `aggregate(t)` where `t` is a computing
    // (sparse-schema) table, stamp `computing_sides_` (parallel to the virtual
    // schema's child_types) and `is_computing_=true` on the aggregate node.
    //
    // The logical plan structure stays unchanged — validator continues to see
    // the virtual schema via `get_computing_table_schema(...).latest_types_struct()`,
    // and `create_plan_aggregate` reads the stamped metadata to emit
    // `scan_computing_table` instead of a `transfer_scan`.
    void annotate_computing_aggregates(std::pmr::memory_resource* resource,
                                       logical_plan::node_ptr root,
                                       const catalog::catalog& cat);

} // namespace components::planner
