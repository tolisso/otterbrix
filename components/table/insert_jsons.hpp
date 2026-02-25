#pragma once

#include "data_table.hpp"
#include <memory_resource>
#include <string>
#include <vector>

namespace components::table {

    // Parses JSON strings, automatically creates STRING_LITERAL columns for every
    // leaf path found across all documents (using dot notation for object keys and
    // bracket notation for array indices, e.g. "commit.collection", "logs[1].info"),
    // then appends one row per JSON string (NULL for any missing path).
    //
    // Takes ownership of the input table and returns the (possibly extended) table.
    // If new columns are needed, the returned table is a fresh data_table_t that
    // shares the underlying row-groups with the original; the original table is
    // consumed and should not be used after the call.
    std::unique_ptr<data_table_t> insert_jsons(std::unique_ptr<data_table_t> table,
                                               std::pmr::memory_resource* resource,
                                               const std::vector<std::string>& jsons);

} // namespace components::table
