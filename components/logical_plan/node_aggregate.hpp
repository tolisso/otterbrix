#pragma once

#include "node.hpp"

#include <vector>

namespace components::logical_plan {

    class node_aggregate_t final : public node_t {
    public:
        explicit node_aggregate_t(std::pmr::memory_resource* resource, const collection_full_name_t& collection);

        void set_distinct(bool d) { distinct_ = d; }
        bool is_distinct() const { return distinct_; }

        // Column projection metadata, populated by the post-validate column_pruning pass.
        // When non-empty, downstream scan operators read only these column indices from the
        // source table instead of scanning every column. An empty vector means "no projection"
        // (i.e. scan all columns) — this is the default.
        const std::vector<size_t>& projected_cols() const { return projected_cols_; }
        void set_projected_cols(std::vector<size_t> cols) { projected_cols_ = std::move(cols); }

        // Marks an aggregate that physically scans a computing table's main storage as a
        // plain table (schema = [row_id]). Set by the computing-schema expansion in planner
        // to break recursion: validator/optimizer for raw-scan aggregates skip the
        // `latest_types_struct` path and treat it as a regular table.
        bool is_raw_computing_scan() const { return raw_computing_scan_; }
        void set_raw_computing_scan(bool v) { raw_computing_scan_ = v; }

    private:
        bool distinct_{false};
        bool raw_computing_scan_{false};
        std::vector<size_t> projected_cols_;
        hash_t hash_impl() const override;
        std::string to_string_impl() const override;
    };

    using node_aggregate_ptr = boost::intrusive_ptr<node_aggregate_t>;

    node_aggregate_ptr make_node_aggregate(std::pmr::memory_resource* resource,
                                           const collection_full_name_t& collection);

} // namespace components::logical_plan
