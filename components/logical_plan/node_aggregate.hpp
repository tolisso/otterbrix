#pragma once

#include "node.hpp"

#include <components/types/types.hpp>

#include <vector>

namespace components::logical_plan {

    // Side-table descriptor stamped onto an aggregate over a computing table.
    // Parallel to the virtual schema's child_types: index i in computing_sides_
    // corresponds to virtual column i.
    struct computing_side_t {
        collection_full_name_t collection;
        components::types::complex_logical_type field_type;
    };

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

        // Set when this aggregate scans a computing (sparse-schema) table. The vector
        // is parallel to the virtual schema's child_types: entry i carries the side
        // collection name and the field's type for virtual column i. Populated by
        // annotate_computing_aggregates before validate, consumed by
        // create_plan_aggregate to instantiate scan_computing_table.
        bool is_computing() const { return is_computing_; }
        const std::vector<computing_side_t>& computing_sides() const { return computing_sides_; }
        void set_computing_sides(std::vector<computing_side_t> sides) {
            is_computing_ = true;
            computing_sides_ = std::move(sides);
        }

    private:
        bool distinct_{false};
        bool is_computing_{false};
        std::vector<size_t> projected_cols_;
        std::vector<computing_side_t> computing_sides_;
        hash_t hash_impl() const override;
        std::string to_string_impl() const override;
    };

    using node_aggregate_ptr = boost::intrusive_ptr<node_aggregate_t>;

    node_aggregate_ptr make_node_aggregate(std::pmr::memory_resource* resource,
                                           const collection_full_name_t& collection);

} // namespace components::logical_plan
