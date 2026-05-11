#pragma once

#include "node.hpp"

#include <components/table/column_definition.hpp>
#include <components/table/constraint.hpp>
#include <components/types/types.hpp>

namespace components::logical_plan {

    class node_create_collection_t final : public node_t {
    public:
        explicit node_create_collection_t(std::pmr::memory_resource* resource,
                                          const collection_full_name_t& collection,
                                          bool disk_storage = false);

        node_create_collection_t(std::pmr::memory_resource* resource,
                                 const collection_full_name_t& collection,
                                 std::vector<table::column_definition_t> column_definitions,
                                 std::vector<table::table_constraint_t> constraints,
                                 bool disk_storage = false);

        std::pmr::vector<types::complex_logical_type> schema() const;

        std::vector<table::column_definition_t>& column_definitions();
        const std::vector<table::column_definition_t>& column_definitions() const;
        const std::vector<table::table_constraint_t>& constraints() const;

        bool is_disk_storage() const { return disk_storage_; }

        // True when this CREATE TABLE asked for the sparse (per-field side-table)
        // storage layout — set only by the SQL parser when the user writes
        // `CREATE TABLE t()` (no columns). Legacy schemaless-collection paths
        // (wrapper API with empty column_definitions) leave this false and keep
        // the old single dynamic-table behavior.
        bool is_dynamic_schema_table() const { return dynamic_schema_table_; }
        void set_dynamic_schema_table(bool v) { dynamic_schema_table_ = v; }

    private:
        hash_t hash_impl() const override;
        std::string to_string_impl() const override;

        std::vector<table::column_definition_t> column_definitions_;
        std::vector<table::table_constraint_t> constraints_;
        bool disk_storage_{false};
        bool dynamic_schema_table_{false};
    };

    using node_create_collection_ptr = boost::intrusive_ptr<node_create_collection_t>;
    node_create_collection_ptr make_node_create_collection(std::pmr::memory_resource* resource,
                                                           const collection_full_name_t& collection);

    node_create_collection_ptr make_node_create_collection(std::pmr::memory_resource* resource,
                                                           const collection_full_name_t& collection,
                                                           std::vector<table::column_definition_t> column_definitions,
                                                           std::vector<table::table_constraint_t> constraints,
                                                           bool disk_storage = false);

} // namespace components::logical_plan
