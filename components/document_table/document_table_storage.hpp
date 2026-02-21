#pragma once

#include "json_path_extractor.hpp"
#include <functional>
#include <components/document/document.hpp>
#include <components/table/data_table.hpp>
#include <components/table/column_definition.hpp>
#include <components/table/storage/in_memory_block_manager.hpp>
#include <components/vector/data_chunk.hpp>
#include <memory>
#include <memory_resource>
#include <unordered_map>

namespace components::document_table {

    // Column information - simple struct for storing column metadata
    struct column_info_t {
        std::string json_path;               // JSON path (e.g., "user.address.city")
        types::complex_logical_type type;    // Column type
        size_t column_index = 0;             // Column index in table
        bool is_array_element = false;       // Is array element?
        size_t array_index = 0;              // Array index
    };

    class document_table_storage_t {
    public:
        explicit document_table_storage_t(std::pmr::memory_resource* resource,
                                          table::storage::block_manager_t& block_manager);

        // Prepare insert: evolve schema + convert documents to data_chunk.
        // Does NOT append to data_table (executor does table()->append() via operator_insert).
        vector::data_chunk_t prepare_insert(
            const std::pmr::vector<std::pair<document::document_id_t, document::document_ptr>>& documents);

        // Scan table
        void scan(vector::data_chunk_t& output, table::table_scan_state& state);

        // Initialize scan
        void initialize_scan(table::table_scan_state& state,
                             const std::vector<table::storage_index_t>& column_ids,
                             const table::table_filter_t* filter = nullptr);

        // Column management
        bool has_column(const std::string& json_path) const;
        const column_info_t* get_column_info(const std::string& json_path) const;
        const column_info_t* get_column_by_index(size_t index) const;
        const std::pmr::vector<column_info_t>& columns() const { return columns_; }
        size_t column_count() const { return columns_.size(); }
        std::vector<table::column_definition_t> to_column_definitions() const;
        json_path_extractor_t& extractor() { return *extractor_; }
        const json_path_extractor_t& extractor() const { return *extractor_; }

        // Access to underlying data table
        table::data_table_t* table() { return table_.get(); }
        const table::data_table_t* table() const { return table_.get(); }

        // Evolve schema from data_chunk column types (for SQL INSERT VALUES path)
        void evolve_schema_from_types(const std::pmr::vector<types::complex_logical_type>& types);

        // Row count
        size_t size() const { return table_ ? table_->row_group()->total_rows() : 0; }

    private:
        void add_column(const std::string& json_path,
                        const types::complex_logical_type& type,
                        bool is_array_element = false,
                        size_t array_index = 0);

        std::pmr::vector<column_info_t> evolve_from_document(const document::document_ptr& doc);
        void evolve_schema(const std::pmr::vector<column_info_t>& new_columns);
        vector::data_chunk_t document_to_row(const document::document_ptr& doc);

        types::logical_value_t extract_value_from_document(const document::document_ptr& doc,
                                                           const std::string& json_path,
                                                           types::logical_type expected_type);

        types::logical_type detect_value_type_in_document(const document::document_ptr& doc,
                                                          const std::string& json_path);

        std::pmr::unordered_map<std::string, types::logical_value_t>
        extract_path_values(const document::document_ptr& doc);

        std::pmr::memory_resource* resource_;
        table::storage::block_manager_t& block_manager_;

        std::pmr::vector<column_info_t> columns_;
        std::pmr::unordered_map<std::string, size_t> path_to_index_;
        std::unique_ptr<json_path_extractor_t> extractor_;
        std::unique_ptr<table::data_table_t> table_;
    };

} // namespace components::document_table
