#pragma once

#include "json_path_extractor.hpp"
#include <functional>
#include <components/document/document.hpp>
#include <components/document/document_id.hpp>
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

    // Hash function for document_id_t
    struct document_id_hash_t {
        std::size_t operator()(const document::document_id_t& id) const {
            return std::hash<std::string_view>{}(std::string_view(reinterpret_cast<const char*>(id.data()), id.size));
        }
    };

    class document_table_storage_t {
    public:
        explicit document_table_storage_t(std::pmr::memory_resource* resource,
                                          table::storage::block_manager_t& block_manager);

        // Insert document
        void insert(const document::document_id_t& id, const document::document_ptr& doc);

        // Batch insert documents
        void batch_insert(const std::pmr::vector<std::pair<document::document_id_t, document::document_ptr>>& documents);

        // Get document by ID
        document::document_ptr get(const document::document_id_t& id);

        // Remove document
        void remove(const document::document_id_t& id);

        // Check existence
        bool contains(const document::document_id_t& id) const;

        // Scan table
        void scan(vector::data_chunk_t& output, table::table_scan_state& state);

        // Initialize scan
        void initialize_scan(table::table_scan_state& state,
                             const std::vector<table::storage_index_t>& column_ids,
                             const table::table_filter_t* filter = nullptr);

        // Column management (replaces dynamic_schema)
        bool has_column(const std::string& json_path) const;
        const column_info_t* get_column_info(const std::string& json_path) const;
        const column_info_t* get_column_by_index(size_t index) const;
        const std::pmr::vector<column_info_t>& columns() const { return columns_; }
        size_t column_count() const { return columns_.size(); }
        std::vector<table::column_definition_t> to_column_definitions() const;
        json_path_extractor_t& extractor() { return *extractor_; }
        const json_path_extractor_t& extractor() const { return *extractor_; }

        // Access to table
        table::data_table_t* table() { return table_.get(); }
        const table::data_table_t* table() const { return table_.get(); }

        // Document count
        size_t size() const { return id_to_row_.size(); }

        // Get row_id by document_id
        bool get_row_id(const document::document_id_t& id, size_t& row_id) const;

    private:
        // Add new column
        void add_column(const std::string& json_path,
                        const types::complex_logical_type& type,
                        bool is_array_element = false,
                        size_t array_index = 0);

        // Evolve schema from document - adds new columns
        std::pmr::vector<column_info_t> evolve_from_document(const document::document_ptr& doc);

        // Evolve schema (incremental column addition)
        void evolve_schema(const std::pmr::vector<column_info_t>& new_columns);

        // Convert document to row
        vector::data_chunk_t document_to_row(const document::document_ptr& doc);

        // Convert row back to document
        document::document_ptr row_to_document(const vector::data_chunk_t& row, size_t row_idx);

        // Helper methods
        types::logical_value_t extract_value_from_document(const document::document_ptr& doc,
                                                           const std::string& json_path,
                                                           types::logical_type expected_type);

        types::logical_type detect_value_type_in_document(const document::document_ptr& doc,
                                                          const std::string& json_path);

        std::pmr::unordered_map<std::string, types::logical_value_t>
        extract_path_values(const document::document_ptr& doc);

        std::pmr::memory_resource* resource_;
        table::storage::block_manager_t& block_manager_;

        // Column storage (inlined from dynamic_schema)
        std::pmr::vector<column_info_t> columns_;
        std::pmr::unordered_map<std::string, size_t> path_to_index_;
        std::unique_ptr<json_path_extractor_t> extractor_;

        // Data table
        std::unique_ptr<table::data_table_t> table_;

        // Document ID -> row ID mapping
        std::pmr::unordered_map<document::document_id_t,
                                size_t,
                                document_id_hash_t,
                                std::equal_to<document::document_id_t>>
            id_to_row_;

        // Next row ID
        size_t next_row_id_;
    };

} // namespace components::document_table
