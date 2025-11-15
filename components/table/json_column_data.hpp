#pragma once
#include "column_data.hpp"
#include "validity_column_data.hpp"
#include <string>
#include <map>
#include <unordered_map>
#include <mutex>

namespace components::table {

    /**
     * @brief Column data implementation for JSON type
     *
     * This class manages JSON data by storing it in an auxiliary table
     * in a decomposed format. Each JSON object is assigned a unique json_id,
     * and its key-value pairs are stored as separate rows in the auxiliary table.
     *
     * Limitations of the prototype:
     * - Only simple JSON objects (no nested objects)
     * - Only INTEGER values
     * - Only read() operation is implemented
     */
    class json_column_data_t : public column_data_t {
    public:
        /**
         * @brief Constructs a JSON column data instance
         *
         * @param resource Memory resource for allocations
         * @param block_manager Block manager for storage operations
         * @param column_index Index of this column in the table
         * @param start_row Starting row number
         * @param type The complex logical type (must be JSON type)
         * @param parent Parent column (if any)
         */
        json_column_data_t(std::pmr::memory_resource* resource,
                          storage::block_manager_t& block_manager,
                          uint64_t column_index,
                          uint64_t start_row,
                          types::complex_logical_type type,
                          column_data_t* parent = nullptr);

        // Validity tracking for NULL values
        validity_column_data_t validity;

        // Bring base class overloads into scope to avoid hiding them
        using column_data_t::scan;
        using column_data_t::scan_committed;

        // Override column_data_t methods
        void set_start(uint64_t new_start) override;

        scan_vector_type
        get_vector_scan_type(column_scan_state& state, uint64_t scan_count, vector::vector_t& result) override;

        void initialize_scan(column_scan_state& state) override;
        void initialize_scan_with_offset(column_scan_state& state, uint64_t row_idx) override;

        uint64_t
        scan(uint64_t vector_index, column_scan_state& state, vector::vector_t& result, uint64_t target_count) override;

        uint64_t scan_committed(uint64_t vector_index,
                                column_scan_state& state,
                                vector::vector_t& result,
                                bool allow_updates,
                                uint64_t target_count) override;

        uint64_t scan_count(column_scan_state& state, vector::vector_t& result, uint64_t count) override;

        void initialize_append(column_append_state& state) override;
        void append_data(column_append_state& state, vector::unified_vector_format& uvf, uint64_t count) override;
        void revert_append(int64_t start_row) override;

        uint64_t fetch(column_scan_state& state, int64_t row_id, vector::vector_t& result) override;
        void
        fetch_row(column_fetch_state& state, int64_t row_id, vector::vector_t& result, uint64_t result_idx) override;

        void update(uint64_t column_index,
                    vector::vector_t& update_vector,
                    int64_t* row_ids,
                    uint64_t update_count) override;

        void update_column(const std::vector<uint64_t>& column_path,
                           vector::vector_t& update_vector,
                           int64_t* row_ids,
                           uint64_t update_count,
                           uint64_t depth) override;

        void get_column_segment_info(uint64_t row_group_index,
                                     std::vector<uint64_t> col_path,
                                     std::vector<column_segment_info>& result) override;

        /**
         * @brief Reads and reconstructs JSON from auxiliary table
         *
         * @param json_id The unique identifier of the JSON object
         * @return std::string The reconstructed JSON string
         *
         * Example: For json_id=1, if auxiliary table contains:
         *   (1, 1, "age", 25)
         *   (2, 1, "score", 100)
         * Returns: {"age": 25, "score": 100}
         */
        std::string read_json(int64_t json_id);

        // Public interface for testing
        // These methods are exposed for unit testing purposes
        std::map<std::string, int64_t> parse_simple_json_for_test(const std::string& json_string) {
            return parse_simple_json(json_string);
        }

        void insert_into_auxiliary_table_for_test(int64_t json_id, const std::map<std::string, int64_t>& fields) {
            insert_into_auxiliary_table(json_id, fields);
        }

        std::map<std::string, int64_t> query_auxiliary_table_for_test(int64_t json_id) {
            return query_auxiliary_table(json_id);
        }

    private:
        /**
         * @brief Parses simple JSON string into key-value pairs
         *
         * @param json_string JSON string in format {"key1": value1, "key2": value2}
         * @return std::map<std::string, int64_t> Parsed key-value pairs
         *
         * Note: Only supports simple flat objects with integer values
         */
        std::map<std::string, int64_t> parse_simple_json(const std::string& json_string);

        /**
         * @brief Inserts JSON data into auxiliary table
         *
         * @param json_id The unique identifier for this JSON object
         * @param fields The key-value pairs to insert
         *
         * For each field, creates a row in auxiliary table:
         * INSERT INTO auxiliary_table (json_id, full_path, int_value)
         * VALUES (json_id, key, value)
         */
        void insert_into_auxiliary_table(int64_t json_id, const std::map<std::string, int64_t>& fields);

        /**
         * @brief Queries auxiliary table for JSON reconstruction
         *
         * @param json_id The unique identifier of the JSON object
         * @return std::map<std::string, int64_t> Retrieved key-value pairs
         *
         * Executes: SELECT full_path, int_value FROM auxiliary_table WHERE json_id = ?
         */
        std::map<std::string, int64_t> query_auxiliary_table(int64_t json_id);

        // Name of the auxiliary table storing JSON data
        // Format: __json_<table_name>_<column_name>
        std::string auxiliary_table_name_;

        // Counter for generating unique json_id values
        std::atomic<int64_t> next_json_id_;

        // In-memory storage for auxiliary table data (prototype implementation)
        // Maps json_id -> (key -> value)
        // In production, this would be replaced with actual table storage
        std::unordered_map<int64_t, std::map<std::string, int64_t>> auxiliary_data_;

        // Mutex to protect auxiliary_data_ from concurrent access
        mutable std::mutex auxiliary_data_mutex_;
    };

} // namespace components::table
