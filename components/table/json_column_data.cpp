#include "json_column_data.hpp"
#include <components/types/types.hpp>
#include <sstream>
#include <algorithm>
#include <cctype>

namespace components::table {

    json_column_data_t::json_column_data_t(std::pmr::memory_resource* resource,
                                           storage::block_manager_t& block_manager,
                                           uint64_t column_index,
                                           uint64_t start_row,
                                           types::complex_logical_type type,
                                           column_data_t* parent)
        : column_data_t(resource, block_manager, column_index, start_row, std::move(type), parent)
        , validity(resource, block_manager, 0, start_row, *this)
        , next_json_id_(1) {

        // Extract auxiliary table name from the type extension
        if (type_.type() == types::logical_type::JSON) {
            auto* json_ext = static_cast<types::json_logical_type_extension*>(type_.extension());
            if (json_ext) {
                auxiliary_table_name_ = json_ext->auxiliary_table_name();
            }
        }
    }

    void json_column_data_t::set_start(uint64_t new_start) {
        column_data_t::set_start(new_start);
        validity.set_start(new_start);
    }

    scan_vector_type json_column_data_t::get_vector_scan_type(column_scan_state& state,
                                                               uint64_t scan_count,
                                                               vector::vector_t& result) {
        auto scan_type = column_data_t::get_vector_scan_type(state, scan_count, result);
        if (scan_type == scan_vector_type::SCAN_FLAT_VECTOR) {
            return scan_vector_type::SCAN_FLAT_VECTOR;
        }
        if (state.child_states.empty()) {
            return scan_type;
        }
        return validity.get_vector_scan_type(state.child_states[0], scan_count, result);
    }

    void json_column_data_t::initialize_scan(column_scan_state& state) {
        column_data_t::initialize_scan(state);

        assert(state.child_states.size() == 1);
        validity.initialize_scan(state.child_states[0]);
    }

    void json_column_data_t::initialize_scan_with_offset(column_scan_state& state, uint64_t row_idx) {
        column_data_t::initialize_scan_with_offset(state, row_idx);

        assert(state.child_states.size() == 1);
        validity.initialize_scan_with_offset(state.child_states[0], row_idx);
    }

    uint64_t json_column_data_t::scan(uint64_t vector_index,
                                      column_scan_state& state,
                                      vector::vector_t& result,
                                      uint64_t target_count) {
        assert(state.row_index == state.child_states[0].row_index);

        // First, scan the json_id values (stored as INT64)
        auto scan_count = column_data_t::scan(vector_index, state, result, target_count);

        // TODO: For each json_id, reconstruct the JSON string
        // This will be implemented when we have full table infrastructure
        // For now, we just scan the json_id values

        validity.scan(vector_index, state.child_states[0], result, target_count);
        return scan_count;
    }

    uint64_t json_column_data_t::scan_committed(uint64_t vector_index,
                                                column_scan_state& state,
                                                vector::vector_t& result,
                                                bool allow_updates,
                                                uint64_t target_count) {
        assert(state.row_index == state.child_states[0].row_index);
        auto scan_count = column_data_t::scan_committed(vector_index, state, result, allow_updates, target_count);
        validity.scan_committed(vector_index, state.child_states[0], result, allow_updates, target_count);
        return scan_count;
    }

    uint64_t json_column_data_t::scan_count(column_scan_state& state, vector::vector_t& result, uint64_t count) {
        auto scan_count = column_data_t::scan_count(state, result, count);
        validity.scan_count(state.child_states[0], result, count);
        return scan_count;
    }

    void json_column_data_t::initialize_append(column_append_state& state) {
        column_data_t::initialize_append(state);
        column_append_state child_append;
        validity.initialize_append(child_append);
        state.child_appends.push_back(std::move(child_append));
    }

    void json_column_data_t::append_data(column_append_state& state,
                                         vector::unified_vector_format& uvf,
                                         uint64_t count) {
        // TODO: Parse JSON strings and insert into auxiliary table
        // For the prototype, we would:
        // 1. For each JSON string in the vector
        // 2. Generate a new json_id
        // 3. Parse the JSON string
        // 4. Insert key-value pairs into auxiliary table
        // 5. Store json_id in the main column

        column_data_t::append_data(state, uvf, count);
        validity.append_data(state.child_appends[0], uvf, count);
    }

    void json_column_data_t::revert_append(int64_t start_row) {
        column_data_t::revert_append(start_row);
        validity.revert_append(start_row);
        // TODO: Also revert entries in auxiliary table
    }

    uint64_t json_column_data_t::fetch(column_scan_state& state, int64_t row_id, vector::vector_t& result) {
        if (state.child_states.empty()) {
            column_scan_state child_state;
            state.child_states.push_back(std::move(child_state));
        }
        auto scan_count = column_data_t::fetch(state, row_id, result);
        validity.fetch(state.child_states[0], row_id, result);
        return scan_count;
    }

    void json_column_data_t::update(uint64_t column_index,
                                    vector::vector_t& update_vector,
                                    int64_t* row_ids,
                                    uint64_t update_count) {
        // TODO: Update auxiliary table entries when updating JSON values
        column_data_t::update(column_index, update_vector, row_ids, update_count);
        validity.update(column_index, update_vector, row_ids, update_count);
    }

    void json_column_data_t::update_column(const std::vector<uint64_t>& column_path,
                                           vector::vector_t& update_vector,
                                           int64_t* row_ids,
                                           uint64_t update_count,
                                           uint64_t depth) {
        if (depth >= column_path.size()) {
            column_data_t::update(column_path[0], update_vector, row_ids, update_count);
        } else {
            validity.update_column(column_path, update_vector, row_ids, update_count, depth + 1);
        }
    }

    void json_column_data_t::fetch_row(column_fetch_state& state,
                                       int64_t row_id,
                                       vector::vector_t& result,
                                       uint64_t result_idx) {
        if (state.child_states.empty()) {
            auto child_state = std::make_unique<column_fetch_state>();
            state.child_states.push_back(std::move(child_state));
        }
        validity.fetch_row(*state.child_states[0], row_id, result, result_idx);
        column_data_t::fetch_row(state, row_id, result, result_idx);
    }

    void json_column_data_t::get_column_segment_info(uint64_t row_group_index,
                                                     std::vector<uint64_t> col_path,
                                                     std::vector<column_segment_info>& result) {
        column_data_t::get_column_segment_info(row_group_index, col_path, result);
        col_path.push_back(0);
        validity.get_column_segment_info(row_group_index, std::move(col_path), result);
    }

    // ============================================================================
    // JSON-specific methods
    // ============================================================================

    std::string json_column_data_t::read_json(int64_t json_id) {
        // Query auxiliary table for all fields with this json_id
        auto fields = query_auxiliary_table(json_id);

        // Assemble JSON string
        std::ostringstream oss;
        oss << "{";

        bool first = true;
        for (const auto& [key, value] : fields) {
            if (!first) {
                oss << ", ";
            }
            oss << "\"" << key << "\": " << value;
            first = false;
        }

        oss << "}";
        return oss.str();
    }

    std::map<std::string, int64_t> json_column_data_t::parse_simple_json(const std::string& json_string) {
        std::map<std::string, int64_t> result;

        // Remove whitespace
        std::string cleaned;
        for (char c : json_string) {
            if (!std::isspace(static_cast<unsigned char>(c))) {
                cleaned += c;
            }
        }

        // Check for valid JSON object format: {...}
        if (cleaned.empty() || cleaned.front() != '{' || cleaned.back() != '}') {
            return result; // Empty result for invalid JSON
        }

        // Remove braces
        cleaned = cleaned.substr(1, cleaned.size() - 2);

        if (cleaned.empty()) {
            return result; // Empty JSON object
        }

        // Split by comma to get key-value pairs
        std::istringstream iss(cleaned);
        std::string pair;

        size_t pos = 0;
        while (pos < cleaned.size()) {
            // Find the key (quoted string)
            size_t key_start = cleaned.find('"', pos);
            if (key_start == std::string::npos) break;

            size_t key_end = cleaned.find('"', key_start + 1);
            if (key_end == std::string::npos) break;

            std::string key = cleaned.substr(key_start + 1, key_end - key_start - 1);

            // Find the colon
            size_t colon_pos = cleaned.find(':', key_end);
            if (colon_pos == std::string::npos) break;

            // Find the value (number)
            size_t value_start = colon_pos + 1;
            size_t value_end = cleaned.find(',', value_start);
            if (value_end == std::string::npos) {
                value_end = cleaned.size();
            }

            std::string value_str = cleaned.substr(value_start, value_end - value_start);

            // Parse the integer value
            try {
                int64_t value = std::stoll(value_str);
                result[key] = value;
            } catch (const std::exception&) {
                // Skip invalid values
            }

            pos = value_end + 1;
        }

        return result;
    }

    void json_column_data_t::insert_into_auxiliary_table(int64_t json_id,
                                                         const std::map<std::string, int64_t>& fields) {
        // TODO: Implement actual table insertion when table infrastructure is ready
        // This would execute something like:
        //
        // for (const auto& [key, value] : fields) {
        //     INSERT INTO auxiliary_table_name_ (json_id, full_path, int_value)
        //     VALUES (json_id, key, value);
        // }
        //
        // For now, this is a placeholder
    }

    std::map<std::string, int64_t> json_column_data_t::query_auxiliary_table(int64_t json_id) {
        // TODO: Implement actual table query when table infrastructure is ready
        // This would execute:
        //
        // SELECT full_path, int_value
        // FROM auxiliary_table_name_
        // WHERE json_id = ?
        //
        // For now, return empty map as placeholder
        return std::map<std::string, int64_t>();
    }

} // namespace components::table
