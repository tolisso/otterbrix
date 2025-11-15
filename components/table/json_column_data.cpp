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

        // Create child_states for validity
        if (state.child_states.empty()) {
            state.child_states.emplace_back();
        }

        assert(state.child_states.size() == 1);
        validity.initialize_scan(state.child_states[0]);
    }

    void json_column_data_t::initialize_scan_with_offset(column_scan_state& state, uint64_t row_idx) {
        column_data_t::initialize_scan_with_offset(state, row_idx);

        // Create child_states for validity
        if (state.child_states.empty()) {
            state.child_states.emplace_back();
        }

        assert(state.child_states.size() == 1);
        validity.initialize_scan_with_offset(state.child_states[0], row_idx);
    }

    uint64_t json_column_data_t::scan(uint64_t vector_index,
                                      column_scan_state& state,
                                      vector::vector_t& result,
                                      uint64_t target_count) {
        assert(state.row_index == state.child_states[0].row_index);

        // FULL IMPLEMENTATION: json_id → JSON string conversion
        // This implementation:
        // 1. Scans json_ids from base column into temporary INT64 vector
        // 2. For each json_id, queries auxiliary_data_
        // 3. Reconstructs JSON string from fields
        // 4. Sets JSON strings in result vector

        // Create temporary INT64 vector to receive json_ids from base column
        vector::vector_t temp_vector(resource_,
                                     types::complex_logical_type(types::logical_type::BIGINT),
                                     target_count);

        // Scan json_ids from base column
        auto scan_count = column_data_t::scan(vector_index, state, temp_vector, target_count);

        // Scan validity FIRST before setting values
        validity.scan(vector_index, state.child_states[0], result, target_count);

        // Convert json_ids to JSON strings
        for (uint64_t i = 0; i < scan_count; i++) {
            // Get the json_id value
            auto val = temp_vector.value(i);

            if (val.is_null()) {
                // NULL value - set null in result
                result.set_value(i, types::logical_value_t{});
                continue;
            }

            // Extract json_id (stored as INT64)
            int64_t json_id = val.value<int64_t>();

            // Reconstruct JSON string from auxiliary table
            std::string json_str = read_json(json_id);

            // Set JSON string in result vector
            result.set_value(i, types::logical_value_t{json_str});
        }

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
        // FULL IMPLEMENTATION: STRING → json_id conversion
        // This implementation:
        // 1. Extracts JSON strings from uvf (as string_view)
        // 2. Parses each JSON string
        // 3. Generates json_id for each
        // 4. Stores parsed fields in auxiliary_data_
        // 5. Creates new uvf with json_ids
        // 6. Passes json_id uvf to column_data_t::append_data

        // Extract string data from unified format
        // Input data contains JSON strings as std::string_view
        auto string_data = uvf.get_data<std::string_view>();

        // Allocate memory for json_ids using unique_ptr (lifetime managed)
        auto json_ids = std::unique_ptr<int64_t[]>(new int64_t[count]);

        // Process each JSON string
        for (uint64_t i = 0; i < count; i++) {
            auto idx = uvf.referenced_indexing->get_index(i);

            // Check if this value is NULL
            if (!uvf.validity.row_is_valid(idx)) {
                json_ids[i] = 0; // NULL represented as 0
                continue;
            }

            // Get the JSON string
            std::string json_string(string_data[idx]);

            // Generate new json_id (thread-safe atomic increment)
            int64_t json_id = next_json_id_.fetch_add(1);

            // Parse JSON and extract fields
            auto fields = parse_simple_json(json_string);

            // Store parsed fields in auxiliary table (thread-safe)
            insert_into_auxiliary_table(json_id, fields);

            // Store json_id for this row
            json_ids[i] = json_id;
        }

        // Create new unified_vector_format for INT64 json_ids
        vector::unified_vector_format json_id_format(resource_, count);
        json_id_format.referenced_indexing = uvf.referenced_indexing;
        json_id_format.validity = uvf.validity;
        json_id_format.data = reinterpret_cast<std::byte*>(json_ids.get());

        // Append json_ids (as INT64) to base column
        // Note: column_data_t::append_data synchronously copies the data,
        // so json_ids unique_ptr can be destroyed after this call
        column_data_t::append_data(state, json_id_format, count);
        validity.append_data(state.child_appends[0], uvf, count);
    }

    void json_column_data_t::revert_append(int64_t start_row) {
        // Read json_ids from base column before reverting
        // We need to read them BEFORE calling column_data_t::revert_append()
        // because that will delete the data

        uint64_t current_count = count_.load();
        if (start_row >= static_cast<int64_t>(current_count)) {
            // Nothing to revert
            column_data_t::revert_append(start_row);
            validity.revert_append(start_row);
            return;
        }

        uint64_t rows_to_revert = current_count - static_cast<uint64_t>(start_row);

        if (rows_to_revert > 0) {
            // Create temporary vector to read json_ids
            vector::vector_t temp_vector(resource_,
                types::complex_logical_type(types::logical_type::BIGINT),
                rows_to_revert);

            // Scan json_ids from base column starting at start_row
            column_scan_state scan_state;
            initialize_scan(scan_state);

            // Skip to start_row
            scan_state.row_index = static_cast<uint64_t>(start_row);

            // Read the json_ids
            auto scanned = column_data_t::scan(0, scan_state, temp_vector, rows_to_revert);

            // Delete auxiliary data for all json_ids being reverted
            {
                std::lock_guard<std::mutex> lock(auxiliary_data_mutex_);

                for (uint64_t i = 0; i < scanned; i++) {
                    auto val = temp_vector.value(i);
                    if (!val.is_null()) {
                        int64_t json_id = *val.value<int64_t*>();
                        if (json_id != 0) { // Skip NULL entries
                            auxiliary_data_.erase(json_id);
                        }
                    }
                }
            }
        }

        // Revert the base column data and validity
        column_data_t::revert_append(start_row);
        validity.revert_append(start_row);

        // In production with real tables, this would be:
        // DELETE FROM auxiliary_table WHERE json_id IN (
        //     SELECT json_id FROM main_table WHERE row_id >= start_row
        // )
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
        // Update auxiliary table entries when updating JSON values
        // Strategy: For each updated row:
        // 1. Get the new JSON string from update_vector
        // 2. Parse it into fields
        // 3. Generate a new json_id
        // 4. Insert into auxiliary table
        // 5. Update main column with new json_id

        // Create unified format from update_vector (contains JSON strings)
        vector::unified_vector_format uvf(resource_, update_count);
        update_vector.to_unified_format(update_count, uvf);

        auto string_data = uvf.get_data<std::string_view>();

        // Allocate memory for new json_ids
        auto json_ids = std::unique_ptr<int64_t[]>(new int64_t[update_count]);

        // For each row being updated
        for (uint64_t i = 0; i < update_count; i++) {
            auto idx = uvf.referenced_indexing->get_index(i);

            // Handle NULL values
            if (!uvf.validity.row_is_valid(idx)) {
                json_ids[i] = 0; // NULL represented as 0
                continue;
            }

            // Get the new JSON string
            std::string json_string(string_data[idx]);

            // Parse the new JSON
            auto new_fields = parse_simple_json(json_string);

            // Generate new json_id
            int64_t new_json_id = next_json_id_.fetch_add(1);

            // Insert new data into auxiliary table
            insert_into_auxiliary_table(new_json_id, new_fields);

            json_ids[i] = new_json_id;

            // Note: In production, we would also:
            // DELETE FROM auxiliary_table WHERE json_id = old_json_id
            // to clean up old entries
        }

        // Create a new vector with json_ids (INT64) instead of JSON strings
        vector::vector_t json_id_vector(resource_,
                                        types::complex_logical_type(types::logical_type::BIGINT),
                                        update_count);

        // Copy json_ids into the vector
        for (uint64_t i = 0; i < update_count; i++) {
            if (uvf.validity.row_is_valid(uvf.referenced_indexing->get_index(i))) {
                json_id_vector.set_value(i, types::logical_value_t{json_ids[i]});
            } else {
                json_id_vector.set_value(i, types::logical_value_t{});
            }
        }

        // Update the base column with json_ids (INT64)
        column_data_t::update(column_index, json_id_vector, row_ids, update_count);
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
        // Thread-safe insertion into in-memory storage
        std::lock_guard<std::mutex> lock(auxiliary_data_mutex_);
        auxiliary_data_[json_id] = fields;

        // TODO: In production, this would execute:
        // for (const auto& [key, value] : fields) {
        //     INSERT INTO auxiliary_table_name_ (json_id, full_path, int_value)
        //     VALUES (json_id, key, value);
        // }
    }

    std::map<std::string, int64_t> json_column_data_t::query_auxiliary_table(int64_t json_id) {
        // Thread-safe query from in-memory storage
        std::lock_guard<std::mutex> lock(auxiliary_data_mutex_);

        auto it = auxiliary_data_.find(json_id);
        if (it != auxiliary_data_.end()) {
            return it->second;
        }

        // TODO: In production, this would execute:
        // SELECT full_path, int_value
        // FROM auxiliary_table_name_
        // WHERE json_id = ?

        return std::map<std::string, int64_t>();
    }

} // namespace components::table
