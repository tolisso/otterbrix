#include "document_table_storage.hpp"
#include <components/table/table_state.hpp>

namespace components::document_table {

    namespace {
        // Convert SQL-safe column name back to document API path
        std::string column_name_to_document_path(const std::string& column_name) {
            std::string result = "/";
            size_t i = 0;
            while (i < column_name.size()) {
                if (i + 5 <= column_name.size() && column_name.substr(i, 5) == "_dot_") {
                    result += '/';
                    i += 5;
                } else if (i + 4 <= column_name.size() &&
                         column_name.substr(i, 4) == "_arr" &&
                         i + 4 < column_name.size() &&
                         std::isdigit(column_name[i + 4])) {
                    result += '[';
                    i += 4;
                    while (i < column_name.size() && std::isdigit(column_name[i])) {
                        result += column_name[i];
                        i++;
                    }
                    result += ']';
                    if (i < column_name.size() && column_name[i] == '_') {
                        i++;
                    }
                } else {
                    result += column_name[i];
                    i++;
                }
            }
            return result;
        }
    } // anonymous namespace

    document_table_storage_t::document_table_storage_t(std::pmr::memory_resource* resource,
                                                       table::storage::block_manager_t& block_manager)
        : resource_(resource)
        , block_manager_(block_manager)
        , columns_(resource)
        , path_to_index_(resource)
        , extractor_(std::make_unique<json_path_extractor_t>(resource))
        , id_to_row_(10, document_id_hash_t{}, std::equal_to<document::document_id_t>{}, resource)
        , next_row_id_(0) {

        // Add _id column
        types::complex_logical_type id_type(types::logical_type::STRING_LITERAL);
        id_type.set_alias("_id");
        add_column("_id", id_type);

        // Create initial table with _id column
        auto column_defs = to_column_definitions();
        table_ = std::make_unique<table::data_table_t>(resource_, block_manager_, std::move(column_defs));
    }

    // Column management methods
    bool document_table_storage_t::has_column(const std::string& json_path) const {
        return path_to_index_.find(json_path) != path_to_index_.end();
    }

    const column_info_t* document_table_storage_t::get_column_info(const std::string& json_path) const {
        auto it = path_to_index_.find(json_path);
        if (it == path_to_index_.end()) {
            return nullptr;
        }
        return &columns_[it->second];
    }

    const column_info_t* document_table_storage_t::get_column_by_index(size_t index) const {
        if (index >= columns_.size()) {
            return nullptr;
        }
        return &columns_[index];
    }

    void document_table_storage_t::add_column(const std::string& json_path,
                                              const types::complex_logical_type& type,
                                              bool is_array_element,
                                              size_t array_index) {
        if (has_column(json_path)) {
            return;
        }

        size_t new_index = columns_.size();
        column_info_t new_col;
        new_col.json_path = json_path;
        new_col.type = type;
        new_col.column_index = new_index;
        new_col.is_array_element = is_array_element;
        new_col.array_index = array_index;

        columns_.push_back(std::move(new_col));
        path_to_index_[json_path] = new_index;
    }

    std::vector<table::column_definition_t> document_table_storage_t::to_column_definitions() const {
        std::vector<table::column_definition_t> result;
        result.reserve(columns_.size());

        for (const auto& col : columns_) {
            auto col_type = col.type;
            col_type.set_alias(col.json_path);
            result.emplace_back(col.json_path, std::move(col_type));
        }

        return result;
    }

    std::pmr::vector<column_info_t> document_table_storage_t::evolve_from_document(const document::document_ptr& doc) {
        std::pmr::vector<column_info_t> new_columns(resource_);

        if (!doc || !doc->is_valid()) {
            return new_columns;
        }

        auto extracted_paths = extractor_->extract_paths(doc);

        for (const auto& path_info : extracted_paths) {
            // Skip existing columns (type is checked in dispatcher)
            if (has_column(path_info.path)) {
                continue;
            }

            types::complex_logical_type col_type(path_info.type);
            col_type.set_alias(path_info.path);

            add_column(path_info.path, col_type, path_info.is_array, path_info.array_index);
            new_columns.push_back(columns_.back());
        }

        return new_columns;
    }

    void document_table_storage_t::insert(const document::document_id_t& id, const document::document_ptr& doc) {
        if (!doc || !doc->is_valid()) {
            return;
        }

        auto new_columns = evolve_from_document(doc);
        if (!new_columns.empty()) {
            evolve_schema(new_columns);
        }

        auto row = document_to_row(doc);

        table::table_append_state state(resource_);
        table_->append_lock(state);
        table_->initialize_append(state);
        table_->append(row, state);
        table_->finalize_append(state);

        id_to_row_[id] = next_row_id_++;
    }

    document::document_ptr document_table_storage_t::get(const document::document_id_t& id) {
        return nullptr;
    }

    void document_table_storage_t::remove(const document::document_id_t& id) {
        auto it = id_to_row_.find(id);
        if (it == id_to_row_.end()) {
            return;
        }

        size_t row_id = it->second;

        vector::vector_t row_ids(resource_, types::logical_type::UBIGINT);
        row_ids.set_value(0, types::logical_value_t(static_cast<uint64_t>(row_id)));

        auto delete_state = table_->initialize_delete({});
        table_->delete_rows(*delete_state, row_ids, 1);

        id_to_row_.erase(it);
    }

    bool document_table_storage_t::contains(const document::document_id_t& id) const {
        return id_to_row_.find(id) != id_to_row_.end();
    }

    void document_table_storage_t::scan(vector::data_chunk_t& output, table::table_scan_state& state) {
        table_->scan(output, state);
    }

    void document_table_storage_t::initialize_scan(table::table_scan_state& state,
                                                    const std::vector<table::storage_index_t>& column_ids,
                                                    const table::table_filter_t* filter) {
        table_->initialize_scan(state, column_ids, filter);
    }

    bool document_table_storage_t::get_row_id(const document::document_id_t& id, size_t& row_id) const {
        auto it = id_to_row_.find(id);
        if (it == id_to_row_.end()) {
            return false;
        }
        row_id = it->second;
        return true;
    }

    void document_table_storage_t::evolve_schema(const std::pmr::vector<column_info_t>& new_columns) {
        for (const auto& col_info : new_columns) {
            auto default_value = std::make_unique<types::logical_value_t>(col_info.type);
            table::column_definition_t new_column_def(col_info.json_path, col_info.type, std::move(default_value));
            auto new_table = std::make_unique<table::data_table_t>(*table_, new_column_def);
            table_ = std::move(new_table);
        }
    }

    vector::data_chunk_t document_table_storage_t::document_to_row(const document::document_ptr& doc) {
        auto types = table_->copy_types();
        vector::data_chunk_t chunk(resource_, types);
        chunk.set_cardinality(1);

        for (size_t i = 0; i < column_count(); ++i) {
            const auto* col_info = get_column_by_index(i);
            auto& vec = chunk.data[i];

            if (col_info->json_path == "_id") {
                auto doc_id = document::get_document_id(doc);
                std::string id_str(reinterpret_cast<const char*>(doc_id.data()), doc_id.size);
                vec.set_value(0, types::logical_value_t(id_str));
                continue;
            }

            std::string doc_path = column_name_to_document_path(col_info->json_path);

            if (!doc->is_exists(doc_path)) {
                vec.set_null(0, true);
                continue;
            }

            switch (col_info->type.type()) {
            case types::logical_type::BOOLEAN:
                if (doc->is_bool(doc_path)) {
                    vec.set_value(0, types::logical_value_t(doc->get_bool(doc_path)));
                } else {
                    vec.set_null(0, true);
                }
                break;

            case types::logical_type::INTEGER:
                if (doc->is_int(doc_path)) {
                    vec.set_value(0, types::logical_value_t(doc->get_int(doc_path)));
                } else {
                    vec.set_null(0, true);
                }
                break;

            case types::logical_type::BIGINT:
                if (doc->is_long(doc_path)) {
                    vec.set_value(0, types::logical_value_t(doc->get_long(doc_path)));
                } else {
                    vec.set_null(0, true);
                }
                break;

            case types::logical_type::UBIGINT:
                if (doc->is_ulong(doc_path)) {
                    vec.set_value(0, types::logical_value_t(doc->get_ulong(doc_path)));
                } else {
                    vec.set_null(0, true);
                }
                break;

            case types::logical_type::DOUBLE:
                if (doc->is_double(doc_path)) {
                    vec.set_value(0, types::logical_value_t(doc->get_double(doc_path)));
                } else {
                    vec.set_null(0, true);
                }
                break;

            case types::logical_type::FLOAT:
                if (doc->is_float(doc_path)) {
                    vec.set_value(0, types::logical_value_t(doc->get_float(doc_path)));
                } else {
                    vec.set_null(0, true);
                }
                break;

            case types::logical_type::STRING_LITERAL:
                if (doc->is_string(doc_path)) {
                    std::string str_val(doc->get_string(doc_path));
                    vec.set_value(0, types::logical_value_t(str_val));
                } else {
                    vec.set_null(0, true);
                }
                break;

            default:
                vec.set_null(0, true);
                break;
            }
        }

        return chunk;
    }

    document::document_ptr document_table_storage_t::row_to_document(const vector::data_chunk_t& row, size_t row_idx) {
        if (row_idx >= row.size()) {
            return nullptr;
        }

        auto doc = document::make_document(resource_);

        for (size_t col_idx = 0; col_idx < columns_.size() && col_idx < row.column_count(); ++col_idx) {
            const auto& col_info = columns_[col_idx];
            auto value = row.value(col_idx, row_idx);

            if (value.is_null()) {
                continue;
            }

            std::string doc_path = col_info.json_path;
            if (!doc_path.empty() && doc_path[0] != '/') {
                doc_path = "/" + doc_path;
            }

            switch (value.type().type()) {
            case types::logical_type::BOOLEAN:
                doc->set(doc_path, value.value<bool>());
                break;
            case types::logical_type::TINYINT:
                doc->set(doc_path, value.value<int8_t>());
                break;
            case types::logical_type::SMALLINT:
                doc->set(doc_path, value.value<int16_t>());
                break;
            case types::logical_type::INTEGER:
                doc->set(doc_path, value.value<int32_t>());
                break;
            case types::logical_type::BIGINT:
                doc->set(doc_path, value.value<int64_t>());
                break;
            case types::logical_type::UTINYINT:
                doc->set(doc_path, value.value<uint8_t>());
                break;
            case types::logical_type::USMALLINT:
                doc->set(doc_path, value.value<uint16_t>());
                break;
            case types::logical_type::UINTEGER:
                doc->set(doc_path, value.value<uint32_t>());
                break;
            case types::logical_type::UBIGINT:
                doc->set(doc_path, value.value<uint64_t>());
                break;
            case types::logical_type::FLOAT:
                doc->set(doc_path, value.value<float>());
                break;
            case types::logical_type::DOUBLE:
                doc->set(doc_path, value.value<double>());
                break;
            case types::logical_type::STRING_LITERAL:
                doc->set(doc_path, std::string(value.value<std::string_view>()));
                break;
            default:
                break;
            }
        }

        return doc;
    }

    types::logical_type document_table_storage_t::detect_value_type_in_document(const document::document_ptr& doc,
                                                                                const std::string& json_path) {
        std::string doc_path = column_name_to_document_path(json_path);

        if (!doc || !doc->is_exists(doc_path)) {
            return types::logical_type::NA;
        }

        if (doc->is_bool(doc_path)) return types::logical_type::BOOLEAN;
        if (doc->is_int(doc_path)) return types::logical_type::INTEGER;
        if (doc->is_long(doc_path)) return types::logical_type::BIGINT;
        if (doc->is_ulong(doc_path)) return types::logical_type::UBIGINT;
        if (doc->is_double(doc_path)) return types::logical_type::DOUBLE;
        if (doc->is_float(doc_path)) return types::logical_type::FLOAT;
        if (doc->is_string(doc_path)) return types::logical_type::STRING_LITERAL;

        return types::logical_type::NA;
    }

    types::logical_value_t document_table_storage_t::extract_value_from_document(
        const document::document_ptr& doc,
        const std::string& json_path,
        types::logical_type expected_type) {

        std::string doc_path = column_name_to_document_path(json_path);

        if (!doc || !doc->is_exists(doc_path)) {
            return types::logical_value_t();
        }

        switch (expected_type) {
        case types::logical_type::BOOLEAN:
            if (doc->is_bool(doc_path)) return types::logical_value_t(doc->get_bool(doc_path));
            break;
        case types::logical_type::INTEGER:
            if (doc->is_int(doc_path)) return types::logical_value_t(doc->get_int(doc_path));
            break;
        case types::logical_type::BIGINT:
            if (doc->is_long(doc_path)) return types::logical_value_t(doc->get_long(doc_path));
            break;
        case types::logical_type::UBIGINT:
            if (doc->is_ulong(doc_path)) return types::logical_value_t(doc->get_ulong(doc_path));
            break;
        case types::logical_type::DOUBLE:
            if (doc->is_double(doc_path)) return types::logical_value_t(doc->get_double(doc_path));
            break;
        case types::logical_type::FLOAT:
            if (doc->is_float(doc_path)) return types::logical_value_t(doc->get_float(doc_path));
            break;
        case types::logical_type::STRING_LITERAL:
            if (doc->is_string(doc_path)) {
                std::string str_val(doc->get_string(doc_path));
                return types::logical_value_t(str_val);
            }
            break;
        default:
            break;
        }

        return types::logical_value_t();
    }

    std::pmr::unordered_map<std::string, types::logical_value_t>
    document_table_storage_t::extract_path_values(const document::document_ptr& doc) {
        std::pmr::unordered_map<std::string, types::logical_value_t> result(resource_);

        if (!doc || !doc->is_valid()) {
            return result;
        }

        auto extracted_paths = extractor_->extract_paths(doc);

        for (const auto& path_info : extracted_paths) {
            std::string doc_path = column_name_to_document_path(path_info.path);

            if (!doc->is_exists(doc_path)) {
                result[path_info.path] = types::logical_value_t();
                continue;
            }

            auto actual_type = detect_value_type_in_document(doc, path_info.path);

            if (actual_type == types::logical_type::NA) {
                result[path_info.path] = types::logical_value_t();
                continue;
            }

            auto value = extract_value_from_document(doc, path_info.path, actual_type);
            result[path_info.path] = std::move(value);
        }

        return result;
    }

    void document_table_storage_t::batch_insert(
        const std::pmr::vector<std::pair<document::document_id_t, document::document_ptr>>& documents) {

        if (documents.empty()) {
            return;
        }

        // Step 1: Evolve schema from all documents
        for (const auto& [id, doc] : documents) {
            if (!doc || !doc->is_valid()) {
                continue;
            }

            auto new_columns = evolve_from_document(doc);
            if (!new_columns.empty()) {
                evolve_schema(new_columns);
            }
        }

        // Step 2: Batch insert
        constexpr size_t BATCH_SIZE = 1024;

        for (size_t batch_start = 0; batch_start < documents.size(); batch_start += BATCH_SIZE) {
            size_t batch_end = std::min(batch_start + BATCH_SIZE, documents.size());
            size_t batch_count = batch_end - batch_start;

            auto types = table_->copy_types();
            vector::data_chunk_t batch_chunk(resource_, types);
            batch_chunk.set_cardinality(batch_count);

            for (size_t batch_idx = 0; batch_idx < batch_count; ++batch_idx) {
                const auto& [id, doc] = documents[batch_start + batch_idx];

                if (!doc || !doc->is_valid()) {
                    for (size_t col_idx = 0; col_idx < column_count(); ++col_idx) {
                        batch_chunk.data[col_idx].set_null(batch_idx, true);
                    }
                    continue;
                }

                auto path_values = extract_path_values(doc);

                for (size_t col_idx = 0; col_idx < column_count(); ++col_idx) {
                    const auto* col_info = get_column_by_index(col_idx);
                    auto& vec = batch_chunk.data[col_idx];

                    if (col_info->json_path == "_id") {
                        std::string id_str(reinterpret_cast<const char*>(id.data()), id.size);
                        vec.set_value(batch_idx, types::logical_value_t(id_str));
                        continue;
                    }

                    auto it = path_values.find(col_info->json_path);
                    if (it != path_values.end() && !it->second.is_null()) {
                        vec.set_value(batch_idx, it->second);
                    } else {
                        vec.set_null(batch_idx, true);
                    }
                }
            }

            table::table_append_state state(resource_);
            table_->append_lock(state);
            table_->initialize_append(state);
            table_->append(batch_chunk, state);
            table_->finalize_append(state);

            for (size_t batch_idx = 0; batch_idx < batch_count; ++batch_idx) {
                const auto& [id, doc] = documents[batch_start + batch_idx];
                id_to_row_[id] = next_row_id_++;
            }
        }
    }

} // namespace components::document_table
