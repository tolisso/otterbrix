#include "collection.hpp"
#include <components/table/table_state.hpp>

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

namespace services::collection {

    bool table_storage_t::has_column(const std::string& json_path) const {
        return path_to_index_.find(json_path) != path_to_index_.end();
    }

    const column_info_t* table_storage_t::get_column_info(const std::string& json_path) const {
        auto it = path_to_index_.find(json_path);
        if (it == path_to_index_.end()) {
            return nullptr;
        }
        return &columns_[it->second];
    }

    const column_info_t* table_storage_t::get_column_by_index(size_t index) const {
        if (index >= columns_.size()) {
            return nullptr;
        }
        return &columns_[index];
    }

    std::vector<components::table::column_definition_t> table_storage_t::to_column_definitions() const {
        std::vector<components::table::column_definition_t> result;
        result.reserve(columns_.size());

        for (const auto& col : columns_) {
            auto col_type = col.type;
            col_type.set_alias(col.json_path);
            result.emplace_back(col.json_path, std::move(col_type));
        }

        return result;
    }

    void table_storage_t::add_column(const std::string& json_path,
                                     const components::types::complex_logical_type& type,
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

    std::pmr::vector<column_info_t> table_storage_t::evolve_from_document(const document_ptr& doc) {
        std::pmr::vector<column_info_t> new_columns(resource_);

        if (!doc || !doc->is_valid()) {
            return new_columns;
        }

        auto extracted_paths = extractor_->extract_paths(doc);

        for (const auto& path_info : extracted_paths) {
            if (has_column(path_info.path)) {
                continue;
            }

            components::types::complex_logical_type col_type(path_info.type);
            col_type.set_alias(path_info.path);

            add_column(path_info.path, col_type, path_info.is_array, path_info.array_index);
            new_columns.push_back(columns_.back());
        }

        return new_columns;
    }

    void table_storage_t::evolve_schema(const std::pmr::vector<column_info_t>& new_columns) {
        for (const auto& col_info : new_columns) {
            auto default_value = std::make_unique<components::types::logical_value_t>(col_info.type);
            components::table::column_definition_t new_column_def(col_info.json_path,
                                                                   col_info.type,
                                                                   std::move(default_value));
            auto new_table = std::make_unique<components::table::data_table_t>(*table_, new_column_def);
            table_ = std::move(new_table);
        }
    }

    void table_storage_t::evolve_schema_from_types(
        const std::pmr::vector<components::types::complex_logical_type>& types) {
        std::pmr::vector<column_info_t> new_columns(resource_);
        for (const auto& col_type : types) {
            const std::string& col_name = col_type.alias();
            if (col_name.empty()) {
                continue;
            }
            if (has_column(col_name) || has_column("/" + col_name)) {
                continue;
            }
            add_column(col_name, col_type);
            new_columns.push_back(columns_.back());
        }
        if (!new_columns.empty()) {
            evolve_schema(new_columns);
        }
    }

    components::types::logical_type table_storage_t::detect_value_type_in_document(
        const document_ptr& doc,
        const std::string& json_path) {
        std::string doc_path = column_name_to_document_path(json_path);

        if (!doc || !doc->is_exists(doc_path)) {
            return components::types::logical_type::NA;
        }

        if (doc->is_bool(doc_path)) return components::types::logical_type::BOOLEAN;
        if (doc->is_int(doc_path)) return components::types::logical_type::INTEGER;
        if (doc->is_long(doc_path)) return components::types::logical_type::BIGINT;
        if (doc->is_ulong(doc_path)) return components::types::logical_type::UBIGINT;
        if (doc->is_double(doc_path)) return components::types::logical_type::DOUBLE;
        if (doc->is_float(doc_path)) return components::types::logical_type::FLOAT;
        if (doc->is_string(doc_path)) return components::types::logical_type::STRING_LITERAL;

        return components::types::logical_type::NA;
    }

    components::types::logical_value_t table_storage_t::extract_value_from_document(
        const document_ptr& doc,
        const std::string& json_path,
        components::types::logical_type expected_type) {
        std::string doc_path = column_name_to_document_path(json_path);

        if (!doc || !doc->is_exists(doc_path)) {
            return components::types::logical_value_t();
        }

        switch (expected_type) {
        case components::types::logical_type::BOOLEAN:
            if (doc->is_bool(doc_path)) return components::types::logical_value_t(doc->get_bool(doc_path));
            break;
        case components::types::logical_type::INTEGER:
            if (doc->is_int(doc_path)) return components::types::logical_value_t(doc->get_int(doc_path));
            break;
        case components::types::logical_type::BIGINT:
            if (doc->is_long(doc_path)) return components::types::logical_value_t(doc->get_long(doc_path));
            break;
        case components::types::logical_type::UBIGINT:
            if (doc->is_ulong(doc_path)) return components::types::logical_value_t(doc->get_ulong(doc_path));
            break;
        case components::types::logical_type::DOUBLE:
            if (doc->is_double(doc_path)) return components::types::logical_value_t(doc->get_double(doc_path));
            break;
        case components::types::logical_type::FLOAT:
            if (doc->is_float(doc_path)) return components::types::logical_value_t(doc->get_float(doc_path));
            break;
        case components::types::logical_type::STRING_LITERAL:
            if (doc->is_string(doc_path)) {
                std::string str_val(doc->get_string(doc_path));
                return components::types::logical_value_t(str_val);
            }
            break;
        default:
            break;
        }

        return components::types::logical_value_t();
    }

    components::vector::data_chunk_t table_storage_t::document_to_row(const document_ptr& doc) {
        auto types = table_->copy_types();
        components::vector::data_chunk_t chunk(resource_, types);
        chunk.set_cardinality(1);

        for (size_t i = 0; i < column_count(); ++i) {
            const auto* col_info = get_column_by_index(i);
            auto& vec = chunk.data[i];

            if (col_info->json_path == "_id") {
                auto doc_id = components::document::get_document_id(doc);
                std::string id_str(reinterpret_cast<const char*>(doc_id.data()), doc_id.size);
                vec.set_value(0, components::types::logical_value_t(id_str));
                continue;
            }

            std::string doc_path = column_name_to_document_path(col_info->json_path);

            if (!doc->is_exists(doc_path)) {
                vec.set_null(0, true);
                continue;
            }

            switch (col_info->type.type()) {
            case components::types::logical_type::BOOLEAN:
                if (doc->is_bool(doc_path)) {
                    vec.set_value(0, components::types::logical_value_t(doc->get_bool(doc_path)));
                } else {
                    vec.set_null(0, true);
                }
                break;
            case components::types::logical_type::INTEGER:
                if (doc->is_int(doc_path)) {
                    vec.set_value(0, components::types::logical_value_t(doc->get_int(doc_path)));
                } else {
                    vec.set_null(0, true);
                }
                break;
            case components::types::logical_type::BIGINT:
                if (doc->is_long(doc_path)) {
                    vec.set_value(0, components::types::logical_value_t(doc->get_long(doc_path)));
                } else {
                    vec.set_null(0, true);
                }
                break;
            case components::types::logical_type::UBIGINT:
                if (doc->is_ulong(doc_path)) {
                    vec.set_value(0, components::types::logical_value_t(doc->get_ulong(doc_path)));
                } else {
                    vec.set_null(0, true);
                }
                break;
            case components::types::logical_type::DOUBLE:
                if (doc->is_double(doc_path)) {
                    vec.set_value(0, components::types::logical_value_t(doc->get_double(doc_path)));
                } else {
                    vec.set_null(0, true);
                }
                break;
            case components::types::logical_type::FLOAT:
                if (doc->is_float(doc_path)) {
                    vec.set_value(0, components::types::logical_value_t(doc->get_float(doc_path)));
                } else {
                    vec.set_null(0, true);
                }
                break;
            case components::types::logical_type::STRING_LITERAL:
                if (doc->is_string(doc_path)) {
                    std::string str_val(doc->get_string(doc_path));
                    vec.set_value(0, components::types::logical_value_t(str_val));
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

    components::vector::data_chunk_t table_storage_t::prepare_insert(
        const std::pmr::vector<std::pair<document_id_t, document_ptr>>& documents) {

        if (documents.empty()) {
            return components::vector::data_chunk_t(resource_, table_->copy_types());
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

        // Step 2: Build column path cache
        std::vector<std::string> doc_paths;
        doc_paths.reserve(column_count());
        for (size_t i = 0; i < column_count(); ++i) {
            const auto* col_info = get_column_by_index(i);
            doc_paths.emplace_back(column_name_to_document_path(col_info->json_path));
        }

        // Step 3: Create one data_chunk for all documents
        auto types = table_->copy_types();
        components::vector::data_chunk_t chunk(resource_, types, documents.size());
        chunk.set_cardinality(documents.size());

        // Step 4: Fill the chunk
        for (size_t row = 0; row < documents.size(); ++row) {
            const auto& [id, doc] = documents[row];

            if (!doc || !doc->is_valid()) {
                for (size_t col = 0; col < column_count(); ++col) {
                    chunk.data[col].set_null(row, true);
                }
                continue;
            }

            for (size_t col = 0; col < column_count(); ++col) {
                const auto* col_info = get_column_by_index(col);
                auto& vec = chunk.data[col];

                const auto& doc_path = doc_paths[col];
                if (!doc->is_exists(doc_path)) {
                    vec.set_null(row, true);
                    continue;
                }

                auto value = extract_value_from_document(doc, col_info->json_path, col_info->type.type());
                if (value.is_null()) {
                    vec.set_null(row, true);
                } else {
                    vec.set_value(row, value);
                }
            }
        }

        return chunk;
    }

} // namespace services::collection
