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

    bool table_storage_t::has_column(const std::string& name) const {
        return path_to_index_.find(name) != path_to_index_.end();
    }

    void table_storage_t::add_column(const std::string& name,
                                     const components::types::complex_logical_type& type) {
        if (has_column(name)) {
            return;
        }
        size_t new_index = table_->column_count();
        auto default_value = std::make_unique<components::types::logical_value_t>(type);
        components::table::column_definition_t col_def(name, type, std::move(default_value));
        auto new_table = std::make_unique<components::table::data_table_t>(*table_, col_def);
        table_ = std::move(new_table);
        path_to_index_[name] = new_index;
    }

    void table_storage_t::evolve_from_document(const document_ptr& doc) {
        if (!doc || !doc->is_valid()) {
            return;
        }
        for (const auto& path_info : extractor_->extract_paths(doc)) {
            if (!has_column(path_info.path)) {
                components::types::complex_logical_type col_type(path_info.type);
                col_type.set_alias(path_info.path);
                add_column(path_info.path, col_type);
            }
        }
    }

    void table_storage_t::evolve_schema_from_types(
        const std::pmr::vector<components::types::complex_logical_type>& types) {
        for (const auto& col_type : types) {
            const std::string& name = col_type.alias();
            if (name.empty() || has_column(name) || has_column("/" + name)) {
                continue;
            }
            add_column(name, col_type);
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

        const auto& cols = table_->columns();
        for (size_t i = 0; i < cols.size(); ++i) {
            const auto& col_name = cols[i].name();
            auto& vec = chunk.data[i];

            if (col_name == "_id") {
                auto doc_id = components::document::get_document_id(doc);
                std::string id_str(reinterpret_cast<const char*>(doc_id.data()), doc_id.size);
                vec.set_value(0, components::types::logical_value_t(id_str));
                continue;
            }

            std::string doc_path = column_name_to_document_path(col_name);

            if (!doc->is_exists(doc_path)) {
                vec.set_null(0, true);
                continue;
            }

            switch (cols[i].type().type()) {
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
            if (doc && doc->is_valid()) {
                evolve_from_document(doc);
            }
        }

        // Step 2: Build column path cache
        const auto& cols = table_->columns();
        std::vector<std::string> doc_paths;
        doc_paths.reserve(cols.size());
        for (const auto& col : cols) {
            doc_paths.emplace_back(column_name_to_document_path(col.name()));
        }

        // Step 3: Create one data_chunk for all documents
        auto types = table_->copy_types();
        components::vector::data_chunk_t chunk(resource_, types, documents.size());
        chunk.set_cardinality(documents.size());

        // Step 4: Fill the chunk
        for (size_t row = 0; row < documents.size(); ++row) {
            const auto& [id, doc] = documents[row];

            if (!doc || !doc->is_valid()) {
                for (size_t col = 0; col < cols.size(); ++col) {
                    chunk.data[col].set_null(row, true);
                }
                continue;
            }

            for (size_t col = 0; col < cols.size(); ++col) {
                auto& vec = chunk.data[col];
                const auto& doc_path = doc_paths[col];
                if (!doc->is_exists(doc_path)) {
                    vec.set_null(row, true);
                    continue;
                }
                auto value = extract_value_from_document(doc, cols[col].name(), cols[col].type().type());
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
