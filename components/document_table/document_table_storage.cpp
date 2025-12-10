#include "document_table_storage.hpp"
#include <components/table/table_state.hpp>

namespace components::document_table {

    namespace {
        // Convert SQL-safe column name back to document API path
        // "kind" -> "/kind"
        // "commit_dot_operation" -> "/commit/operation"
        // "field_arr0_" -> "/field[0]"
        std::string column_name_to_document_path(const std::string& column_name) {
            std::string result = "/";
            size_t i = 0;
            while (i < column_name.size()) {
                // Check for "_dot_"
                if (i + 5 <= column_name.size() && column_name.substr(i, 5) == "_dot_") {
                    result += '/';
                    i += 5;
                }
                // Check for "_arr" followed by digit(s) and "_"
                else if (i + 4 <= column_name.size() && 
                         column_name.substr(i, 4) == "_arr" && 
                         i + 4 < column_name.size() && 
                         std::isdigit(column_name[i + 4])) {
                    result += '[';
                    i += 4;
                    // Read all digits
                    while (i < column_name.size() && std::isdigit(column_name[i])) {
                        result += column_name[i];
                        i++;
                    }
                    result += ']';
                    // Skip trailing "_" if present
                    if (i < column_name.size() && column_name[i] == '_') {
                        i++;
                    }
                }
                else {
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
        , schema_(std::make_unique<dynamic_schema_t>(resource))
        , id_to_row_(10, document_id_hash_t{}, std::equal_to<document::document_id_t>{}, resource)
        , next_row_id_(0) {

        // Создаем начальную таблицу с минимальной схемой (_id колонка)
        auto column_defs = schema_->to_column_definitions();
        table_ = std::make_unique<table::data_table_t>(resource_, block_manager_, std::move(column_defs));
    }

    void document_table_storage_t::insert(const document::document_id_t& id, const document::document_ptr& doc) {
        if (!doc || !doc->is_valid()) {
            return;
        }

        // 1. Проверяем, нужна ли эволюция схемы
        auto new_columns = schema_->evolve(doc);

        // 2. Если есть новые колонки - расширяем таблицу
        if (!new_columns.empty()) {
            evolve_schema(new_columns);
        }

        // 3. Конвертируем документ в row
        auto row = document_to_row(doc);

        // 4. Вставляем в таблицу
        table::table_append_state state(resource_);
        table_->append_lock(state);
        table_->initialize_append(state);
        table_->append(row, state);
        table_->finalize_append(state);

        // 5. Сохраняем маппинг
        id_to_row_[id] = next_row_id_++;
    }

    document::document_ptr document_table_storage_t::get(const document::document_id_t& id) {
        // TODO: реализовать конвертацию row -> document
        // Пока возвращаем nullptr
        return nullptr;
    }

    void document_table_storage_t::remove(const document::document_id_t& id) {
        auto it = id_to_row_.find(id);
        if (it == id_to_row_.end()) {
            return;
        }

        // Получаем row_id
        size_t row_id = it->second;

        // Удаляем из таблицы
        vector::vector_t row_ids(resource_, types::logical_type::UBIGINT);
        row_ids.set_value(0, types::logical_value_t(static_cast<uint64_t>(row_id)));

        auto delete_state = table_->initialize_delete({});
        table_->delete_rows(*delete_state, row_ids, 1);

        // Удаляем из маппинга
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

    bool document_table_storage_t::needs_evolution(const document::document_ptr& doc) const {
        auto extracted_paths = schema_->extractor().extract_paths(doc);

        for (const auto& path_info : extracted_paths) {
            if (!schema_->has_path(path_info.path)) {
                return true;
            }
        }

        return false;
    }

    void document_table_storage_t::evolve_schema(const std::pmr::vector<column_info_t>& new_columns) {
        // Инкрементально добавляем новые колонки
        // Используем конструктор data_table_t(parent, new_column) который переиспользует
        // существующие row_groups через shared_ptr (O(1) вместо O(N))
        
        for (const auto& col_info : new_columns) {
            // Создаем NULL default значение для новой колонки с правильным типом
            // Если колонка nullable (union с NULL), создаем значение того же union типа
            auto default_value = std::make_unique<types::logical_value_t>(col_info.type);
            
            // Создаем column_definition для новой колонки с default значением
            table::column_definition_t new_column_def(col_info.json_path, col_info.type, std::move(default_value));
            
            // Создаем новую таблицу, добавляя одну колонку к существующей
            // Это НЕ копирует данные - только переиспользует row_groups через shared_ptr
            auto new_table = std::make_unique<table::data_table_t>(*table_, new_column_def);
            
            // Заменяем старую таблицу новой
            table_ = std::move(new_table);
        }
    }


    vector::data_chunk_t document_table_storage_t::document_to_row(const document::document_ptr& doc) {
        // Создаем chunk на одну строку
        auto types = table_->copy_types();
        vector::data_chunk_t chunk(resource_, types);
        chunk.set_cardinality(1);

        // Проходим по всем колонкам схемы
        for (size_t i = 0; i < schema_->column_count(); ++i) {
            const auto* col_info = schema_->get_column_by_index(i);
            auto& vec = chunk.data[i];

            // Специальная обработка для _id
            if (col_info->json_path == "_id") {
                // Получаем document_id из документа
                auto doc_id = document::get_document_id(doc);
                std::string id_str(reinterpret_cast<const char*>(doc_id.data()), doc_id.size);
                vec.set_value(0, types::logical_value_t(id_str));
                continue;
            }

            // Convert column name to document path
            std::string doc_path = column_name_to_document_path(col_info->json_path);
            
            // Проверяем, есть ли это поле в документе
            if (!doc->is_exists(doc_path)) {
                vec.set_null(0, true);
                continue;
            }

            // Извлекаем значение в зависимости от типа
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

            case types::logical_type::UNION:
                // ВРЕМЕННОЕ РЕШЕНИЕ: для UNION колонок сохраняем данные в их фактическом типе
                // Schema отслеживает что колонка может содержать разные типы
                if (col_info->is_union) {
                    // Определяем фактический тип значения в документе
                    auto actual_type = detect_value_type_in_document(doc, col_info->json_path);

                    if (actual_type == types::logical_type::NA) {
                        vec.set_null(0, true);
                        break;
                    }

                    // Извлекаем значение нужного типа и сохраняем напрямую
                    auto value = extract_value_from_document(doc, col_info->json_path, actual_type);

                    if (value.is_null()) {
                        vec.set_null(0, true);
                    } else {
                        vec.set_value(0, std::move(value));
                    }
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

    document::document_ptr document_table_storage_t::row_to_document(const vector::data_chunk_t& row,
                                                                      size_t row_idx) {
        if (row_idx >= row.size()) {
            return nullptr;
        }
        
        auto doc = document::make_document(resource_);
        const auto& columns = schema_->columns();
        
        // Итерируем по всем колонкам и восстанавливаем значения
        for (size_t col_idx = 0; col_idx < columns.size() && col_idx < row.column_count(); ++col_idx) {
            const auto& col_info = columns[col_idx];
            
            // Получаем значение из chunk
            auto value = row.value(col_idx, row_idx);
            
            // Пропускаем NULL значения
            if (value.is_null()) {
                continue;
            }
            
            // Устанавливаем значение в документ по JSON path
            const std::string& path = col_info.json_path;
            
            // Добавляем "/" в начало если нужно
            std::string doc_path = path;
            if (!doc_path.empty() && doc_path[0] != '/') {
                doc_path = "/" + doc_path;
            }
            
            // Устанавливаем значение в зависимости от типа
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
                // Неподдерживаемый тип, пропускаем
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

        // Проверяем типы в порядке приоритета
        if (doc->is_bool(doc_path)) {
            return types::logical_type::BOOLEAN;
        }
        if (doc->is_int(doc_path)) {
            return types::logical_type::INTEGER;
        }
        if (doc->is_long(doc_path)) {
            return types::logical_type::BIGINT;
        }
        if (doc->is_ulong(doc_path)) {
            return types::logical_type::UBIGINT;
        }
        if (doc->is_double(doc_path)) {
            return types::logical_type::DOUBLE;
        }
        if (doc->is_float(doc_path)) {
            return types::logical_type::FLOAT;
        }
        if (doc->is_string(doc_path)) {
            return types::logical_type::STRING_LITERAL;
        }

        return types::logical_type::NA;
    }

    types::logical_value_t document_table_storage_t::extract_value_from_document(
        const document::document_ptr& doc,
        const std::string& json_path,
        types::logical_type expected_type) {

        std::string doc_path = column_name_to_document_path(json_path);
        
        if (!doc || !doc->is_exists(doc_path)) {
            return types::logical_value_t(); // null value
        }

        // Извлекаем значение в зависимости от ожидаемого типа
        switch (expected_type) {
        case types::logical_type::BOOLEAN:
            if (doc->is_bool(doc_path)) {
                return types::logical_value_t(doc->get_bool(doc_path));
            }
            break;

        case types::logical_type::INTEGER:
            if (doc->is_int(doc_path)) {
                return types::logical_value_t(doc->get_int(doc_path));
            }
            break;

        case types::logical_type::BIGINT:
            if (doc->is_long(doc_path)) {
                return types::logical_value_t(doc->get_long(doc_path));
            }
            break;

        case types::logical_type::UBIGINT:
            if (doc->is_ulong(doc_path)) {
                return types::logical_value_t(doc->get_ulong(doc_path));
            }
            break;

        case types::logical_type::DOUBLE:
            if (doc->is_double(doc_path)) {
                return types::logical_value_t(doc->get_double(doc_path));
            }
            break;

        case types::logical_type::FLOAT:
            if (doc->is_float(doc_path)) {
                return types::logical_value_t(doc->get_float(doc_path));
            }
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

        // Если тип не совпал, возвращаем null
        return types::logical_value_t();
    }

} // namespace components::document_table
