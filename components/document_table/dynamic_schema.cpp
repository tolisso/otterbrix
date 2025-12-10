#include "dynamic_schema.hpp"
#include <stdexcept>

namespace components::document_table {

    namespace {
        // Создает универсальный UNION тип для всех колонок
        // UNION(NULL, STRING, BIGINT, UBIGINT, INTEGER, BOOL, DOUBLE, FLOAT)
        types::complex_logical_type create_universal_union_type() {
            std::vector<types::complex_logical_type> union_types;
            union_types.emplace_back(types::logical_type::NA);            // NULL
            union_types.emplace_back(types::logical_type::STRING_LITERAL);
            union_types.emplace_back(types::logical_type::BIGINT);
            union_types.emplace_back(types::logical_type::UBIGINT);
            union_types.emplace_back(types::logical_type::INTEGER);
            union_types.emplace_back(types::logical_type::BOOLEAN);
            union_types.emplace_back(types::logical_type::DOUBLE);
            union_types.emplace_back(types::logical_type::FLOAT);
            
            return types::complex_logical_type::create_union(std::move(union_types));
        }
    } // anonymous namespace

    dynamic_schema_t::dynamic_schema_t(std::pmr::memory_resource* resource)
        : resource_(resource)
        , columns_(resource)
        , path_to_index_(resource)
        , extractor_(std::make_unique<json_path_extractor_t>(resource)) {

        // Всегда добавляем служебную колонку для document_id
        types::complex_logical_type id_type(types::logical_type::STRING_LITERAL);
        id_type.set_alias("_id");
        add_column("_id", id_type);
    }

    bool dynamic_schema_t::has_path(const std::string& json_path) const {
        return path_to_index_.find(json_path) != path_to_index_.end();
    }

    const column_info_t* dynamic_schema_t::get_column_info(const std::string& json_path) const {
        auto it = path_to_index_.find(json_path);
        if (it == path_to_index_.end()) {
            return nullptr;
        }
        return &columns_[it->second];
    }

    const column_info_t* dynamic_schema_t::get_column_by_index(size_t index) const {
        if (index >= columns_.size()) {
            return nullptr;
        }
        return &columns_[index];
    }

    void dynamic_schema_t::add_column(const std::string& json_path,
                                      const types::complex_logical_type& type,
                                      bool is_array_element,
                                      size_t array_index) {
        // Проверяем, что колонка еще не существует
        if (has_path(json_path)) {
            return; // Уже есть
        }

        // Добавляем новую колонку
        size_t new_index = columns_.size();
        column_info_t new_col(resource_);
        new_col.json_path = json_path;
        new_col.type = type;
        new_col.column_index = new_index;
        new_col.is_array_element = is_array_element;
        new_col.array_index = array_index;
        
        columns_.push_back(std::move(new_col));

        // Обновляем маппинг
        path_to_index_[json_path] = new_index;
    }

    const std::pmr::vector<column_info_t>& dynamic_schema_t::columns() const { return columns_; }

    size_t dynamic_schema_t::column_count() const { return columns_.size(); }

    std::vector<table::column_definition_t> dynamic_schema_t::to_column_definitions() const {
        std::vector<table::column_definition_t> result;
        result.reserve(columns_.size());

        for (const auto& col : columns_) {
            result.emplace_back(col.json_path, col.type);
        }

        return result;
    }

    std::pmr::vector<column_info_t> dynamic_schema_t::evolve(const document::document_ptr& doc) {
        std::pmr::vector<column_info_t> new_columns(resource_);

        if (!doc || !doc->is_valid()) {
            return new_columns;
        }

        // Упрощенное извлечение: только имена полей (без типов!)
        auto field_names = extractor_->extract_field_names(doc);

        // Проходим по всем полям
        for (const auto& field_name : field_names) {
            // Если поле уже существует - пропускаем (тип не проверяем, т.к. universal union)
            if (has_path(field_name)) {
                continue;
            }

            // Создаем новую колонку с универсальным union типом
            auto universal_type = create_universal_union_type();
            universal_type.set_alias(field_name);

            add_column(field_name, universal_type, false, 0);

            // Запоминаем, что это новая колонка
            new_columns.push_back(columns_.back());
        }

        return new_columns;
    }

    void dynamic_schema_t::create_union_column(const std::string& json_path,
                                               types::logical_type type1,
                                               types::logical_type type2) {
        auto it = path_to_index_.find(json_path);
        if (it == path_to_index_.end()) {
            return;
        }

        auto& col = columns_[it->second];

        // ВРЕМЕННОЕ РЕШЕНИЕ: вместо создания UNION type, оставляем первый тип
        // col.type уже установлен как type1, не меняем его
        col.is_union = true;
        col.union_types.clear();
        col.union_types.push_back(type1);
        col.union_types.push_back(type2);
    }

    void dynamic_schema_t::extend_union_column(const std::string& json_path, types::logical_type new_type) {
        auto it = path_to_index_.find(json_path);
        if (it == path_to_index_.end()) {
            return;
        }

        auto& col = columns_[it->second];

        // Проверяем, что тип еще не в union
        if (std::find(col.union_types.begin(), col.union_types.end(), new_type) != col.union_types.end()) {
            return; // Уже есть
        }

        // Добавляем новый тип в union
        col.union_types.push_back(new_type);

        // ВРЕМЕННОЕ РЕШЕНИЕ: не меняем col.type, оставляем первый тип
    }

    uint8_t dynamic_schema_t::get_union_tag(const column_info_t* col, types::logical_type type) const {
        if (!col || !col->is_union) {
            return 0;
        }

        auto it = std::find(col->union_types.begin(), col->union_types.end(), type);
        if (it == col->union_types.end()) {
            throw std::runtime_error("Type " + std::to_string(static_cast<int>(type)) +
                                     " not found in union for path '" + col->json_path + "'");
        }

        return static_cast<uint8_t>(std::distance(col->union_types.begin(), it));
    }

} // namespace components::document_table
