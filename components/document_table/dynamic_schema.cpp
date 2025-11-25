#include "dynamic_schema.hpp"
#include <stdexcept>

namespace components::document_table {

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
        columns_.push_back(column_info_t{.json_path = json_path,
                                         .type = type,
                                         .column_index = new_index,
                                         .is_array_element = is_array_element,
                                         .array_index = array_index});

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

        // Извлекаем все пути из документа
        auto extracted_paths = extractor_->extract_paths(doc);

        // Проходим по всем путям
        for (const auto& path_info : extracted_paths) {
            // Если путь уже существует, проверяем совместимость типов
            if (has_path(path_info.path)) {
                const auto* existing_col = get_column_info(path_info.path);
                if (existing_col) {
                    auto existing_type = existing_col->type.type();
                    auto new_type = path_info.type;

                    // Проверяем, что типы совпадают
                    if (existing_type != new_type) {
                        throw std::runtime_error(
                            "Type mismatch for path '" + path_info.path + "': " +
                            "existing type is " + std::to_string(static_cast<int>(existing_type)) +
                            ", but document has type " + std::to_string(static_cast<int>(new_type)));
                    }
                }
                continue;
            }

            // Создаем новую колонку
            types::complex_logical_type col_type(path_info.type);
            col_type.set_alias(path_info.path);

            add_column(path_info.path, col_type, path_info.is_array, path_info.array_index);

            // Запоминаем, что это новая колонка
            new_columns.push_back(columns_.back());
        }

        return new_columns;
    }

} // namespace components::document_table
