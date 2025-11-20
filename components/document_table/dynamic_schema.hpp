#pragma once

#include "json_path_extractor.hpp"
#include <components/document/document.hpp>
#include <components/table/column_definition.hpp>
#include <components/types/types.hpp>
#include <memory>
#include <memory_resource>
#include <string>
#include <unordered_map>
#include <vector>

namespace components::document_table {

    struct column_info_t {
        std::string json_path;               // JSON path (например, "user.address.city")
        types::complex_logical_type type;    // Тип колонки
        size_t column_index;                 // Индекс колонки в таблице
        bool is_array_element;               // Элемент массива?
        size_t array_index;                  // Индекс в массиве
    };

    class dynamic_schema_t {
    public:
        explicit dynamic_schema_t(std::pmr::memory_resource* resource);

        // Проверка, существует ли путь в схеме
        bool has_path(const std::string& json_path) const;

        // Получение информации о колонке по пути
        const column_info_t* get_column_info(const std::string& json_path) const;

        // Получение колонки по индексу
        const column_info_t* get_column_by_index(size_t index) const;

        // Добавление новой колонки
        void add_column(const std::string& json_path,
                        const types::complex_logical_type& type,
                        bool is_array_element = false,
                        size_t array_index = 0);

        // Получение всех колонок
        const std::pmr::vector<column_info_t>& columns() const;

        // Количество колонок
        size_t column_count() const;

        // Конвертация в column_definition_t для data_table
        std::vector<table::column_definition_t> to_column_definitions() const;

        // Эволюция схемы: добавление новых путей из документа
        std::pmr::vector<column_info_t> evolve(const document::document_ptr& doc);

        // Доступ к экстрактору
        json_path_extractor_t& extractor() { return *extractor_; }
        const json_path_extractor_t& extractor() const { return *extractor_; }

    private:
        std::pmr::memory_resource* resource_;

        // Список всех колонок в порядке добавления
        std::pmr::vector<column_info_t> columns_;

        // Маппинг JSON path -> индекс в columns_
        std::pmr::unordered_map<std::string, size_t> path_to_index_;

        // Экстрактор путей
        std::unique_ptr<json_path_extractor_t> extractor_;
    };

} // namespace components::document_table
