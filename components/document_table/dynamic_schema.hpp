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
        column_info_t(std::pmr::memory_resource* resource)
            : union_types(resource) {}
        
        column_info_t(const column_info_t& other)
            : json_path(other.json_path)
            , type(other.type)
            , column_index(other.column_index)
            , is_array_element(other.is_array_element)
            , array_index(other.array_index)
            , is_union(other.is_union)
            , union_types(other.union_types) {}
        
        column_info_t(column_info_t&&) = default;
        column_info_t& operator=(const column_info_t&) = default;
        column_info_t& operator=(column_info_t&&) = default;
        
        std::string json_path;               // JSON path (например, "user.address.city")
        types::complex_logical_type type;    // Тип колонки (может быть UNION!)
        size_t column_index = 0;             // Индекс колонки в таблице
        bool is_array_element = false;       // Элемент массива?
        size_t array_index = 0;              // Индекс в массиве
        
        // Union type support
        bool is_union = false;               // Является ли колонка union типом?
        std::pmr::vector<types::logical_type> union_types; // Типы в union (в порядке добавления)
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

        // Работа с union типами
        uint8_t get_union_tag(const column_info_t* col, types::logical_type type) const;

    private:
        // Создание union колонки из двух типов
        void create_union_column(const std::string& json_path,
                                types::logical_type type1,
                                types::logical_type type2);
        
        // Расширение существующего union новым типом
        void extend_union_column(const std::string& json_path, types::logical_type new_type);
        std::pmr::memory_resource* resource_;

        // Список всех колонок в порядке добавления
        std::pmr::vector<column_info_t> columns_;

        // Маппинг JSON path -> индекс в columns_
        std::pmr::unordered_map<std::string, size_t> path_to_index_;

        // Экстрактор путей
        std::unique_ptr<json_path_extractor_t> extractor_;
    };

} // namespace components::document_table
