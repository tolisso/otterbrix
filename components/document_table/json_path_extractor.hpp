#pragma once

#include <components/document/document.hpp>
#include <components/document/json_trie_node.hpp>
#include <components/types/types.hpp>
#include <memory_resource>
#include <string>
#include <vector>

namespace components::document_table {

    struct extracted_path_t {
        std::string path;            // JSON path (например, "user.address.city")
        types::logical_type type;    // Тип данных
        bool is_array;               // Это элемент массива?
        size_t array_index;          // Индекс в массиве (если is_array=true)
        bool is_nullable;            // Может быть NULL
    };

    class json_path_extractor_t {
    public:
        explicit json_path_extractor_t(std::pmr::memory_resource* resource);

        // Извлечение всех путей из документа
        std::pmr::vector<extracted_path_t> extract_paths(const document::document_ptr& doc);

        // Конфигурация
        struct config_t {
            size_t max_array_size = 100;           // Максимум элементов массива
            bool flatten_arrays = true;            // Разворачивать массивы в колонки
            bool use_separate_array_table = false; // Использовать отдельную таблицу для массивов
            bool extract_nested_objects = true;    // Извлекать вложенные объекты
            size_t max_nesting_depth = 10;         // Максимальная глубина вложенности
        };

        config_t& config() { return config_; }
        const config_t& config() const { return config_; }

    private:
        // Рекурсивное извлечение с текущим путем
        void extract_recursive(const document::json::json_trie_node* node,
                               const std::string& current_path,
                               size_t depth,
                               std::pmr::vector<extracted_path_t>& result);

        // Определение типа из element
        types::logical_type infer_type(const document::impl::element* elem) const;

        // Объединение путей с разделителем
        std::string join_path(const std::string& parent, const std::string& child) const;

        std::pmr::memory_resource* resource_;
        config_t config_;
    };

} // namespace components::document_table
