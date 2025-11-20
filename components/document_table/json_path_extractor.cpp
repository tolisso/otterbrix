#include "json_path_extractor.hpp"
#include <components/document/container/json_array.hpp>
#include <components/document/container/json_object.hpp>

namespace components::document_table {

    json_path_extractor_t::json_path_extractor_t(std::pmr::memory_resource* resource)
        : resource_(resource) {}

    std::pmr::vector<extracted_path_t> json_path_extractor_t::extract_paths(const document::document_ptr& doc) {
        std::pmr::vector<extracted_path_t> result(resource_);

        if (!doc || !doc->is_valid()) {
            return result;
        }

        // Получаем корневой узел JSON trie
        const auto* root = doc->json_trie().get();

        // Рекурсивно извлекаем все пути
        extract_recursive(root, "", 0, result);

        return result;
    }

    void json_path_extractor_t::extract_recursive(const document::json::json_trie_node* node,
                                                   const std::string& current_path,
                                                   size_t depth,
                                                   std::pmr::vector<extracted_path_t>& result) {
        if (!node) {
            return;
        }

        // Проверка максимальной глубины
        if (depth >= config_.max_nesting_depth) {
            return;
        }

        if (node->is_object()) {
            // Объект: проходим по всем полям
            const auto* obj = node->get_object();
            for (const auto& [key_node, value_node] : *obj) {
                // Извлекаем имя поля из ключа
                std::string field_name;
                if (key_node->is_mut()) {
                    const auto* elem = key_node->get_mut();
                    if (elem->is_string()) {
                        auto str_result = elem->get_string();
                        if (!str_result.error()) {
                            field_name = std::string(str_result.value());
                        }
                    }
                }

                if (field_name.empty()) {
                    continue; // Пропускаем невалидные ключи
                }

                std::string field_path = join_path(current_path, field_name);
                const auto* child = value_node.get();

                if (child->is_object() || child->is_array()) {
                    // Рекурсивно обрабатываем вложенные структуры
                    if (config_.extract_nested_objects) {
                        extract_recursive(child, field_path, depth + 1, result);
                    }
                } else if (child->is_mut()) {
                    // Скалярное значение - добавляем в результат
                    const auto* elem = child->get_mut();
                    result.push_back(extracted_path_t{
                        .path = field_path,
                        .type = infer_type(elem),
                        .is_array = false,
                        .array_index = 0,
                        .is_nullable = true});
                }
            }

        } else if (node->is_array()) {
            // Массив: обрабатываем элементы
            const auto* arr = node->get_array();

            if (config_.use_separate_array_table) {
                // Вариант 1: Массивы в отдельной таблице
                // TODO: добавить в array_storage
                // Пока пропускаем

            } else if (config_.flatten_arrays) {
                // Вариант 2: Разворачиваем в колонки array[0], array[1], ...
                size_t max_index = std::min(arr->size(), config_.max_array_size);

                for (size_t i = 0; i < max_index; ++i) {
                    const auto* elem_node = arr->get(i);
                    if (!elem_node) {
                        continue;
                    }

                    std::string array_path = current_path + "[" + std::to_string(i) + "]";

                    if (elem_node->is_object() || elem_node->is_array()) {
                        // Вложенная структура в массиве
                        extract_recursive(elem_node, array_path, depth + 1, result);
                    } else if (elem_node->is_mut()) {
                        const auto* elem = elem_node->get_mut();
                        result.push_back(extracted_path_t{
                            .path = array_path,
                            .type = infer_type(elem),
                            .is_array = true,
                            .array_index = i,
                            .is_nullable = true});
                    }
                }
            } else {
                // Вариант 3: Массив как JSON строка
                result.push_back(extracted_path_t{
                    .path = current_path,
                    .type = types::logical_type::STRING_LITERAL,
                    .is_array = true,
                    .array_index = 0,
                    .is_nullable = true});
            }

        } else if (node->is_mut()) {
            // Скалярное значение на корневом уровне
            const auto* elem = node->get_mut();
            result.push_back(extracted_path_t{
                .path = current_path.empty() ? "$root" : current_path,
                .type = infer_type(elem),
                .is_array = false,
                .array_index = 0,
                .is_nullable = true});
        }
    }

    types::logical_type json_path_extractor_t::infer_type(const document::impl::element* elem) const {
        if (!elem || elem->is_null()) {
            return types::logical_type::STRING_LITERAL; // NULL как VARCHAR fallback
        }

        if (elem->is_bool()) {
            return types::logical_type::BOOLEAN;
        }

        if (elem->is_int64()) {
            return types::logical_type::BIGINT;
        }

        if (elem->is_uint64()) {
            return types::logical_type::UBIGINT;
        }

        if (elem->is_int32()) {
            return types::logical_type::INTEGER;
        }

        if (elem->is_double()) {
            return types::logical_type::DOUBLE;
        }

        if (elem->is_float()) {
            return types::logical_type::FLOAT;
        }

        if (elem->is_string()) {
            return types::logical_type::STRING_LITERAL;
        }

        // По умолчанию - строка
        return types::logical_type::STRING_LITERAL;
    }

    std::string json_path_extractor_t::join_path(const std::string& parent, const std::string& child) const {
        if (parent.empty()) {
            return child;
        }
        return parent + "." + child;
    }

} // namespace components::document_table
