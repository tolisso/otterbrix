# JSON Path Extractor - Техническая спецификация

## Назначение

Компонент для автоматического извлечения всех JSON paths из документа и определения их типов для создания схемы таблицы.

## Архитектура

### Основной класс: json_path_extractor_t

```cpp
// components/document_table/json_path_extractor.hpp

namespace components::document_table {

struct extracted_path_t {
    std::string path;                    // JSON path (например, "user.address.city")
    types::logical_type type;            // Тип данных
    bool is_array;                       // Это элемент массива?
    size_t array_index;                  // Индекс в массиве (если is_array=true)
    bool is_nullable;                    // Может быть NULL
};

class json_path_extractor_t {
public:
    explicit json_path_extractor_t(std::pmr::memory_resource* resource);

    // Извлечение всех путей из документа
    std::pmr::vector<extracted_path_t> extract_paths(const document::document_ptr& doc);

    // Рекурсивное извлечение с текущим путем
    void extract_recursive(
        const document::json::json_trie_node* node,
        const std::string& current_path,
        std::pmr::vector<extracted_path_t>& result
    );

private:
    std::pmr::memory_resource* resource_;

    // Настройки извлечения
    struct config_t {
        size_t max_array_size = 100;          // Максимум элементов массива
        bool flatten_arrays = true;           // Разворачивать массивы в колонки
        bool use_separate_array_table = false; // Использовать отдельную таблицу для массивов
        bool extract_nested_objects = true;   // Извлекать вложенные объекты
        size_t max_nesting_depth = 10;        // Максимальная глубина вложенности
    } config_;

    // Определение типа из element
    types::logical_type infer_type(const document::impl::element* elem) const;

    // Объединение путей с разделителем
    std::string join_path(const std::string& parent, const std::string& child) const;
};

} // namespace components::document_table
```

## Реализация

### extract_paths - главная функция

```cpp
std::pmr::vector<extracted_path_t> json_path_extractor_t::extract_paths(
    const document::document_ptr& doc
) {
    std::pmr::vector<extracted_path_t> result(resource_);

    // Получаем корневой узел JSON trie
    const auto* root = doc->json_trie().get();

    // Рекурсивно извлекаем все пути
    extract_recursive(root, "", result);

    return result;
}
```

### extract_recursive - рекурсивное извлечение

```cpp
void json_path_extractor_t::extract_recursive(
    const document::json::json_trie_node* node,
    const std::string& current_path,
    std::pmr::vector<extracted_path_t>& result
) {
    if (!node) return;

    if (node->is_object()) {
        // Объект: проходим по всем полям
        const auto* obj = node->get_object();
        for (auto it = obj->begin(); it != obj->end(); ++it) {
            std::string field_path = join_path(current_path, it->first);
            const auto* child = it->second.get();

            if (child->is_object() || child->is_array()) {
                // Рекурсивно обрабатываем вложенные структуры
                if (config_.extract_nested_objects) {
                    extract_recursive(child, field_path, result);
                }
            } else if (child->is_mut()) {
                // Скалярное значение - добавляем в результат
                const auto* elem = child->get_mut();
                result.push_back(extracted_path_t{
                    .path = field_path,
                    .type = infer_type(elem),
                    .is_array = false,
                    .array_index = 0,
                    .is_nullable = true
                });
            }
        }

    } else if (node->is_array()) {
        // Массив: обрабатываем элементы
        const auto* arr = node->get_array();

        if (config_.use_separate_array_table) {
            // Вариант 1: Массивы в отдельной таблице
            // TODO: добавить в array_storage

        } else if (config_.flatten_arrays) {
            // Вариант 2: Разворачиваем в колонки array[0], array[1], ...
            size_t max_index = std::min(arr->size(), config_.max_array_size);

            for (size_t i = 0; i < max_index; ++i) {
                const auto* elem_node = arr->get(i);
                std::string array_path = current_path + "[" + std::to_string(i) + "]";

                if (elem_node->is_object() || elem_node->is_array()) {
                    // Вложенная структура в массиве
                    extract_recursive(elem_node, array_path, result);
                } else if (elem_node->is_mut()) {
                    const auto* elem = elem_node->get_mut();
                    result.push_back(extracted_path_t{
                        .path = array_path,
                        .type = infer_type(elem),
                        .is_array = true,
                        .array_index = i,
                        .is_nullable = true
                    });
                }
            }
        } else {
            // Вариант 3: Массив как JSON строка
            result.push_back(extracted_path_t{
                .path = current_path,
                .type = types::logical_type::VARCHAR,  // JSON string
                .is_array = true,
                .array_index = 0,
                .is_nullable = true
            });
        }

    } else if (node->is_mut()) {
        // Скалярное значение на корневом уровне
        const auto* elem = node->get_mut();
        result.push_back(extracted_path_t{
            .path = current_path.empty() ? "$root" : current_path,
            .type = infer_type(elem),
            .is_array = false,
            .array_index = 0,
            .is_nullable = true
        });
    }
}
```

### infer_type - определение типа

```cpp
types::logical_type json_path_extractor_t::infer_type(
    const document::impl::element* elem
) const {
    // Проверяем типы в порядке приоритета

    if (elem->is_null()) {
        // NULL значение - используем VARCHAR как fallback
        return types::logical_type::VARCHAR;
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

    if (elem->is_string()) {
        return types::logical_type::VARCHAR;
    }

    // По умолчанию - строка
    return types::logical_type::VARCHAR;
}
```

### join_path - объединение путей

```cpp
std::string json_path_extractor_t::join_path(
    const std::string& parent,
    const std::string& child
) const {
    if (parent.empty()) {
        return child;
    }
    return parent + "." + child;
}
```

## Использование

### Пример 1: Простой документ

```cpp
// Входной документ
auto doc = parse_json(R"({
    "id": 1,
    "name": "Alice",
    "age": 30,
    "active": true
})");

json_path_extractor_t extractor(resource);
auto paths = extractor.extract_paths(doc);

// Результат:
// paths = [
//   { path: "id", type: INTEGER, is_array: false },
//   { path: "name", type: VARCHAR, is_array: false },
//   { path: "age", type: INTEGER, is_array: false },
//   { path: "active", type: BOOLEAN, is_array: false }
// ]
```

### Пример 2: Вложенные объекты

```cpp
// Входной документ
auto doc = parse_json(R"({
    "user": {
        "profile": {
            "name": "Bob",
            "age": 25
        },
        "address": {
            "city": "Moscow",
            "zip": "123456"
        }
    }
})");

auto paths = extractor.extract_paths(doc);

// Результат:
// paths = [
//   { path: "user.profile.name", type: VARCHAR },
//   { path: "user.profile.age", type: INTEGER },
//   { path: "user.address.city", type: VARCHAR },
//   { path: "user.address.zip", type: VARCHAR }
// ]
```

### Пример 3: Массивы

```cpp
// Входной документ
auto doc = parse_json(R"({
    "tags": ["developer", "golang", "rust"],
    "scores": [95, 87, 92]
})");

auto paths = extractor.extract_paths(doc);

// Результат (с flatten_arrays=true):
// paths = [
//   { path: "tags[0]", type: VARCHAR, is_array: true, array_index: 0 },
//   { path: "tags[1]", type: VARCHAR, is_array: true, array_index: 1 },
//   { path: "tags[2]", type: VARCHAR, is_array: true, array_index: 2 },
//   { path: "scores[0]", type: INTEGER, is_array: true, array_index: 0 },
//   { path: "scores[1]", type: INTEGER, is_array: true, array_index: 1 },
//   { path: "scores[2]", type: INTEGER, is_array: true, array_index: 2 }
// ]
```

### Пример 4: Массивы объектов

```cpp
// Входной документ
auto doc = parse_json(R"({
    "orders": [
        { "id": 1, "price": 100.5 },
        { "id": 2, "price": 200.0 }
    ]
})");

auto paths = extractor.extract_paths(doc);

// Результат:
// paths = [
//   { path: "orders[0].id", type: INTEGER },
//   { path: "orders[0].price", type: DOUBLE },
//   { path: "orders[1].id", type: INTEGER },
//   { path: "orders[1].price", type: DOUBLE }
// ]
```

## Конфигурация

### Стратегии обработки массивов

```cpp
// Стратегия 1: Разворачивание в колонки (по умолчанию)
config.flatten_arrays = true;
config.max_array_size = 100;
// Результат: tags[0], tags[1], tags[2], ...

// Стратегия 2: Отдельная таблица для массивов
config.use_separate_array_table = true;
// Результат: основная таблица + array_table(doc_id, path, index, value)

// Стратегия 3: JSON строка
config.flatten_arrays = false;
config.use_separate_array_table = false;
// Результат: tags (VARCHAR) = '["developer", "golang"]'
```

### Ограничения глубины

```cpp
config.max_nesting_depth = 10;  // Максимум 10 уровней вложенности
config.extract_nested_objects = true;  // Извлекать вложенные объекты
```

## Тесты

### Unit тесты

1. `test_extract_simple_document()` - простой документ со скалярами
2. `test_extract_nested_objects()` - вложенные объекты
3. `test_extract_arrays()` - массивы примитивов
4. `test_extract_array_of_objects()` - массивы объектов
5. `test_type_inference()` - определение типов
6. `test_max_array_size()` - ограничение размера массивов
7. `test_max_nesting_depth()` - ограничение глубины
8. `test_null_values()` - NULL значения

### Интеграционные тесты

1. Извлечение из реальных документов
2. Обработка больших документов (>1000 полей)
3. Документы с нестандартной структурой

## Файлы для создания

```
components/document_table/
├── json_path_extractor.hpp
├── json_path_extractor.cpp
└── test/
    └── test_json_path_extractor.cpp
```

## Зависимости

- `components/document/document.hpp` - структура документа
- `components/document/json_trie_node.hpp` - JSON trie
- `components/types/logical_type.hpp` - типы данных
