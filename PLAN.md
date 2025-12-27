# План реализации Union типов для document_table

## Цель
Изменить document_table так, чтобы каждая колонка была типа `UNION(string, int64, double, bool)` вместо динамического определения типа. Это позволит:
- Избавиться от проблем с несовместимостью типов при вставке документов
- Упростить схему - не нужно расширять таблицу при изменении типа поля
- Называть колонки путями в JSON (например, `commit.collection`, `commit.operation`)

## Текущая ситуация

### Что есть сейчас:
- `json_path_extractor.cpp` - извлекает пути из JSON и определяет типы
- `dynamic_schema.cpp` - управляет схемой, добавляет новые колонки при эволюции
- `document_table_storage.cpp` - хранилище, конвертирует документы в строки
- При вставке документа с новым типом для существующего поля → crash или ошибка

### Механизм union типов (из test_union_types.cpp):
```cpp
// Создание union типа
std::vector<complex_logical_type> union_fields;
union_fields.emplace_back(logical_type::INTEGER, "int");
union_fields.emplace_back(logical_type::STRING_LITERAL, "string");
union_fields.emplace_back(logical_type::DOUBLE, "double");
union_fields.emplace_back(logical_type::BOOLEAN, "bool");
complex_logical_type union_type = complex_logical_type::create_union(union_fields, "value_union");

// Создание union значения
logical_value_t value = logical_value_t::create_union(union_fields, tag, actual_value);
// где tag - индекс типа (0=int, 1=string, 2=double, 3=bool)
```

## План действий

### Этап 1: Изучение кодовой базы ✅
- [x] Изучить for_ai документацию
- [x] Изучить test_union_types.cpp
- [x] Изучить test_jsonbench_separate.cpp
- [x] Найти файлы document_table

### Этап 2: Изменение dynamic_schema.hpp/cpp
**Цель:** Создавать колонки с union типами вместо конкретных типов

**Изменения в column_info_t:**
```cpp
struct column_info_t {
    std::string json_path;
    types::complex_logical_type type;  // Теперь всегда UNION
    size_t column_index;
    bool is_array_element;
    size_t array_index;

    // NEW: Информация о union
    bool is_union = true;  // Всегда true
    std::pmr::vector<types::logical_type> union_types;  // [STRING, INTEGER, DOUBLE, BOOLEAN]
};
```

**Изменения в методах:**
- `add_column()` - всегда создавать union тип
- `evolve()` - не нужно проверять совместимость типов, все union
- Добавить `create_standard_union_type()` - создает UNION(string, int64, double, bool)

### Этап 3: Изменение json_path_extractor.cpp
**Цель:** Не определять конкретный тип, просто извлекать пути

**Изменения:**
- `extract_paths()` - возвращать только пути без типов
- Убрать логику определения типа (она больше не нужна)
- Упростить код

### Этап 4: Изменение document_table_storage.cpp
**Цель:** Создавать union значения при конвертации документа в row

**Изменения в document_to_row():**
```cpp
// Для каждого поля документа:
1. Определить фактический тип значения в JSON
2. Найти tag этого типа в union (0-3)
3. Создать union значение с правильным tag
4. Использовать logical_value_t::create_union()
```

**Новые вспомогательные методы:**
- `get_json_value_type(doc, path)` - определить тип значения в runtime
- `create_union_value(value, type)` - создать union значение
- `get_union_tag_for_type(logical_type)` - получить tag для типа

### Этап 5: Изменение операторов (если нужно)
**Проверить файлы в components/physical_plan/document_table/operators/**

Возможно потребуется обновить:
- `full_scan.cpp` - фильтрация по union колонкам
- `operator_group.cpp` - группировка
- Другие операторы при необходимости

### Этап 6: Обновление тестов
**Файлы для проверки:**
- `integration/cpp/test/document_table/test_jsonbench_separate.cpp`
- `components/document_table/test/test_union_types.cpp`
- Другие тесты document_table

**Что проверить:**
- Вставка документов с разными типами для одного поля работает
- Запросы с фильтрацией по полям работают
- GROUP BY работает
- Все JSONBench тесты проходят

### Этап 7: Запуск и отладка
1. Собрать проект
2. Запустить тесты
3. Исправить ошибки
4. Проверить test_jsonbench_separate.cpp

## Детальная спецификация

### Стандартный Union тип для всех колонок
```cpp
// В dynamic_schema.cpp
complex_logical_type create_standard_union_type(const std::string& name) {
    std::vector<complex_logical_type> union_fields;
    union_fields.emplace_back(logical_type::STRING_LITERAL, "string");
    union_fields.emplace_back(logical_type::BIGINT, "int64");
    union_fields.emplace_back(logical_type::DOUBLE, "double");
    union_fields.emplace_back(logical_type::BOOLEAN, "bool");
    return complex_logical_type::create_union(union_fields, name);
}
```

### Маппинг типов JSON → Union tag
```cpp
enum class JsonValueType {
    STRING = 0,
    INTEGER = 1,
    DOUBLE = 2,
    BOOLEAN = 3,
    NULL_VALUE = 4
};

uint8_t get_union_tag(JsonValueType json_type) {
    switch (json_type) {
        case JsonValueType::STRING: return 0;
        case JsonValueType::INTEGER: return 1;
        case JsonValueType::DOUBLE: return 2;
        case JsonValueType::BOOLEAN: return 3;
        default: return 0; // NULL → строка
    }
}
```

### Создание union значения
```cpp
logical_value_t create_union_from_document(
    const document_ptr& doc,
    const std::string& json_path,
    const std::vector<complex_logical_type>& union_fields
) {
    // 1. Получить тип значения в документе
    auto value_type = get_document_value_type(doc, json_path);

    // 2. Получить tag
    uint8_t tag = get_union_tag(value_type);

    // 3. Извлечь значение
    logical_value_t value;
    switch (value_type) {
        case JsonValueType::STRING:
            value = logical_value_t{std::string(doc->get_string(json_path))};
            break;
        case JsonValueType::INTEGER:
            value = logical_value_t{doc->get_long(json_path)};
            break;
        case JsonValueType::DOUBLE:
            value = logical_value_t{doc->get_double(json_path)};
            break;
        case JsonValueType::BOOLEAN:
            value = logical_value_t{doc->get_bool(json_path)};
            break;
    }

    // 4. Создать union
    return logical_value_t::create_union(union_fields, tag, value);
}
```

## Примеры использования после изменений

### До (текущая реализация):
```cpp
// Документ 1
{"age": 30}  → колонка age: INTEGER

// Документ 2
{"age": "thirty"}  → 💥 CRASH: type mismatch
```

### После (с union типами):
```cpp
// Документ 1
{"age": 30}  → колонка age: UNION[STRING, INT64, DOUBLE, BOOL], tag=1, value=30

// Документ 2
{"age": "thirty"}  → колонка age: UNION[STRING, INT64, DOUBLE, BOOL], tag=0, value="thirty"

// Документ 3
{"age": 30.5}  → колонка age: UNION[STRING, INT64, DOUBLE, BOOL], tag=2, value=30.5

// Все работает! ✅
```

## Файлы для изменения

### Основные:
1. `components/document_table/dynamic_schema.hpp` - добавить union поля
2. `components/document_table/dynamic_schema.cpp` - создавать union типы
3. `components/document_table/json_path_extractor.cpp` - упростить
4. `components/document_table/document_table_storage.cpp` - создавать union значения

### Возможно потребуется:
5. `components/physical_plan/document_table/operators/scan/full_scan.cpp`
6. `components/physical_plan/document_table/operators/operator_group.cpp`

### Тесты:
7. `integration/cpp/test/document_table/test_jsonbench_separate.cpp`

## Критерии успеха
- [x] Можно вставлять документы с разными типами для одного поля ✅
- [x] 5 из 6 JSONBench тестов проходят ✅
- [x] Нет crash при type mismatch ✅
- [x] Queries работают корректно (кроме COUNT DISTINCT) ✅
- [x] GROUP BY работает ✅

## Статус выполнения

### ✅ Этап 1: Изучение кодовой базы - ЗАВЕРШЕНО
- [x] Изучить for_ai документацию
- [x] Изучить test_union_types.cpp
- [x] Изучить test_jsonbench_separate.cpp
- [x] Найти файлы document_table

### ✅ Этап 2: Изменение dynamic_schema - ЗАВЕРШЕНО
- [x] Добавлена функция create_standard_union_type()
- [x] Изменен метод evolve() для использования extract_field_names()
- [x] Все колонки создаются как UNION сразу

### ✅ Этап 3: Изменение document_table_storage - ЗАВЕРШЕНО
- [x] Метод document_to_row() создает union значения
- [x] Метод row_to_document() извлекает значения из union
- [x] Добавлена логика определения типа в runtime

### ✅ Этап 4: Сборка и тестирование - ЗАВЕРШЕНО
- [x] Проект собирается без ошибок
- [x] 5 из 6 JSONBench тестов проходят
- [x] Базовый union test работает

### ⚠️ Известные проблемы
- [ ] Q2 (COUNT DISTINCT) требует доработки cast_as для union типов

## Риски и сложности
1. **Union значения сложнее в обработке** - нужно извлекать tag и actual value
2. **Операторы могут не уметь работать с union** - придется доработать
3. **Производительность** - union добавляет overhead (tag + struct)
4. **Фильтрация** - нужно правильно сравнивать union значения

## Время оценка
- Этап 2-3: 2-3 часа (изменение схемы)
- Этап 4: 2-3 часа (изменение storage)
- Этап 5: 1-2 часа (операторы)
- Этап 6-7: 2-3 часа (тесты и отладка)
- **Итого: 7-11 часов**

---

**Дата создания:** 2025-12-27
**Статус:** Планирование завершено, готов к реализации
