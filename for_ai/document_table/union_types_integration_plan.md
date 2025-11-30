# План интеграции Union типов в document_table

## Проблема

Сейчас `dynamic_schema_t::evolve()` выбрасывает исключение, если один и тот же JSON path имеет разные типы в разных документах:

```cpp
// dynamic_schema.cpp:93-98
if (existing_type != new_type) {
    throw std::runtime_error("Type mismatch for path...");
}
```

**Пример проблемного случая:**
```json
{"age": 30}        // age - INT32
{"age": "thirty"}  // age - STRING  ❌ Type mismatch exception!
```

## Решение с Union типами

Использовать `complex_logical_type::create_union()` и `logical_value_t::create_union()` для хранения разных типов по одному пути.

### Как работают Union типы в Otterbrix

```cpp
// Создание union типа
std::vector<complex_logical_type> types = {
    complex_logical_type(logical_type::INT32),
    complex_logical_type(logical_type::STRING_LITERAL)
};
auto union_type = complex_logical_type::create_union(types);

// Создание значения (INT32, tag=0)
auto value1 = logical_value_t::create_union(
    types,          // Все типы union
    0,              // tag - какой тип активен (INT32)
    logical_value_t(42)  // Значение
);

// Создание значения (STRING, tag=1)
auto value2 = logical_value_t::create_union(
    types,
    1,              // tag - STRING активен
    logical_value_t("thirty")
);
```

## План реализации

### Шаг 1: Расширить `column_info_t`

```cpp
// dynamic_schema.hpp
struct column_info_t {
    std::string json_path;
    types::complex_logical_type type;  // Может быть UNION!
    size_t column_index;
    bool is_array_element;
    size_t array_index;
    
    // НОВОЕ: информация для union типов
    bool is_union;                                  // Является ли колонка union?
    std::pmr::vector<types::logical_type> union_types;  // Типы в union (в порядке добавления)
};
```

### Шаг 2: Модифицировать `dynamic_schema_t::evolve()`

Вместо exception при type mismatch:

```cpp
// dynamic_schema.cpp:evolve()
if (has_path(path_info.path)) {
    const auto* existing_col = get_column_info(path_info.path);
    auto existing_type = existing_col->type.type();
    auto new_type = path_info.type;
    
    if (existing_type != new_type) {
        // ВМЕСТО EXCEPTION - создать/расширить union
        if (existing_col->is_union) {
            // Уже union - добавить новый тип если его нет
            extend_union_column(path_info.path, new_type);
        } else {
            // Первое несовпадение - создать union из двух типов
            create_union_column(path_info.path, existing_type, new_type);
        }
        return new_columns; // Схема изменена!
    }
    continue;
}
```

### Шаг 3: Добавить новые методы в `dynamic_schema_t`

```cpp
// dynamic_schema.hpp
class dynamic_schema_t {
    // ...
    
private:
    // Создание union колонки из двух типов
    void create_union_column(const std::string& json_path,
                            types::logical_type type1,
                            types::logical_type type2);
    
    // Расширение существующего union новым типом
    void extend_union_column(const std::string& json_path,
                            types::logical_type new_type);
    
    // Получение tag для типа в union
    uint8_t get_union_tag(const column_info_t* col, types::logical_type type) const;
};
```

### Шаг 4: Реализовать `create_union_column()`

```cpp
// dynamic_schema.cpp
void dynamic_schema_t::create_union_column(const std::string& json_path,
                                          types::logical_type type1,
                                          types::logical_type type2) {
    auto it = path_to_index_.find(json_path);
    if (it == path_to_index_.end()) return;
    
    auto& col = columns_[it->second];
    
    // Создаем union с двумя типами
    std::vector<types::complex_logical_type> union_members = {
        types::complex_logical_type(type1),
        types::complex_logical_type(type2)
    };
    
    col.type = types::complex_logical_type::create_union(std::move(union_members));
    col.is_union = true;
    col.union_types.push_back(type1);
    col.union_types.push_back(type2);
}
```

### Шаг 5: Реализовать `extend_union_column()`

```cpp
void dynamic_schema_t::extend_union_column(const std::string& json_path,
                                          types::logical_type new_type) {
    auto it = path_to_index_.find(json_path);
    if (it == path_to_index_.end()) return;
    
    auto& col = columns_[it->second];
    
    // Проверяем, что тип еще не в union
    if (std::find(col.union_types.begin(), col.union_types.end(), new_type) 
        != col.union_types.end()) {
        return; // Уже есть
    }
    
    // Добавляем новый тип в union
    col.union_types.push_back(new_type);
    
    // Пересоздаем union тип
    std::vector<types::complex_logical_type> union_members;
    for (auto t : col.union_types) {
        union_members.emplace_back(t);
    }
    
    col.type = types::complex_logical_type::create_union(std::move(union_members));
}
```

### Шаг 6: Реализовать `get_union_tag()`

```cpp
uint8_t dynamic_schema_t::get_union_tag(const column_info_t* col,
                                       types::logical_type type) const {
    if (!col || !col->is_union) return 0;
    
    auto it = std::find(col->union_types.begin(), col->union_types.end(), type);
    if (it == col->union_types.end()) {
        throw std::runtime_error("Type not found in union");
    }
    
    return static_cast<uint8_t>(std::distance(col->union_types.begin(), it));
}
```

### Шаг 7: Модифицировать `document_to_row()` в `document_table_storage.cpp`

```cpp
// document_table_storage.cpp:document_to_row()
for (size_t i = 0; i < schema_->column_count(); ++i) {
    const auto* col_info = schema_->get_column_by_index(i);
    auto& vec = chunk.data[i];
    
    // ... специальная обработка _id ...
    
    // Проверяем, есть ли поле в документе
    if (!doc->is_exists(col_info->json_path)) {
        vec.set_null(0, true);
        continue;
    }
    
    // Извлекаем значение
    auto extracted_value = extract_value(doc, col_info);
    
    // НОВОЕ: если колонка - union, оборачиваем значение в union
    if (col_info->is_union) {
        auto value_type = extracted_value.type().type();
        uint8_t tag = schema_->get_union_tag(col_info, value_type);
        
        // Создаем union типы для create_union
        std::vector<types::complex_logical_type> union_types;
        for (auto t : col_info->union_types) {
            union_types.emplace_back(t);
        }
        
        // Создаем union значение
        auto union_value = types::logical_value_t::create_union(
            std::move(union_types),
            tag,
            std::move(extracted_value)
        );
        
        vec.set_value(0, std::move(union_value));
    } else {
        // Обычное значение
        vec.set_value(0, std::move(extracted_value));
    }
}
```

### Шаг 8: Обработка при чтении (если нужно)

При чтении union значений через `get()` или `scan()`, может потребоваться:
- Извлечение активного типа (через tag)
- Преобразование union обратно в простой тип

```cpp
// Чтение union значения
auto union_val = vec.get_value(row_idx);
if (union_val.type().type() == types::logical_type::UNION) {
    auto& children = union_val.children();
    uint8_t tag = children[0].value<uint8_t>();  // Первый элемент - tag
    auto& actual_value = children[tag + 1];      // +1 из-за tag поля
    
    return actual_value;
}
```

### Шаг 9: Обновление существующих данных при эволюции

Когда union создается, старые данные уже хранят простые значения. Нужно:

**Вариант A: Lazy conversion**
- Оставить старые данные как есть
- При чтении проверять: если колонка union, а значение нет - оборачивать на лету

**Вариант B: Eager conversion** (сложнее, но чище)
- При создании union, пересканировать все данные
- Обернуть существующие значения в union с правильным tag

Рекомендуется начать с Варианта A.

## Пример использования

```cpp
// Документы с разными типами
doc1: {"age": 30}           // INT32
doc2: {"age": "thirty"}     // STRING
doc3: {"age": 30.5}         // FLOAT

// После вставки всех документов:
// age колонка имеет union тип [INT32, STRING, FLOAT]

// Хранимые значения:
// row 0: union(tag=0, value=30)       - INT32
// row 1: union(tag=1, value="thirty") - STRING  
// row 2: union(tag=2, value=30.5)     - FLOAT
```

## Альтернативный подход: Schema on Write

Вместо union, можно:
1. Создавать отдельные колонки для каждого типа: `age_int`, `age_string`, `age_float`
2. Автоматически выбирать правильную колонку при записи

**Плюсы:**
- Проще реализовать
- Лучшая производительность при чтении

**Минусы:**
- Множество колонок (взрыв схемы)
- Сложнее в запросах (нужно объединять `age_int OR age_string`)

**Union типы - более элегантное решение!**

## Тестирование

```cpp
// Тест
TEST(DynamicSchemaUnion, BasicUnion) {
    auto resource = std::pmr::new_delete_resource();
    dynamic_schema_t schema(resource);
    
    // Документ 1: age - INT
    auto doc1 = create_doc(R"({"age": 30})");
    schema.evolve(doc1);
    
    auto* col = schema.get_column_info("age");
    ASSERT_EQ(col->type.type(), types::logical_type::INT32);
    ASSERT_FALSE(col->is_union);
    
    // Документ 2: age - STRING (должен создать union!)
    auto doc2 = create_doc(R"({"age": "thirty"})");
    schema.evolve(doc2);
    
    col = schema.get_column_info("age");
    ASSERT_EQ(col->type.type(), types::logical_type::UNION);
    ASSERT_TRUE(col->is_union);
    ASSERT_EQ(col->union_types.size(), 2);
    ASSERT_EQ(col->union_types[0], types::logical_type::INT32);
    ASSERT_EQ(col->union_types[1], types::logical_type::STRING_LITERAL);
}
```

## Следующие шаги

1. ✅ Создать план (этот файл)
2. ⏭️ Расширить `column_info_t` с union полями
3. ⏭️ Реализовать `create_union_column()` и `extend_union_column()`
4. ⏭️ Модифицировать `evolve()` - убрать exception, создавать union
5. ⏭️ Модифицировать `document_to_row()` - оборачивать в union при нужде
6. ⏭️ Добавить тесты
7. ⏭️ Обработать edge cases (array элементы, вложенные структуры)

## Заметки

- Union типы уже реализованы в Otterbrix (для Arrow compatibility)
- Нужно только интегрировать их в document_table
- Первая версия может не поддерживать union в nested структурах
- Lazy conversion проще для старта

---

**Статус**: План готов к реализации  
**Дата**: 30 ноября 2024

