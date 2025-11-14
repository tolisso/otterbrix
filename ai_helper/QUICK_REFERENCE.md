# Quick Reference - components/table

Быстрая справка по основным классам и их использованию.

---

## Основные классы (кратко)

| Класс | Назначение | Файл |
|-------|-----------|------|
| `data_table_t` | Основная таблица | `data_table.hpp` |
| `collection_t` | Коллекция row groups | `collection.hpp` |
| `row_group_t` | Группа строк (~2048 строк) | `row_group.hpp` |
| `column_data_t` | Базовый класс колонки | `column_data.hpp` |
| `column_segment_t` | Физический сегмент данных | `column_segment.hpp` |
| `column_definition_t` | Определение колонки | `column_definition.hpp` |
| `block_manager_t` | Управление блоками на диске | `storage/block_manager.hpp` |
| `buffer_manager_t` | Буферный пул | `storage/buffer_manager.hpp` |

---

## Наследники column_data_t

| Класс | Для каких типов | Файл |
|-------|----------------|------|
| `standard_column_data_t` | INT, VARCHAR, DOUBLE, BIGINT, etc. | `standard_column_data.hpp` |
| `struct_column_data_t` | STRUCT (композитные типы) | `struct_column_data.hpp` |
| `list_column_data_t` | LIST (переменная длина) | `list_column_data.hpp` |
| `array_column_data_t` | ARRAY (фиксированная длина) | `array_column_data.hpp` |
| `json_column_data_t` | JSON | `json_column_data.hpp` |
| `validity_column_data_t` | NULL значения (битовая маска) | `validity_column_data.hpp` |

---

## Основные операции

### CREATE TABLE
```cpp
// 1. Определения колонок
std::vector<column_definition_t> columns;
columns.emplace_back("id", complex_logical_type::create_bigint());
columns.emplace_back("name", complex_logical_type::create_varchar());

// 2. Создание таблицы
data_table_t table(resource, block_manager, std::move(columns), "users");
```

### INSERT
```cpp
// 1. Подготовка данных
vector::data_chunk_t chunk;
chunk.initialize(table.columns());
chunk.set_value(0, 0, logical_value_t{1LL});
chunk.set_value(1, 0, logical_value_t{"Alice"});

// 2. Вставка
table_append_state state;
table.initialize_append(state);
table.append(chunk, state);
table.commit_append(0, 1);
```

### SELECT
```cpp
// 1. Инициализация сканирования
table_scan_state state;
std::vector<storage_index_t> column_ids = {0, 1, 2};
table.initialize_scan(state, column_ids);

// 2. Чтение данных
vector::data_chunk_t result;
table.scan(result, state);
```

### UPDATE
```cpp
// 1. Подготовка обновлений
vector::data_chunk_t updates;
updates.initialize(/* column types */);
updates.set_value(0, 0, logical_value_t{30}); // новое значение

// 2. Обновление
int64_t row_ids[] = {10, 20, 30};
table.update_column(row_ids, {2}, updates); // обновить колонку 2
```

### DELETE
```cpp
auto delete_state = table.initialize_delete(constraints);
int64_t row_ids[] = {10, 20, 30};
uint64_t deleted = table.delete_rows(*delete_state, row_ids_vector, 3);
```

---

## State объекты

| State класс | Для операции | Содержит |
|-------------|--------------|----------|
| `table_scan_state` | SELECT | collection_scan_state, column_ids, filter |
| `table_append_state` | INSERT | row_group_append_state |
| `table_update_state` | UPDATE | constraint_state |
| `table_delete_state` | DELETE | constraint_state |
| `column_scan_state` | Сканирование колонки | current segment, row_index, child_states |
| `column_append_state` | Вставка в колонку | current_segment, child_appends |
| `column_fetch_state` | Fetch по row_id | child_states |

---

## Важные методы column_data_t

| Метод | Назначение |
|-------|-----------|
| `create_column()` | **Фабричный метод** - создает нужный тип column_data_t |
| `scan()` | Чтение данных из колонки |
| `append()` | Добавление данных в колонку |
| `update()` | Обновление значений в колонке |
| `fetch()` | Получение конкретной строки по row_id |
| `initialize_scan()` | Инициализация состояния сканирования |
| `initialize_append()` | Инициализация состояния вставки |

---

## Важные методы row_group_t

| Метод | Назначение |
|-------|-----------|
| `initialize_scan()` | Начать сканирование группы |
| `scan()` | Прочитать данные |
| `append()` | Добавить данные в группу |
| `commit_append()` | Зафиксировать добавление |
| `delete_rows()` | Удалить строки |
| `update()` | Обновить строки |

---

## Segment Tree

```cpp
// В column_data_t
segment_tree_t<column_segment_t> data_;

// Основные методы:
root_segment()              // Получить первый сегмент
get_segment(row_idx)        // Найти сегмент по индексу строки
append_segment(segment)     // Добавить новый сегмент
```

---

## Block Manager

```cpp
// Создание блока
auto block_id = block_manager.free_block_id();
auto block = block_manager.create_block(block_id, nullptr);

// Чтение блока
block_manager.read(*block);

// Запись блока
block_manager.write(*block, block_id);

// Регистрация handle
auto handle = block_manager.register_block(block_id);
```

---

## Buffer Manager

```cpp
// Выделение памяти
auto handle = buffer_manager.allocate(memory_tag::BASE_TABLE, size);

// Pin/Unpin блока
auto buffer_handle = buffer_manager.pin(block_handle);
// ... работа с данными ...
buffer_manager.unpin(block_handle);
```

---

## Типичные паттерны кода

### Паттерн 1: Обход всех row groups
```cpp
for (auto& row_group_node : collection.row_groups_.segments()) {
    row_group_t* rg = row_group_node.node.get();
    // Работа с row_group
}
```

### Паттерн 2: Обход всех сегментов колонки
```cpp
for (auto& segment_node : column_data.data_.segments()) {
    column_segment_t* seg = segment_node.node.get();
    // Работа с сегментом
}
```

### Паттерн 3: Создание child column_data для struct
```cpp
// В struct_column_data_t конструкторе
for (size_t i = 0; i < struct_type.child_count(); i++) {
    auto& child_type = struct_type.child_type(i);
    child_columns_.push_back(
        column_data_t::create_column(
            resource, block_manager, i, start_row, child_type, this
        )
    );
}
```

### Паттерн 4: Работа с validity (NULL значения)
```cpp
// В standard_column_data_t
validity_column_data_t validity;

// При сканировании
validity.scan(vector_index, state.child_states[0], result, target_count);

// При вставке
validity.append_data(state.child_appends[0], uvf, count);
```

### Паттерн 5: RAII для блокировок
```cpp
auto lock = segment_tree.lock(); // std::unique_lock<std::mutex>
// ... критическая секция ...
// lock автоматически освобождается
```

---

## Физическое хранение

### Размеры
```
DEFAULT_BLOCK_ALLOC_SIZE = 262144 байта (256 KB)
DEFAULT_BLOCK_HEADER_SIZE = 64 байта
Полезный размер блока = 262080 байт

DEFAULT_VECTOR_CAPACITY = 2048 строк
MAX_ROW_GROUP_SIZE = 2^30 строк
```

### Расположение на диске
```
Блок (262144 байта)
├── Header (64 байта)
│   ├── block_id (4 байта)
│   ├── metadata
│   └── checksum
└── Data (262080 байт)
    └── Сегменты column_segment_t
```

---

## Memory Tags (для buffer_manager)

```cpp
enum class memory_tag : uint8_t {
    BASE_TABLE,
    HASH_TABLE,
    OVERFLOW_STRINGS,
    IN_MEMORY_TABLE,
    // ... и другие
};
```

---

## Константы

```cpp
// В row_group.hpp
constexpr uint64_t MAX_ROW_GROUP_SIZE = (1ULL << 30); // 2^30

// В column_data.hpp
constexpr uint64_t MAX_ROW_ID = 36028797018960000ULL; // 2^55

// В vector.hpp
constexpr uint64_t DEFAULT_VECTOR_CAPACITY = 2048;

// В storage/buffer_manager.hpp
constexpr size_t DEFAULT_BLOCK_ALLOC_SIZE = (1ULL << 18); // 262144
```

---

## Enums

### scan_vector_type
```cpp
enum class scan_vector_type : uint8_t {
    SCAN_FLAT_VECTOR = 0,
    SCAN_ENTIRE_VECTOR = 1
};
```

### table_scan_type
```cpp
enum class table_scan_type : uint8_t {
    TABLE_SCAN_REGULAR,
    TABLE_SCAN_COMMITTED,
    TABLE_SCAN_UNCOMMITTED
};
```

### filter_propagate_result_t
```cpp
enum class filter_propagate_result_t : uint8_t {
    NO_PRUNING_POSSIBLE = 0,
    ALWAYS_TRUE = 1,
    ALWAYS_FALSE = 2,
    TRUE_OR_NULL = 3,
    FALSE_OR_NULL = 4
};
```

---

## Частые ошибки и как их избежать

### ❌ Ошибка 1: Забыть commit_append()
```cpp
table.append(chunk, state);
// Забыли commit_append() - данные не будут видны!
```
✅ **Правильно:**
```cpp
table.append(chunk, state);
table.commit_append(0, count);
```

### ❌ Ошибка 2: Неправильный порядок блокировок
```cpp
auto lock2 = tree2.lock();
auto lock1 = tree1.lock();  // Может быть deadlock!
```
✅ **Правильно:** Всегда один и тот же порядок блокировок

### ❌ Ошибка 3: Использовать физический тип для JSON
```cpp
if (type.to_physical_type() == physical_type::INT64) {
    // Это сработает и для JSON! JSON физически = INT64
}
```
✅ **Правильно:**
```cpp
if (type.type() == logical_type::JSON) {
    return std::make_unique<json_column_data_t>(...);
} else if (type.to_physical_type() == physical_type::INT64) {
    return std::make_unique<standard_column_data_t>(...);
}
```

### ❌ Ошибка 4: Забыть child_states
```cpp
// В scan методе для struct/list
column_data_t::scan(...); // Забыли обработать дочерние колонки
```
✅ **Правильно:**
```cpp
column_data_t::scan(...);
for (size_t i = 0; i < child_columns_.size(); i++) {
    child_columns_[i]->scan(state.child_states[i], ...);
}
```

---

## Debugging Tips

### Проверка состояния row_group
```cpp
std::cout << "Row group: start=" << rg->start
          << " count=" << rg->count
          << " columns=" << rg->columns_.size() << "\n";
```

### Проверка сегментов колонки
```cpp
for (auto& seg_node : col->data_.segments()) {
    auto* seg = seg_node.node.get();
    std::cout << "Segment: start=" << seg->start
              << " count=" << seg->count
              << " block_id=" << seg->block_id << "\n";
}
```

### Проверка типа column_data
```cpp
if (auto* std_col = dynamic_cast<standard_column_data_t*>(col.get())) {
    std::cout << "Standard column\n";
} else if (auto* json_col = dynamic_cast<json_column_data_t*>(col.get())) {
    std::cout << "JSON column: " << json_col->auxiliary_table_name_ << "\n";
}
```

---

## Полезные макросы и функции

### Assert (для отладки)
```cpp
assert(state.child_states.size() == 1);
assert(row_id < MAX_ROW_ID);
```

### Move семантика
```cpp
// Для больших объектов
auto col = std::move(column);
table.append(std::move(chunk), state);
```

### Smart pointers
```cpp
// unique_ptr - единоличное владение
std::unique_ptr<column_data_t> col = ...;

// shared_ptr - разделяемое владение
std::shared_ptr<collection_t> collection = ...;

// weak_ptr - слабая ссылка (не увеличивает счетчик)
std::weak_ptr<block_handle_t> weak_handle = ...;
```

---

## Связь с другими компонентами

### components/types
```cpp
#include <components/types/types.hpp>

types::complex_logical_type type = complex_logical_type::create_bigint();
types::logical_value_t value{42LL};
```

### components/vector
```cpp
#include <components/vector/vector.hpp>
#include <components/vector/data_chunk.hpp>

vector::vector_t vec(types::physical_type::INT64, 2048);
vector::data_chunk_t chunk;
```

### components/expressions
```cpp
#include <components/expressions/table_filter.hpp>

table_filter_t filter = ...; // WHERE условие
```

---

## Тестирование

### Unit тест для column_data_t
```cpp
TEST_CASE("column_data append and scan") {
    auto resource = std::pmr::get_default_resource();
    auto block_manager = create_test_block_manager();

    auto col = column_data_t::create_column(
        resource, *block_manager, 0, 0,
        complex_logical_type::create_bigint()
    );

    // Append
    column_append_state append_state;
    col->initialize_append(append_state);
    // ... append data

    // Scan
    column_scan_state scan_state;
    col->initialize_scan(scan_state);
    vector::vector_t result(...);
    col->scan(0, scan_state, result, 10);

    // Verify
    REQUIRE(result.count() == 10);
}
```

---

## Cheatsheet: Какой класс использовать?

| Задача | Используй |
|--------|-----------|
| Создать таблицу | `data_table_t` |
| Добавить данные | `data_table_t::append()` |
| Прочитать данные | `data_table_t::scan()` |
| Получить строку по ID | `data_table_t::fetch()` |
| Обновить данные | `data_table_t::update()` |
| Удалить строки | `data_table_t::delete_rows()` |
| Создать новый тип колонки | Наследник от `column_data_t` |
| Управлять блоками | `block_manager_t` |
| Управлять памятью | `buffer_manager_t` |
| Хранить метаданные колонки | `column_definition_t` |

---

Эта справка покрывает 90% случаев использования components/table!
