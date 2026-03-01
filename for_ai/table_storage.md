# table_storage_t — динамическая схема в обычном table

## Текущее состояние (актуально)

`document_table` удалён. Вся его функциональность перенесена в `table_storage_t`.

## Архитектура хранилищ

```
context_collection_t
├── DOCUMENT_BTREE         → document_storage_ (B-tree)
└── TABLE_COLUMNS          → table_storage_t
    ├── фиксированная схема  (has_dynamic_schema = false)
    └── динамическая схема   (has_dynamic_schema = true)
```

## Где живёт код

```
services/collection/
  ├── collection.hpp          — context_collection_t, table_storage_t, column_info_t
  ├── table_storage.cpp       — реализация методов динамической схемы
  ├── json_path_extractor.hpp — извлечение JSON paths из документов
  └── json_path_extractor.cpp
```

## table_storage_t: динамическая схема

Создаётся через конструктор с `bool dynamic_schema = true`:

```cpp
// В memory_storage.cpp при CREATE TABLE ... () с пустой схемой:
new context_collection_t(resource, name, true /*dynamic_schema*/, disk, log)
```

### Поддерживаемые операции

| Метод | Назначение |
|-------|-----------|
| `prepare_insert(documents)` | API insert: эволюция схемы + конвертация документов в data_chunk |
| `evolve_schema_from_types(types)` | SQL INSERT VALUES: добавить новые колонки из типов chunk'а |
| `has_column(json_path)` | Проверить наличие колонки |
| `get_column_info(json_path)` | Метаданные колонки по JSON path |
| `get_column_by_index(i)` | Метаданные колонки по индексу |
| `column_count()` | Количество колонок |
| `table()` | Доступ к `data_table_t` |

### Эволюция схемы

При вставке нового документа с неизвестными полями:
1. `evolve_from_document(doc)` — определяет новые колонки
2. `evolve_schema(new_cols)` — расширяет `data_table_t` через конструктор `data_table_t(parent, new_column)`
3. Существующие строки получают NULL для новых колонок автоматически

### JSON path → имя колонки

- `user.name` → `user_dot_name`
- `tags[0]` → `tags_arr0_`
- Обратная конвертация при чтении: `column_name_to_document_path()` (в `table_storage.cpp`)

## SQL синтаксис

```sql
-- Создание таблицы с динамической схемой
CREATE TABLE db.users() WITH (storage='document_table');
-- или просто (пустая схема → dynamic schema по умолчанию):
CREATE TABLE db.users();

-- Вставка: схема расширяется автоматически
INSERT INTO db.users (_id, name, age) VALUES ('u1', 'Alice', 30);
INSERT INTO db.users (_id, name, city) VALUES ('u2', 'Bob', 'Moscow');
-- Схема теперь: [_id, name, age, city], u1.city = NULL
```

## Интеграция с executor

В `executor.cpp::execute_plan()`:
```cpp
if (it->second->has_dynamic_schema()) {
    auto& storage = it->second->table_storage();
    if (data_node->uses_documents()) {
        auto chunk = storage.prepare_insert(pairs);  // API path
    } else {
        storage.evolve_schema_from_types(types);     // SQL path
        // padding chunk to full schema...
    }
}
// Routing через table::planner (как обычная table)
plan = table::planner::create_plan(...);
```

## Что было удалено

- `components/document_table/` — весь компонент
- `document_table_storage_wrapper_t` из `collection.hpp`
- `otterbrix::document_table` из CMakeLists
- `services::document_table::planner` namespace из `create_plan.hpp`

## Тесты

Тесты переименованы:
- `test_document_table_sql.cpp` → `test_dynamic_table_sql.cpp`
- `test_document_table_primary_key.cpp` → `test_dynamic_table_primary_key.cpp`
- `test_document_table_catalog.cpp` → `test_dynamic_table_catalog.cpp`
