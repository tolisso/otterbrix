# Архитектура хранения данных в Otterbrix

## Обзор

Otterbrix использует **единую структуру контекста** (`context_collection_t`) для хранения **всех типов данных** - как документов без схемы, так и таблиц с фиксированной схемой. Внутри контекста есть два хранилища, и выбор между ними определяется наличием схемы при создании.

## Ключевые компоненты

### 1. Контекст коллекции (services/collection/collection.hpp)

```cpp
class context_collection_t {
    document_storage_t document_storage_;  // B-tree для документов
    table_storage_t table_storage_;        // data_table для колоночного хранения
    bool uses_datatable_;                  // флаг использования table_storage
};
```

**Важно:** Оба хранилища создаются всегда, но используется только одно!

### 2. Два набора операторов

Операторы физического плана разделены на два параллельных набора:

- **`components/physical_plan/collection/operators/`** - для документного хранения
  - Используют `context_->document_storage()` (B-tree)
  - Работают с `std::pmr::vector<document_ptr>`
  - Row-oriented обработка

- **`components/physical_plan/table/operators/`** - для табличного хранения
  - Используют `context_->table_storage().table()` (data_table)
  - Работают с `vector::data_chunk_t` (колоночный формат)
  - Column-oriented batch-обработка

**Важно:** Оба набора операторов принимают один тип контекста - `services::collection::context_collection_t*`

## Поток выполнения запросов

### CREATE COLLECTION без схемы

```
SQL: CREATE COLLECTION users()

↓ SQL Parser
node_create_collection (schema пустая)

↓ Dispatcher (dispatcher.cpp:676)
catalog.create_computing_table(id)

↓ Catalog (catalog.cpp:62)
namespaces.computing[id] = ...
get_table_format(id) → used_format_t::documents

↓ Executor (executor.cpp:95)
collection::planner::create_plan()

↓ Planner (components/physical_plan_generator/create_plan.cpp)
Создает операторы из collection/operators/

↓ Collection Operators
context_->document_storage().insert_or_assign(...)

↓ Storage
B-tree хранение документов
```

### CREATE TABLE со схемой

```
SQL: CREATE TABLE users(id int, name string)

↓ SQL Parser (transform_table.cpp:55-78)
node_create_collection (schema заполнена!)

↓ Dispatcher (dispatcher.cpp:690)
catalog.create_table(id, metadata)

↓ Catalog (catalog.cpp:55)
namespaces.tables[id] = metadata
get_table_format(id) → used_format_t::columns

↓ Executor (executor.cpp:100)
table::planner::create_plan()

↓ Planner
Создает операторы из table/operators/

↓ Table Operators
context_->table_storage().table().append(...)

↓ Storage
Колоночное хранение в data_table
```

## Ключевые различия операторов

### Full Scan

**Collection** (collection/operators/scan/full_scan.cpp:21):
```cpp
for (auto& it : context_->document_storage()) {
    if (predicate_->check(it.second, parameters)) {
        output_->append(it.second);
    }
}
```

**Table** (table/operators/scan/full_scan.cpp:67-79):
```cpp
auto types = context_->table_storage().table().copy_types();
context_->table_storage().table().initialize_scan(state, column_indices, filter);
context_->table_storage().table().scan(output_->data_chunk(), state);
```

### Insert

**Collection** (collection/operators/operator_insert.cpp:26):
```cpp
context_->document_storage().insert_or_assign(id, document);
context_->index_engine()->insert_document(document, pipeline_context);
```

**Table** (table/operators/operator_insert.cpp:16-24):
```cpp
table::table_append_state state(context_->resource());
context_->table_storage().table().append_lock(state);
context_->table_storage().table().initialize_append(state);
context_->table_storage().table().append(chunk, state);
context_->index_engine()->insert_row(chunk, row_id, pipeline_context);
```

### Delete

**Collection** (collection/operators/operator_delete.cpp:19-21):
```cpp
auto it = context_->document_storage().find(id);
context_->document_storage().erase(it);
context_->index_engine()->delete_document(document, pipeline_context);
```

**Table** (table/operators/operator_delete.cpp:49-50):
```cpp
auto state = context_->table_storage().table().initialize_delete({});
context_->table_storage().table().delete_rows(*state, ids, count);
context_->index_engine()->delete_row(chunk, row_id, pipeline_context);
```

## Структуры данных

### operator_data_t (base/operators/operator_data.hpp)

```cpp
using data_t = std::variant<
    std::pmr::vector<document::document_ptr>,  // для collection/operators
    vector::data_chunk_t                       // для table/operators
>;
```

Операторы работают с `operator_data_t`, который может содержать либо документы, либо chunk данных.

## Каталог и выбор формата

### Catalog (components/catalog/catalog.cpp:50-67)

```cpp
used_format_t catalog::get_table_format(const table_id& id) const {
    // Проверяем в tables
    if (namespaces.tables.contains(id))
        return used_format_t::columns;

    // Проверяем в computing
    if (namespaces.computing.contains(id))
        return used_format_t::documents;

    return used_format_t::undefined;
}
```

### Используемые форматы

```cpp
enum class used_format_t {
    documents = 0,  // коллекции без схемы → collection/operators
    columns = 1,    // таблицы со схемой → table/operators
    undefined = 2
};
```

## Важные замечания

### 1. CREATE TABLE vs CREATE COLLECTION

- **На уровне SQL:** разные команды
- **На уровне логического плана:** обе становятся `node_create_collection`
- **Разница:** наличие схемы (columns)
- **На уровне каталога:** регистрируются в разных namespaces
- **На уровне memory_storage:** создается одна структура `context_collection_t`

### 2. Флаг uses_datatable_

**НЕ используется** для выбора операторов! Это внутренний флаг контекста.

Выбор операторов идет через:
1. Каталог определяет формат (documents vs columns)
2. Executor выбирает планировщик
3. Планировщик создает нужные операторы

### 3. Незавершенная функциональность

Есть возможность создать `CREATE COLLECTION` **со схемой**, но это приводет к неоднозначности:
- Создается `context_collection_t` с `uses_datatable_=true`
- Но регистрируется в `catalog.computing`
- Поэтому используются `collection/operators`
- Которые обращаются к `document_storage()`, а не к `table_storage()`

**TODO комментарии в коде:**
- collection.hpp:104: `// TODO: only one should exist at all times`
- executor.cpp:93: `// TODO: this does not handle cross documents/columns operations`
- executor.cpp:322: `// TODO: fill chunk with modified rows`
- executor.cpp:387: `// TODO: disk support for data_table`

## Файловая структура

```
components/physical_plan/
├── base/operators/              # Базовые классы операторов
│   ├── operator.hpp/cpp         # operator_t, read_only_operator_t, read_write_operator_t
│   ├── operator_data.hpp/cpp    # operator_data_t (variant документов/chunks)
│   └── operator_write_data.hpp
├── collection/operators/        # Операторы для B-tree (документы)
│   ├── scan/
│   │   ├── full_scan.*         # Сканирование B-tree
│   │   ├── index_scan.*
│   │   └── primary_key_scan.*
│   ├── operator_insert.*       # Вставка в document_storage
│   ├── operator_delete.*       # Удаление из document_storage
│   ├── operator_update.*
│   └── predicates/             # Предикаты для документов
└── table/operators/            # Операторы для data_table (колонки)
    ├── scan/
    │   ├── full_scan.*         # Сканирование data_table
    │   ├── index_scan.*
    │   └── primary_key_scan.*
    ├── operator_insert.*       # Вставка в table_storage
    ├── operator_delete.*       # Удаление из table_storage
    ├── operator_update.*
    └── predicates/             # Предикаты для chunks

services/collection/
├── collection.hpp              # context_collection_t, document_storage_t, table_storage_t
└── executor.cpp                # Выбор операторов по формату данных

components/catalog/
├── catalog.hpp/cpp             # Каталог таблиц и коллекций
└── table_metadata.hpp          # used_format_t

components/physical_plan_generator/
├── create_plan.hpp/cpp         # collection::planner и table::planner
└── impl/
    ├── create_plan_match.*     # Два namespace: collection::planner::impl и table::planner::impl
    ├── create_plan_insert.*
    └── create_plan_delete.*
```

## Примеры использования

### Коллекция без схемы (документы)

```sql
CREATE COLLECTION products();
INSERT INTO products VALUES ('{"name": "Laptop", "price": 1000}');
SELECT * FROM products WHERE price > 500;
```

Использует:
- `document_storage_` (B-tree)
- `collection/operators`
- Динамическая схема

### Таблица со схемой (колонки)

```sql
CREATE TABLE users(id INT, name STRING, age INT);
INSERT INTO users VALUES (1, 'Alice', 30);
SELECT * FROM users WHERE age > 25;
```

Использует:
- `table_storage_` (data_table)
- `table/operators`
- Фиксированная схема, колоночное хранение

## Производительность

### Collection (B-tree)
- ✅ Гибкость схемы (schema-less)
- ✅ Поддержка вложенных структур JSON
- ✅ Быстрый поиск по ключу
- ❌ Row-oriented (медленнее для аналитики)
- ❌ Больше аллокаций памяти

### Table (data_table)
- ✅ Column-oriented (быстрее для аналитики)
- ✅ Batch операции с векторизацией
- ✅ Меньше аллокаций памяти
- ✅ Лучшее сжатие данных
- ❌ Фиксированная схема
- ❌ Сложнее работа с вложенными структурами
