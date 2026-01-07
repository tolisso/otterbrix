# Document Table Integration Plan

## Часть 1: Роль Catalog в системе

### Зачем нужен Catalog?

Catalog - это **центральное хранилище метаданных** о всех объектах базы данных:

| Что хранит | Зачем нужно |
|------------|-------------|
| Список databases (namespaces) | Проверка существования при запросах |
| Список таблиц/коллекций | Валидация SQL запросов |
| Схема каждой таблицы | Проверка типов, оптимизация запросов |
| Тип хранилища (storage_format) | Выбор правильного planner/executor |
| Пользовательские типы | CREATE TYPE, использование в схемах |
| Версии полей (для documents) | Schema evolution, refcount для DELETE |

**Без catalog:**
- Нельзя узнать какой storage использует коллекция
- Нельзя проверить типы при INSERT
- Нельзя вернуть схему через GET SCHEMA
- Нельзя отследить эволюцию схемы

### Структура Catalog

```
catalog_t
├── namespaces_: namespace_storage
│   └── map<database_name, namespace_info>
│       ├── tables: map<table_name, table_metadata>      // Строгая схема (columns)
│       └── computing: map<table_name, computed_schema>  // Динамическая схема (documents, document_table)
│
└── registered_types_: map<type_name, complex_logical_type>  // CREATE TYPE
```

### Два типа схем в Catalog

**1. table_metadata (для columns storage)**
```cpp
struct table_metadata {
    schema schema_struct_;           // Фиксированная схема с типами
    std::pmr::string description_;   // Описание таблицы
    timestamp last_updated_ms_;      // Время обновления
};
```

**2. computed_schema (для documents и document_table)**
```cpp
struct computed_schema {
    versioned_trie<string, complex_logical_type> fields_;  // Все версии всех полей
    map<string, refcounted_entry_t> existing_versions_;    // Текущие версии + refcount
    used_format_t storage_format_;                         // documents / document_table
};
```

---

## Часть 2: Как Catalog участвует в командах (documents storage)

### CREATE DATABASE

```
SQL: CREATE DATABASE mydb;
         │
         ▼
┌─────────────────────────────────────────┐
│  dispatcher_t::update_catalog()         │
│                                         │
│  catalog_.create_namespace("mydb");     │
│  // Создаёт пустой namespace_info       │
└─────────────────────────────────────────┘
```

### CREATE TABLE (без схемы → documents)

```
SQL: CREATE TABLE mydb.users();
         │
         ▼
┌─────────────────────────────────────────┐
│  dispatcher_t::update_catalog()         │
│                                         │
│  // Схема пустая → computing table      │
│  catalog_.create_computing_table(       │
│      id,                                │
│      used_format_t::documents           │
│  );                                     │
│  // Создаёт computed_schema с пустыми   │
│  // fields_ и storage_format=documents  │
└─────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────┐
│  memory_storage_t::create_collection_() │
│                                         │
│  // Проверяет storage_format            │
│  if (format == documents)               │
│      → context_collection_t(BTREE)      │
└─────────────────────────────────────────┘
```

### CREATE TABLE (со схемой → columns)

```
SQL: CREATE TABLE mydb.users(id INT, name TEXT);
         │
         ▼
┌─────────────────────────────────────────┐
│  dispatcher_t::update_catalog()         │
│                                         │
│  // Схема НЕ пустая → обычная table     │
│  schema sch = create_struct([           │
│      {id, INT}, {name, TEXT}            │
│  ]);                                    │
│  catalog_.create_table(                 │
│      id,                                │
│      table_metadata(sch)                │
│  );                                     │
└─────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────┐
│  memory_storage_t::create_collection_() │
│                                         │
│  // Схема не пустая                     │
│  → context_collection_t(TABLE_COLUMNS)  │
└─────────────────────────────────────────┘
```

### CREATE TABLE (document_table)

```
SQL: CREATE TABLE mydb.users() WITH (storage='document_table');
         │
         ▼
┌─────────────────────────────────────────┐
│  dispatcher_t::update_catalog()         │
│                                         │
│  catalog_.create_computing_table(       │
│      id,                                │
│      used_format_t::document_table  ◄───┼── storage_format из SQL
│  );                                     │
└─────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────┐
│  memory_storage_t::create_collection_() │
│                                         │
│  if (format == document_table)          │
│      → context_collection_t(DOC_TABLE)  │
└─────────────────────────────────────────┘
```

### INSERT (documents storage)

```
SQL: INSERT INTO mydb.users (name, age) VALUES ('Alice', 30);
         │
         ▼
┌─────────────────────────────────────────┐
│  dispatcher_t::execute_plan()           │
│                                         │
│  // Получаем формат из catalog          │
│  format = catalog_.get_table_format(id);│
│  // → documents                         │
│                                         │
│  // Отправляем в memory_storage         │
│  send(memory_storage, plan, format);    │
└─────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────┐
│  executor_t::execute_plan()             │
│                                         │
│  if (format == documents)               │
│      plan = collection::planner(...)    │
│                                         │
│  // Выполняем INSERT                    │
└─────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────┐
│  dispatcher_t::update_catalog()         │
│                                         │
│  // ОБНОВЛЯЕМ СХЕМУ В CATALOG!          │
│  auto& sch = catalog_                   │
│      .get_computing_table_schema(id);   │
│                                         │
│  for (field in document) {              │
│      sch.append(field.name, field.type);│
│  }                                      │
│  // Теперь catalog знает о полях        │
│  // name:STRING, age:BIGINT             │
└─────────────────────────────────────────┘
```

### SELECT (documents storage)

```
SQL: SELECT * FROM mydb.users WHERE age > 25;
         │
         ▼
┌─────────────────────────────────────────┐
│  dispatcher_t::execute_plan()           │
│                                         │
│  // Получаем формат из catalog          │
│  format = catalog_.get_table_format(id);│
│  // → documents                         │
└─────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────┐
│  executor_t::execute_plan()             │
│                                         │
│  if (format == documents)               │
│      plan = collection::planner(...)    │
│                                         │
│  // Выполняем SELECT                    │
│  // Сканируем B-tree                    │
└─────────────────────────────────────────┘
```

### DELETE (documents storage)

```
SQL: DELETE FROM mydb.users WHERE age < 20;
         │
         ▼
┌─────────────────────────────────────────┐
│  executor + dispatcher                  │
│                                         │
│  // После удаления документов           │
└─────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────┐
│  dispatcher_t::update_catalog()         │
│                                         │
│  // УМЕНЬШАЕМ REFCOUNT В CATALOG!       │
│  auto& sch = catalog_                   │
│      .get_computing_table_schema(id);   │
│                                         │
│  for ((field, type, count) in deleted) {│
│      sch.drop_n(field, type, count);    │
│  }                                      │
│  // Если refcount=0, поле удаляется     │
│  // из текущей версии схемы             │
└─────────────────────────────────────────┘
```

### GET SCHEMA

```
SQL: -- через API dispatcher->get_schema()
         │
         ▼
┌─────────────────────────────────────────┐
│  dispatcher_t::get_schema()             │
│                                         │
│  if (catalog_.table_exists(id)) {       │
│      // Строгая схема (columns)         │
│      return catalog_                    │
│          .get_table_schema(id)          │
│          .schema_struct();              │
│  }                                      │
│  if (catalog_.table_computes(id)) {     │
│      // Вычисленная схема (documents)   │
│      return catalog_                    │
│          .get_computing_table_schema(id)│
│          .latest_types_struct();        │
│  }                                      │
└─────────────────────────────────────────┘
```

---

## Часть 3: Проблема document_table с Catalog

### Текущее состояние: ДВЕ НЕСИНХРОНИЗИРОВАННЫЕ СХЕМЫ

```
┌────────────────────────────────────────────────────────────────┐
│                         DISPATCHER                              │
│  catalog_                                                       │
│  └── computed_schema (storage_format=document_table)            │
│      └── fields_: {} ◄─── ПУСТАЯ! Не обновляется при INSERT    │
└────────────────────────────────────────────────────────────────┘
                              │
                    execute_plan(format=document_table)
                              │
                              ▼
┌────────────────────────────────────────────────────────────────┐
│                      MEMORY_STORAGE                             │
│  collection                                                     │
│  └── document_table_storage                                     │
│      └── dynamic_schema_ ◄─── АКТУАЛЬНАЯ! Обновляется          │
│          └── columns: [name:STRING, age:INT, ...]               │
└────────────────────────────────────────────────────────────────┘
```

### Почему это проблема?

| Операция | Documents | Document_table |
|----------|-----------|----------------|
| CREATE TABLE | ✓ catalog обновляется | ✓ catalog обновляется |
| INSERT | ✓ catalog.append() | ✗ catalog НЕ обновляется |
| DELETE | ✓ catalog.drop_n() | ✗ catalog НЕ обновляется |
| GET SCHEMA | ✓ возвращает схему | ✗ возвращает ПУСТУЮ схему |
| SELECT | ✓ format из catalog | ✓ format из catalog |

### Где именно происходит расхождение

**dispatcher.cpp:837-874 (INSERT)**
```cpp
case node_type::insert_t: {
    // ...
    if (comp_sch.has_value()) {
        // Только если uses_documents() == true!
        // Для document_table это FALSE
        if (node_info->uses_documents()) {  // ◄── document_table использует data_chunk
            for (doc in documents) {
                comp_sch.value().get().append(key, type);
            }
        }
        // document_table не попадает сюда!
    }
}
```

**document_table_storage.cpp:batch_insert()**
```cpp
void batch_insert(documents) {
    for (doc in documents) {
        auto new_cols = schema_->evolve(doc);  // Локальная схема обновляется
        // catalog НЕ обновляется!
    }
}
```

---

## Часть 4: Как должен работать document_table с Catalog

### Вариант A: Синхронизация после операций

```
┌────────────────────────────────────────────────────────────────┐
│                      INSERT (document_table)                    │
└────────────────────────────────────────────────────────────────┘
         │
         ▼
┌────────────────────────────────────────────────────────────────┐
│  executor_t::insert_document_impl()                            │
│                                                                │
│  // 1. Выполняем INSERT через operator                         │
│  plan->execute();                                              │
│                                                                │
│  // 2. Получаем новые колонки из storage                       │
│  auto& storage = collection->document_table_storage().storage();│
│  auto new_columns = storage.get_new_columns_since_last_sync(); │
│                                                                │
│  // 3. Возвращаем их в dispatcher для синхронизации            │
│  result->set_schema_updates(new_columns);                      │
└────────────────────────────────────────────────────────────────┘
         │
         ▼
┌────────────────────────────────────────────────────────────────┐
│  dispatcher_t::update_catalog()                                │
│                                                                │
│  if (result->has_schema_updates()) {                           │
│      auto& sch = catalog_.get_computing_table_schema(id);      │
│      for (col in result->schema_updates()) {                   │
│          sch.append(col.name, col.type);                       │
│      }                                                         │
│  }                                                             │
└────────────────────────────────────────────────────────────────┘
```

### Вариант B: Catalog как единственный источник

```
┌────────────────────────────────────────────────────────────────┐
│  document_table_storage (БЕЗ локальной схемы)                  │
│                                                                │
│  // Вместо: std::unique_ptr<dynamic_schema_t> schema_;         │
│  // Используем: ссылку на computed_schema из catalog           │
│                                                                │
│  void batch_insert(docs, catalog_ref) {                        │
│      auto& sch = catalog_ref.get_computing_table_schema(id);   │
│      for (doc in docs) {                                       │
│          auto new_cols = detect_new_columns(doc, sch);         │
│          for (col in new_cols) {                               │
│              sch.append(col.name, col.type);  // Сразу в catalog│
│              table_.add_column(col);                           │
│          }                                                     │
│      }                                                         │
│  }                                                             │
└────────────────────────────────────────────────────────────────┘
```

### Вариант C: Минимальные изменения (рекомендуется)

Добавить обновление catalog в `dispatcher_t::update_catalog()` для document_table:

```cpp
// dispatcher.cpp:update_catalog()
case node_type::insert_t: {
    if (catalog_.table_computes(id)) {
        auto& comp_sch = catalog_.get_computing_table_schema(id);

        if (node_info->uses_documents()) {
            // Существующий код для documents
            for (doc in documents) {
                comp_sch.append(key, type);
            }
        }
        else if (node_info->uses_data_chunk()) {
            // НОВЫЙ КОД для document_table
            auto& chunk = node_info->data_chunk();
            for (col in chunk.columns()) {
                comp_sch.append(col.name(), col.type());
            }
        }
    }
    break;
}
```

---

## Часть 5: Полная схема интеграции

### Documents Storage (текущая, работает)

```
                              SQL Query
                                  │
                                  ▼
                    ┌─────────────────────────┐
                    │      SQL Parser         │
                    └─────────────────────────┘
                                  │
                                  ▼
                    ┌─────────────────────────┐
                    │    Logical Plan         │
                    └─────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────┐
│                        DISPATCHER                                │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │                      catalog_                            │    │
│  │  ┌─────────────────────────────────────────────────┐    │    │
│  │  │  computed_schema                                 │    │    │
│  │  │  ├── fields_: {name:STRING, age:INT}  ◄─────────┼────┼────┼── UPDATE
│  │  │  └── storage_format: documents                  │    │    │
│  │  └─────────────────────────────────────────────────┘    │    │
│  └─────────────────────────────────────────────────────────┘    │
│                                  │                               │
│                    get_table_format() → documents                │
│                                  │                               │
└──────────────────────────────────┼───────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────┐
│                      MEMORY_STORAGE                              │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │  context_collection_t (DOCUMENT_BTREE)                  │    │
│  │  └── document_storage_: btree<doc_id, document>         │    │
│  └─────────────────────────────────────────────────────────┘    │
│                                  │                               │
│                    collection::planner                           │
│                                  │                               │
└──────────────────────────────────┼───────────────────────────────┘
                                   │
                                   ▼
                    ┌─────────────────────────┐
                    │       Result            │
                    └─────────────────────────┘
```

### Document_table Storage (текущая, catalog не синхронизирован)

```
                              SQL Query
                                  │
                                  ▼
                    ┌─────────────────────────┐
                    │      SQL Parser         │
                    └─────────────────────────┘
                                  │
                                  ▼
                    ┌─────────────────────────┐
                    │    Logical Plan         │
                    └─────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────┐
│                        DISPATCHER                                │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │                      catalog_                            │    │
│  │  ┌─────────────────────────────────────────────────┐    │    │
│  │  │  computed_schema                                 │    │    │
│  │  │  ├── fields_: {} ◄────────────────── ПУСТАЯ!    │    │    │
│  │  │  └── storage_format: document_table              │    │    │
│  │  └─────────────────────────────────────────────────┘    │    │
│  └─────────────────────────────────────────────────────────┘    │
│                                  │                               │
│                    get_table_format() → document_table           │
│                                  │                               │
└──────────────────────────────────┼───────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────┐
│                      MEMORY_STORAGE                              │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │  context_collection_t (DOCUMENT_TABLE)                  │    │
│  │  └── document_table_storage_                            │    │
│  │      └── dynamic_schema_: {name:STRING, age:INT} ◄──────┼────┼── АКТУАЛЬНАЯ
│  └─────────────────────────────────────────────────────────┘    │
│                                  │                               │
│                    document_table::planner                       │
│                                  │                               │
└──────────────────────────────────┼───────────────────────────────┘
                                   │
                                   ▼
                    ┌─────────────────────────┐
                    │       Result            │
                    └─────────────────────────┘
```

### Document_table Storage (целевая, с синхронизацией)

```
                              SQL Query
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────┐
│                        DISPATCHER                                │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │                      catalog_                            │    │
│  │  ┌─────────────────────────────────────────────────┐    │    │
│  │  │  computed_schema                                 │    │    │
│  │  │  ├── fields_: {name:STRING, age:INT} ◄──────────┼────┼────┼── СИНХРОНИЗИРОВАНА
│  │  │  └── storage_format: document_table              │    │    │
│  │  └─────────────────────────────────────────────────┘    │    │
│  └─────────────────────────────────────────────────────────┘    │
│                                  │                               │
└──────────────────────────────────┼───────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────┐
│                      MEMORY_STORAGE                              │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │  document_table_storage_                                │    │
│  │  └── dynamic_schema_: {name:STRING, age:INT}            │    │
│  └─────────────────────────────────────────────────────────┘    │
│                                  │                               │
│                         После INSERT:                            │
│                    sync_to_catalog(new_columns)                  │
│                                  │                               │
└──────────────────────────────────┼───────────────────────────────┘
                                   │
                                   ▼
                         update_catalog()
                    catalog_.append(new_columns)
```

---

## Часть 6: План действий

### Этап 1: Минимальная синхронизация (рекомендуется сейчас)

1. В `dispatcher.cpp:update_catalog()` добавить ветку для `uses_data_chunk()`
2. Получать типы колонок из `data_chunk` и обновлять `computed_schema`

### Этап 2: GET SCHEMA для document_table

1. В `dispatcher.cpp:get_schema()` для document_table возвращать схему из `computed_schema`
2. Или получать схему напрямую из `document_table_storage.schema()`

### Этап 3: DELETE синхронизация

1. Отслеживать удалённые документы
2. Уменьшать refcount в `computed_schema.drop_n()`

### Этап 4 (опционально): Единый источник истины

1. Убрать `dynamic_schema_` из `document_table_storage`
2. Использовать `computed_schema` из catalog напрямую
3. Передавать ссылку на catalog в storage операции

---

## Часть 7: Проблема дублирования — dynamic_schema как "свой каталог"

### Что произошло

При создании document_table мы фактически **создали свой мини-каталог** в виде `dynamic_schema`:

```
┌─────────────────────────────────────────────────────────────────┐
│                    ГЛАВНЫЙ CATALOG                               │
│  (в dispatcher)                                                  │
│                                                                  │
│  computed_schema:                                                │
│  ├── fields_: versioned_trie<string, complex_logical_type>      │
│  ├── existing_versions_: map<string, refcounted_entry_t>        │
│  └── storage_format_: document_table                            │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│                    "СВОЙ КАТАЛОГ" в document_table               │
│  (в document_table_storage)                                      │
│                                                                  │
│  dynamic_schema:                                                 │
│  ├── columns_: vector<column_info_t>                            │
│  ├── path_to_column_: map<string, size_t>                       │
│  └── type для каждой колонки                                    │
└─────────────────────────────────────────────────────────────────┘
```

### Почему это плохо

| Проблема | Описание |
|----------|----------|
| **Дублирование данных** | Схема хранится в двух местах |
| **Рассинхронизация** | catalog пустой, dynamic_schema актуальная |
| **Нарушение архитектуры** | catalog должен быть единственным источником истины |
| **GET SCHEMA не работает** | catalog не знает о колонках |
| **Сложность поддержки** | Изменения нужно делать в двух местах |

### Сравнение: что хранят

| Поле | computed_schema (catalog) | dynamic_schema (document_table) |
|------|---------------------------|--------------------------------|
| Имя колонки | ✓ fields_ key | ✓ column_info_t.json_path |
| Тип колонки | ✓ complex_logical_type | ✓ column_info_t.type |
| Версионирование | ✓ versioned_trie | ✗ нет |
| Refcount | ✓ refcounted_entry_t | ✗ нет |
| Индекс колонки | ✗ нет | ✓ column_info_t.column_index |
| Union types | ✓ в complex_logical_type | ✓ column_info_t.union_types |

### Вывод

**dynamic_schema дублирует computed_schema**, добавляя только `column_index` для маппинга на физические колонки в data_table.

---

## Часть 8: План миграции с dynamic_schema на catalog

### Допущение

> По одному JSON path хранится один тип данных (без union types на первом этапе)

### Что нужно изменить

#### Шаг 1: Передать catalog в document_table_storage

**Проблема:** document_table_storage не имеет доступа к catalog.

**Решение:** Передавать ссылку на computed_schema при операциях.

```cpp
// Было:
class document_table_storage_t {
    std::unique_ptr<dynamic_schema_t> schema_;  // свой каталог
};

// Станет:
class document_table_storage_t {
    // Только маппинг path -> column_index (то, чего нет в catalog)
    std::pmr::unordered_map<std::string, size_t> path_to_column_index_;
};
```

#### Шаг 2: Изменить batch_insert

```cpp
// Было:
void batch_insert(documents) {
    for (doc : documents) {
        auto new_cols = schema_->evolve(doc);  // локальная схема
        // ...
    }
}

// Станет:
void batch_insert(documents, computed_schema& catalog_schema) {
    for (doc : documents) {
        for (field : doc) {
            // Проверяем в catalog
            if (!catalog_schema.has_field(field.path)) {
                // Добавляем в catalog
                catalog_schema.append(field.path, field.type);
                // Добавляем колонку в data_table
                auto col_idx = table_.add_column(field.type);
                path_to_column_index_[field.path] = col_idx;
            }
        }
    }
}
```

#### Шаг 3: Изменить full_scan

```cpp
// Было:
void full_scan::on_execute() {
    auto& schema = storage_->schema();  // dynamic_schema
    // ...
}

// Станет:
void full_scan::on_execute(const computed_schema& catalog_schema) {
    // Получаем типы из catalog
    for (field : catalog_schema.fields()) {
        auto col_idx = storage_->get_column_index(field.path);
        // ...
    }
}
```

#### Шаг 4: Передача catalog через цепочку вызовов

```
dispatcher (имеет catalog_)
    │
    ▼
memory_storage (получает ссылку)
    │
    ▼
executor (получает ссылку)
    │
    ▼
operator_insert/full_scan (получает ссылку)
    │
    ▼
document_table_storage (использует для schema operations)
```

### Что остаётся в document_table_storage

После миграции в document_table_storage остаётся только:

```cpp
class document_table_storage_t {
    // Физическое хранение данных
    std::unique_ptr<table::data_table_t> table_;

    // Маппинг document_id -> row_id (для O(1) доступа)
    std::pmr::unordered_map<document_id_t, row_id_t> id_to_row_;

    // Маппинг json_path -> column_index (чего нет в catalog)
    std::pmr::unordered_map<std::string, size_t> path_to_column_index_;

    // НЕТ dynamic_schema_ - используем catalog
};
```

### Порядок миграции

```
┌─────────────────────────────────────────────────────────────────┐
│  Этап 1: Синхронизация (не ломает текущий код)                  │
│                                                                  │
│  - Добавить обновление catalog в dispatcher::update_catalog()   │
│  - dynamic_schema остаётся, но catalog тоже обновляется         │
│  - GET SCHEMA начинает работать                                  │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  Этап 2: Передача catalog в storage                             │
│                                                                  │
│  - Добавить параметр computed_schema& в методы storage          │
│  - Читать типы из catalog вместо dynamic_schema                 │
│  - dynamic_schema всё ещё существует (fallback)                 │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  Этап 3: Удаление dynamic_schema                                │
│                                                                  │
│  - Убрать dynamic_schema_ из document_table_storage             │
│  - Оставить только path_to_column_index_                        │
│  - catalog = единственный источник истины                       │
└─────────────────────────────────────────────────────────────────┘
```

### Диаграмма целевой архитектуры

```
                         SQL Query
                             │
                             ▼
┌────────────────────────────────────────────────────────────────┐
│                        DISPATCHER                               │
│                                                                 │
│  catalog_                                                       │
│  └── computed_schema                                            │
│      ├── fields_: {name:STRING, age:INT, ...}  ◄── ЕДИНСТВЕННЫЙ│
│      └── storage_format_: document_table            ИСТОЧНИК   │
│                             │                                   │
└─────────────────────────────┼───────────────────────────────────┘
                              │ ссылка на computed_schema
                              ▼
┌────────────────────────────────────────────────────────────────┐
│                      MEMORY_STORAGE                             │
│                              │                                  │
└──────────────────────────────┼──────────────────────────────────┘
                               │
                               ▼
┌────────────────────────────────────────────────────────────────┐
│                        EXECUTOR                                 │
│                              │                                  │
└──────────────────────────────┼──────────────────────────────────┘
                               │
                               ▼
┌────────────────────────────────────────────────────────────────┐
│                  document_table_storage                         │
│                                                                 │
│  table_: data_table_t          ◄── физические данные           │
│  id_to_row_: map<doc_id, row>  ◄── O(1) доступ по _id          │
│  path_to_column_: map<path, idx> ◄── маппинг path→колонка      │
│                                                                 │
│  // НЕТ dynamic_schema_!                                        │
│  // Типы берём из catalog через переданную ссылку               │
└────────────────────────────────────────────────────────────────┘
```

---

## Часть 9: Файлы для изменения

| Файл | Изменение |
|------|-----------|
| `services/dispatcher/dispatcher.cpp` | Добавить синхронизацию catalog для document_table в `update_catalog()` |
| `services/collection/executor.cpp` | Возвращать информацию о новых колонках в результате |
| `components/physical_plan/document_table/operators/operator_insert.cpp` | Собирать информацию о schema evolution |
| `components/document_table/document_table_storage.hpp` | Добавить метод `get_schema_updates()` |
