# Integration with Catalog and Executor - Интеграция

## Обзор изменений

Для интеграции document_table в систему нужно изменить:
1. `catalog` - добавить новый формат `document_table`
2. `executor` - добавить выбор document_table планировщика
3. `context_collection_t` - добавить `document_table_storage_`
4. `dispatcher` - обработка создания document_table коллекций
5. `planner` - создание физических планов для document_table

## 1. Изменения в Catalog

### catalog/table_metadata.hpp

```cpp
enum class used_format_t {
    documents = 0,      // B-tree (существующее)
    columns = 1,        // data_table (существующее)
    document_table = 2  // NEW: гибридное хранилище
};
```

### catalog/catalog.cpp

Без изменений - `document_table` будет регистрироваться так же как `documents`:

```cpp
// Для CREATE DOCUMENT_TABLE или CREATE COLLECTION STORAGE=DOCUMENT_TABLE
catalog_.create_computing_table(id);
```

Но добавляем метаданные о типе хранения:

```cpp
struct collection_metadata_t {
    used_format_t storage_format;  // NEW: явное указание формата
    bool uses_document_table;      // NEW: флаг для document_table
};
```

## 2. Изменения в context_collection_t

### services/collection/collection.hpp

```cpp
// Новый тип хранилища
class document_table_storage_t {
public:
    explicit document_table_storage_t(
        std::pmr::memory_resource* resource,
        storage::block_manager_t& block_manager
    );

    document_table::document_table_storage_t& storage();

private:
    // Компоненты storage (аналогично table_storage_t)
    core::filesystem::local_file_system_t fs_;
    table::storage::buffer_pool_t buffer_pool_;
    table::storage::standard_buffer_manager_t buffer_manager_;
    table::storage::in_memory_block_manager_t block_manager_;

    // Само хранилище
    std::unique_ptr<document_table::document_table_storage_t> storage_;
};

enum class storage_type_t {
    DOCUMENT_BTREE = 0,    // document_storage_
    TABLE_COLUMNS = 1,     // table_storage_
    DOCUMENT_TABLE = 2     // NEW: document_table_storage_
};

class context_collection_t final {
public:
    // Существующие конструкторы
    explicit context_collection_t(...);  // B-tree
    explicit context_collection_t(..., columns);  // table

    // NEW: Конструктор для document_table
    explicit context_collection_t(
        std::pmr::memory_resource* resource,
        const collection_full_name_t& name,
        storage_type_t storage_type,  // явно указываем DOCUMENT_TABLE
        const actor_zeta::address_t& mdisk,
        const log_t& log
    );

    // Доступ к хранилищам
    document_storage_t& document_storage() noexcept;
    table_storage_t& table_storage() noexcept;
    document_table_storage_t& document_table_storage() noexcept;  // NEW

    // Получение типа хранилища
    storage_type_t storage_type() const noexcept { return storage_type_; }

private:
    document_storage_t document_storage_;
    table_storage_t table_storage_;
    document_table_storage_t document_table_storage_;  // NEW

    storage_type_t storage_type_;  // NEW: какое хранилище используется
    bool uses_datatable_;  // DEPRECATED: заменяется на storage_type_
};
```

### services/collection/collection.cpp - новый конструктор

```cpp
context_collection_t::context_collection_t(
    std::pmr::memory_resource* resource,
    const collection_full_name_t& name,
    storage_type_t storage_type,
    const actor_zeta::address_t& mdisk,
    const log_t& log
)
    : resource_(resource)
    , document_storage_(resource_)
    , table_storage_(resource_)
    , document_table_storage_(resource_)  // создаем document_table
    , storage_type_(storage_type)
    , name_(name)
    , mdisk_(mdisk)
    , log_(log)
    , uses_datatable_(storage_type == storage_type_t::DOCUMENT_TABLE)
{
    assert(resource != nullptr);
    assert(storage_type == storage_type_t::DOCUMENT_TABLE);
}
```

## 3. Изменения в Executor

### services/collection/executor.cpp

```cpp
void executor_t::execute_plan(
    const session_id_t& session,
    logical_plan::node_ptr logical_plan
) {
    // Получаем формат данных
    auto data_format = check_collections_format_(plan);

    // Создаем физический план
    base::operators::operator_ptr plan;

    if (data_format == catalog::used_format_t::documents) {
        plan = collection::planner::create_plan(...);

    } else if (data_format == catalog::used_format_t::columns) {
        plan = table::planner::create_plan(...);

    } else if (data_format == catalog::used_format_t::document_table) {  // NEW
        plan = document_table::planner::create_plan(
            context_storage,
            logical_plan,
            resource()
        );
    }

    // Выполняем план
    plan->on_execute(nullptr);

    // Обработка результата
    process_result(session, collection, plan, data_format);
}
```

### Определение формата данных

```cpp
catalog::used_format_t executor_t::check_collections_format_(
    const logical_plan::node_ptr& plan
) {
    // Для каждой таблицы в плане проверяем формат
    for (const auto& table_id : plan->get_tables()) {
        auto* collection = context_storage_->get_collection(table_id);

        if (collection->storage_type() == storage_type_t::DOCUMENT_TABLE) {
            return catalog::used_format_t::document_table;
        } else if (collection->storage_type() == storage_type_t::TABLE_COLUMNS) {
            return catalog::used_format_t::columns;
        } else {
            return catalog::used_format_t::documents;
        }
    }

    return catalog::used_format_t::documents;
}
```

## 4. Изменения в Dispatcher

### services/dispatcher/dispatcher.cpp

```cpp
void dispatcher_t::execute_plan(...) {
    switch (node->type()) {
        case node_type::create_collection_t: {
            auto node_info = reinterpret_cast<node_create_collection_ptr&>(node);

            // Проверяем, указан ли тип хранилища
            auto storage_type = node_info->storage_type();

            if (storage_type == storage_type_t::DOCUMENT_TABLE) {
                // NEW: создаем document_table коллекцию
                catalog_.create_computing_table(id);  // регистрируем как computing
                // Но сохраняем метаданные о том, что это document_table
                catalog_.set_storage_type(id, used_format_t::document_table);

            } else if (node_info->schema().empty()) {
                // Обычная коллекция без схемы
                catalog_.create_computing_table(id);

            } else {
                // Таблица со схемой
                auto sch = schema(...);
                catalog_.create_table(id, table_metadata(resource(), std::move(sch)));
            }
            break;
        }
        // ...
    }
}
```

## 5. SQL Syntax для CREATE DOCUMENT_TABLE

### components/sql/transformer/impl/transform_table.cpp

```cpp
logical_plan::node_ptr transformer::transform_create_table(CreateStmt& node) {
    auto coldefs = reinterpret_cast<List*>(node.tableElts);

    // Проверяем опции для определения типа хранилища
    storage_type_t storage_type = storage_type_t::DOCUMENT_BTREE;  // по умолчанию

    // Проверяем STORAGE=... опцию
    for (auto opt : node.options->lst) {
        auto def_elem = pg_ptr_assert_cast<DefElem>(opt.data, T_DefElem);

        if (std::string(def_elem->defname) == "storage") {
            auto storage_str = strVal(def_elem->arg);

            if (std::string(storage_str) == "document_table") {
                storage_type = storage_type_t::DOCUMENT_TABLE;
            } else if (std::string(storage_str) == "columns") {
                storage_type = storage_type_t::TABLE_COLUMNS;
            }
        }
    }

    // Извлекаем колонки (если есть)
    std::pmr::vector<complex_logical_type> columns(resource_);
    // ... (существующий код)

    // Создаем узел с указанием типа хранилища
    if (storage_type == storage_type_t::DOCUMENT_TABLE) {
        return logical_plan::make_node_create_document_table(
            resource_,
            rangevar_to_collection(node.relation)
        );
    }

    // Существующая логика
    if (columns.empty()) {
        return logical_plan::make_node_create_collection(...);
    }

    return logical_plan::make_node_create_collection(..., columns);
}
```

### SQL примеры

```sql
-- Вариант 1: Явное указание STORAGE
CREATE COLLECTION users STORAGE=DOCUMENT_TABLE;

-- Вариант 2: Специальная команда (если добавим в parser)
CREATE DOCUMENT_TABLE users;

-- Вариант 3: Через опцию таблицы
CREATE TABLE users() WITH (storage='document_table');
```

## 6. Планировщик для document_table

### components/physical_plan_generator/document_table_planner.hpp

```cpp
namespace components::document_table::planner {

base::operators::operator_ptr create_plan(
    services::memory_storage::context_storage_t* context_storage,
    const logical_plan::node_ptr& logical_plan,
    std::pmr::memory_resource* resource
);

namespace impl {

base::operators::operator_ptr create_plan_match(
    services::collection::context_collection_t* collection,
    const logical_plan::node_ptr& node,
    std::pmr::memory_resource* resource
);

base::operators::operator_ptr create_plan_insert(
    services::collection::context_collection_t* collection,
    const logical_plan::node_ptr& node,
    std::pmr::memory_resource* resource
);

base::operators::operator_ptr create_plan_delete(
    services::collection::context_collection_t* collection,
    const logical_plan::node_ptr& node,
    std::pmr::memory_resource* resource
);

base::operators::operator_ptr create_plan_update(
    services::collection::context_collection_t* collection,
    const logical_plan::node_ptr& node,
    std::pmr::memory_resource* resource
);

} // namespace impl
} // namespace components::document_table::planner
```

### Реализация create_plan

```cpp
// components/physical_plan_generator/document_table_planner.cpp

base::operators::operator_ptr planner::create_plan(
    services::memory_storage::context_storage_t* context_storage,
    const logical_plan::node_ptr& logical_plan,
    std::pmr::memory_resource* resource
) {
    auto* collection = context_storage->get_collection(
        logical_plan->collection_full_name()
    );

    assert(collection->storage_type() == storage_type_t::DOCUMENT_TABLE);

    // Делегируем создание плана в зависимости от типа узла
    switch (logical_plan->type()) {
        case node_type::match_t:
            return impl::create_plan_match(collection, logical_plan, resource);

        case node_type::insert_t:
            return impl::create_plan_insert(collection, logical_plan, resource);

        case node_type::delete_t:
            return impl::create_plan_delete(collection, logical_plan, resource);

        case node_type::update_t:
            return impl::create_plan_update(collection, logical_plan, resource);

        case node_type::group_t:
            return impl::create_plan_group(collection, logical_plan, resource);

        case node_type::join_t:
            return impl::create_plan_join(collection, logical_plan, resource);

        default:
            throw std::runtime_error("Unsupported node type for document_table");
    }
}
```

## 7. Изменения в memory_storage

### services/memory_storage/memory_storage.cpp

```cpp
void memory_storage_t::create_collection_(
    const session_id_t& session,
    logical_plan::node_ptr logical_plan
) {
    auto create_plan = reinterpret_cast<const node_create_collection_ptr&>(logical_plan);

    // Определяем тип хранилища
    auto storage_type = create_plan->storage_type();

    if (storage_type == storage_type_t::DOCUMENT_TABLE) {
        // NEW: Создаем document_table коллекцию
        collections_.emplace(
            logical_plan->collection_full_name(),
            new collection::context_collection_t(
                resource(),
                logical_plan->collection_full_name(),
                storage_type_t::DOCUMENT_TABLE,
                manager_disk_,
                log_.clone()
            )
        );

    } else if (create_plan->schema().empty()) {
        // Обычная коллекция (B-tree)
        collections_.emplace(...);  // существующий код

    } else {
        // Таблица (columns)
        collections_.emplace(...);  // существующий код
    }

    // Результат
    actor_zeta::send(current_message()->sender(), ...);
}
```

## 8. Изменения в logical_plan

### components/logical_plan/node_create_collection.hpp

```cpp
class node_create_collection_t {
public:
    // Существующие конструкторы
    node_create_collection_t(...);
    node_create_collection_t(..., schema);

    // NEW: С указанием типа хранилища
    node_create_collection_t(
        std::pmr::memory_resource* resource,
        const collection_full_name_t& name,
        storage_type_t storage_type
    );

    storage_type_t storage_type() const { return storage_type_; }

private:
    storage_type_t storage_type_ = storage_type_t::DOCUMENT_BTREE;  // NEW
};
```

## Порядок реализации

1. ✅ Дизайн архитектуры
2. ✅ JSON path extractor
3. ✅ Dynamic schema
4. ✅ Document table storage
5. ✅ Операторы
6. **Интеграция (текущий этап):**
   - Изменить `storage_type_t` enum
   - Добавить поле в `context_collection_t`
   - Добавить конструктор для document_table
   - Изменить `executor` для выбора планировщика
   - Создать `document_table::planner`
   - Изменить `dispatcher` для обработки CREATE
   - Добавить SQL syntax
7. Тесты

## Файлы для изменения

```
services/collection/
├── collection.hpp                    # MODIFY: добавить document_table_storage_
└── collection.cpp                    # MODIFY: новый конструктор

services/collection/
├── executor.hpp                      # MODIFY: добавить document_table формат
└── executor.cpp                      # MODIFY: выбор планировщика

services/dispatcher/
└── dispatcher.cpp                    # MODIFY: обработка CREATE DOCUMENT_TABLE

services/memory_storage/
└── memory_storage.cpp                # MODIFY: создание document_table

components/catalog/
└── table_metadata.hpp                # MODIFY: добавить document_table формат

components/logical_plan/
├── forward.hpp                       # MODIFY: storage_type_t
└── node_create_collection.hpp        # MODIFY: добавить storage_type

components/physical_plan_generator/
├── document_table_planner.hpp        # NEW
├── document_table_planner.cpp        # NEW
└── impl/
    ├── create_plan_match.cpp         # NEW
    ├── create_plan_insert.cpp        # NEW
    ├── create_plan_delete.cpp        # NEW
    └── create_plan_update.cpp        # NEW

components/sql/transformer/
└── impl/transform_table.cpp          # MODIFY: STORAGE опция
```

## Тесты интеграции

1. `test_create_document_table()` - создание через SQL
2. `test_insert_query_document_table()` - вставка и запросы
3. `test_schema_evolution_integration()` - эволюция схемы
4. `test_mixed_storage_types()` - разные типы хранилищ в одной БД
5. `test_join_document_table_with_table()` - join между разными типами
