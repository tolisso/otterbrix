# Dynamic Schema - Динамическая схема с автоматическим расширением

## Концепция

Динамическая схема автоматически расширяется при встрече новых полей в документах. При вставке документа с новыми полями:
1. Извлекаются новые JSON paths
2. Определяются типы новых полей
3. Добавляются новые колонки в таблицу
4. Существующие строки получают NULL значения для новых колонок

## Архитектура

### Основной класс: dynamic_schema_t

```cpp
// components/document_table/dynamic_schema.hpp

namespace components::document_table {

struct column_info_t {
    std::string json_path;               // JSON path (например, "user.address.city")
    types::complex_logical_type type;    // Тип колонки
    size_t column_index;                 // Индекс колонки в таблице
    bool is_array_element;               // Элемент массива?
    size_t array_index;                  // Индекс в массиве
};

class dynamic_schema_t {
public:
    explicit dynamic_schema_t(std::pmr::memory_resource* resource);

    // Проверка, существует ли путь в схеме
    bool has_path(const std::string& json_path) const;

    // Получение информации о колонке по пути
    const column_info_t* get_column_info(const std::string& json_path) const;

    // Получение колонки по индексу
    const column_info_t* get_column_by_index(size_t index) const;

    // Добавление новой колонки
    void add_column(const std::string& json_path,
                   const types::complex_logical_type& type,
                   bool is_array_element = false,
                   size_t array_index = 0);

    // Получение всех колонок
    const std::pmr::vector<column_info_t>& columns() const;

    // Количество колонок
    size_t column_count() const;

    // Конвертация в column_definition_t для data_table
    std::vector<table::column_definition_t> to_column_definitions() const;

    // Эволюция схемы: добавление новых путей из документа
    std::pmr::vector<column_info_t> evolve(const document::document_ptr& doc);

private:
    std::pmr::memory_resource* resource_;

    // Список всех колонок в порядке добавления
    std::pmr::vector<column_info_t> columns_;

    // Маппинг JSON path -> индекс в columns_
    std::pmr::unordered_map<std::string, size_t> path_to_index_;

    // Экстрактор путей
    std::unique_ptr<json_path_extractor_t> extractor_;
};

} // namespace components::document_table
```

## Реализация

### Конструктор

```cpp
dynamic_schema_t::dynamic_schema_t(std::pmr::memory_resource* resource)
    : resource_(resource)
    , columns_(resource)
    , path_to_index_(resource)
    , extractor_(std::make_unique<json_path_extractor_t>(resource)) {

    // Всегда добавляем служебную колонку для document_id
    add_column("_id", types::complex_logical_type(types::logical_type::VARCHAR));
}
```

### add_column - добавление колонки

```cpp
void dynamic_schema_t::add_column(
    const std::string& json_path,
    const types::complex_logical_type& type,
    bool is_array_element,
    size_t array_index
) {
    // Проверяем, что колонка еще не существует
    if (has_path(json_path)) {
        return;  // Уже есть
    }

    // Добавляем новую колонку
    size_t new_index = columns_.size();
    columns_.push_back(column_info_t{
        .json_path = json_path,
        .type = type,
        .column_index = new_index,
        .is_array_element = is_array_element,
        .array_index = array_index
    });

    // Обновляем маппинг
    path_to_index_[json_path] = new_index;
}
```

### evolve - эволюция схемы

```cpp
std::pmr::vector<column_info_t> dynamic_schema_t::evolve(
    const document::document_ptr& doc
) {
    std::pmr::vector<column_info_t> new_columns(resource_);

    // Извлекаем все пути из документа
    auto extracted_paths = extractor_->extract_paths(doc);

    // Проходим по всем путям
    for (const auto& path_info : extracted_paths) {
        // Если путь уже существует, пропускаем
        if (has_path(path_info.path)) {
            // TODO: проверить совместимость типов
            continue;
        }

        // Создаем новую колонку
        types::complex_logical_type col_type(path_info.type);
        col_type.set_alias(path_info.path);

        add_column(
            path_info.path,
            col_type,
            path_info.is_array,
            path_info.array_index
        );

        // Запоминаем, что это новая колонка
        new_columns.push_back(columns_.back());
    }

    return new_columns;
}
```

### to_column_definitions - конвертация для data_table

```cpp
std::vector<table::column_definition_t> dynamic_schema_t::to_column_definitions() const {
    std::vector<table::column_definition_t> result;
    result.reserve(columns_.size());

    for (const auto& col : columns_) {
        result.emplace_back(col.json_path, col.type);
    }

    return result;
}
```

## document_table_storage_t с динамической схемой

```cpp
// components/document_table/document_table_storage.hpp

class document_table_storage_t {
public:
    explicit document_table_storage_t(
        std::pmr::memory_resource* resource,
        storage::block_manager_t& block_manager
    );

    // Вставка документа с автоматической эволюцией схемы
    void insert(document_id_t id, const document::document_ptr& doc);

    // Проверка, нужна ли эволюция схемы
    bool needs_evolution(const document::document_ptr& doc) const;

    // Эволюция схемы и пересоздание таблицы
    void evolve_schema(const std::pmr::vector<column_info_t>& new_columns);

    // Конвертация документа в row согласно текущей схеме
    vector::data_chunk_t document_to_row(const document::document_ptr& doc);

    // Конвертация row обратно в документ
    document::document_ptr row_to_document(const vector::data_chunk_t& row, size_t row_idx);

private:
    std::pmr::memory_resource* resource_;
    storage::block_manager_t& block_manager_;

    // Динамическая схема
    std::unique_ptr<dynamic_schema_t> schema_;

    // Текущая таблица
    std::unique_ptr<table::data_table_t> table_;

    // Маппинг document_id -> row_id
    std::pmr::unordered_map<document_id_t, size_t> id_to_row_;

    // Следующий row_id
    size_t next_row_id_;
};
```

### Реализация insert с эволюцией

```cpp
void document_table_storage_t::insert(
    document_id_t id,
    const document::document_ptr& doc
) {
    // 1. Проверяем, нужна ли эволюция схемы
    auto new_columns = schema_->evolve(doc);

    // 2. Если есть новые колонки - расширяем таблицу
    if (!new_columns.empty()) {
        evolve_schema(new_columns);
    }

    // 3. Конвертируем документ в row
    auto row = document_to_row(doc);

    // 4. Вставляем в таблицу
    table::table_append_state state(resource_);
    table_->append_lock(state);
    table_->initialize_append(state);
    table_->append(row, state);
    table_->finalize_append(state);

    // 5. Сохраняем маппинг
    id_to_row_[id] = next_row_id_++;
}
```

### evolve_schema - расширение схемы

```cpp
void document_table_storage_t::evolve_schema(
    const std::pmr::vector<column_info_t>& new_columns
) {
    // Стратегия 1: Создаем новую таблицу с расширенной схемой
    // и копируем все данные

    // Получаем новую полную схему
    auto new_column_defs = schema_->to_column_definitions();

    // Создаем новую таблицу
    auto new_table = std::make_unique<table::data_table_t>(
        resource_,
        block_manager_,
        std::move(new_column_defs)
    );

    // Копируем существующие данные
    if (table_ && table_->calculate_size() > 0) {
        migrate_data(table_.get(), new_table.get(), new_columns);
    }

    // Заменяем старую таблицу новой
    table_ = std::move(new_table);
}
```

### migrate_data - миграция данных при расширении

```cpp
void document_table_storage_t::migrate_data(
    table::data_table_t* old_table,
    table::data_table_t* new_table,
    const std::pmr::vector<column_info_t>& new_columns
) {
    // Сканируем старую таблицу
    table::table_scan_state scan_state(resource_);

    // Получаем все колонки из старой таблицы
    std::vector<table::storage_index_t> old_column_ids;
    for (size_t i = 0; i < old_table->column_count(); ++i) {
        old_column_ids.push_back(i);
    }

    old_table->initialize_scan(scan_state, old_column_ids);

    // Подготавливаем append в новую таблицу
    table::table_append_state append_state(resource_);
    new_table->append_lock(append_state);
    new_table->initialize_append(append_state);

    // Читаем и копируем данные чанками
    while (true) {
        vector::data_chunk_t old_chunk(resource_, old_table->copy_types());
        old_table->scan(old_chunk, scan_state);

        if (old_chunk.size() == 0) {
            break;  // Данные закончились
        }

        // Создаем новый chunk с расширенной схемой
        vector::data_chunk_t new_chunk(resource_, new_table->copy_types());
        new_chunk.set_cardinality(old_chunk.size());

        // Копируем существующие колонки
        for (size_t i = 0; i < old_table->column_count(); ++i) {
            new_chunk.data[i] = old_chunk.data[i];
        }

        // Заполняем новые колонки NULL значениями
        for (size_t i = old_table->column_count(); i < new_table->column_count(); ++i) {
            auto& vec = new_chunk.data[i];
            vec.set_type(new_table->copy_types()[i]);
            vec.resize(old_chunk.size());

            // Заполняем NULL
            for (size_t row = 0; row < old_chunk.size(); ++row) {
                vec.set_null(row, true);
            }
        }

        // Вставляем в новую таблицу
        new_table->append(new_chunk, append_state);
    }

    new_table->finalize_append(append_state);
}
```

### document_to_row - конвертация документа в row

```cpp
vector::data_chunk_t document_table_storage_t::document_to_row(
    const document::document_ptr& doc
) {
    // Создаем chunk на одну строку
    auto types = table_->copy_types();
    vector::data_chunk_t chunk(resource_, types);
    chunk.set_cardinality(1);

    // Проходим по всем колонкам схемы
    for (size_t i = 0; i < schema_->column_count(); ++i) {
        const auto* col_info = schema_->get_column_by_index(i);
        auto& vec = chunk.data[i];

        // Специальная обработка для _id
        if (col_info->json_path == "_id") {
            // Получаем document_id из документа
            auto doc_id = document::get_document_id(doc);
            vec.set_value(0, std::string(doc_id.bytes, doc_id.size));
            continue;
        }

        // Проверяем, есть ли это поле в документе
        if (!doc->is_exists(col_info->json_path)) {
            vec.set_null(0, true);
            continue;
        }

        // Извлекаем значение в зависимости от типа
        switch (col_info->type.type()) {
            case types::logical_type::BOOLEAN:
                vec.set_value(0, doc->get_bool(col_info->json_path));
                break;

            case types::logical_type::INTEGER:
                vec.set_value(0, static_cast<int32_t>(doc->get_int(col_info->json_path)));
                break;

            case types::logical_type::BIGINT:
                vec.set_value(0, doc->get_long(col_info->json_path));
                break;

            case types::logical_type::DOUBLE:
                vec.set_value(0, doc->get_double(col_info->json_path));
                break;

            case types::logical_type::VARCHAR:
                vec.set_value(0, std::string(doc->get_string(col_info->json_path)));
                break;

            default:
                vec.set_null(0, true);
                break;
        }
    }

    return chunk;
}
```

## Примеры использования

### Пример 1: Постепенное расширение схемы

```cpp
// Создаем хранилище
document_table_storage_t storage(resource, block_manager);

// Первый документ
auto doc1 = parse_json(R"({
    "_id": "doc1",
    "name": "Alice",
    "age": 30
})");

storage.insert("doc1", doc1);
// Схема: [_id, name, age]

// Второй документ с новыми полями
auto doc2 = parse_json(R"({
    "_id": "doc2",
    "name": "Bob",
    "age": 25,
    "city": "Moscow",
    "email": "bob@example.com"
})");

storage.insert("doc2", doc2);
// Схема расширена: [_id, name, age, city, email]
// doc1 теперь имеет NULL для city и email

// Третий документ
auto doc3 = parse_json(R"({
    "_id": "doc3",
    "name": "Charlie",
    "age": 35,
    "city": "SPb",
    "phone": "+7123456789"
})");

storage.insert("doc3", doc3);
// Схема расширена: [_id, name, age, city, email, phone]
// doc1 имеет NULL для city, email, phone
// doc2 имеет NULL для phone
```

### Пример 2: Вложенные объекты

```cpp
auto doc = parse_json(R"({
    "_id": "doc1",
    "user": {
        "profile": {
            "name": "Alice",
            "age": 30
        },
        "contacts": {
            "email": "alice@example.com",
            "phone": "+71234567890"
        }
    }
})");

storage.insert("doc1", doc);

// Схема:
// [_id, user.profile.name, user.profile.age,
//  user.contacts.email, user.contacts.phone]
```

## Оптимизации

### 1. Кэширование схемы

```cpp
// Кэш последней использованной схемы для быстрой проверки
struct schema_cache_t {
    size_t schema_version;
    std::pmr::vector<std::string> paths;
};
```

### 2. Batch эволюция

```cpp
// Собираем все новые колонки из батча документов
// и выполняем эволюцию один раз
std::pmr::vector<column_info_t> collect_new_columns(
    const std::pmr::vector<document::document_ptr>& docs
);
```

### 3. Ленивая миграция

```cpp
// Не мигрируем данные сразу, а делаем это при следующем сканировании
// Храним старые и новые колонки отдельно
```

## Ограничения

1. **Изменение типов**: Если поле меняет тип, используется VARCHAR как fallback
2. **Максимум колонок**: Ограничение на количество колонок (например, 1000)
3. **Удаление колонок**: Колонки не удаляются автоматически, даже если не используются
4. **Переименование полей**: Рассматривается как добавление новой колонки

## Метрики и мониторинг

```cpp
struct schema_stats_t {
    size_t total_columns;
    size_t evolution_count;
    size_t migration_time_ms;
    size_t null_ratio;  // Процент NULL значений
};
```

## Тесты

1. `test_schema_evolution_simple()` - добавление одной колонки
2. `test_schema_evolution_multiple()` - добавление нескольких колонок
3. `test_data_migration()` - миграция данных с NULL для новых колонок
4. `test_nested_objects()` - вложенные объекты
5. `test_type_compatibility()` - совместимость типов
6. `test_concurrent_evolution()` - конкурентное добавление документов
