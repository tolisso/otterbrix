# TODO: Операторы индексного сканирования для document_table

## Текущее состояние

Все запросы используют `full_scan` - полное сканирование таблицы O(N).
Это работает, но медленно на больших коллекциях.

## Необходимо реализовать

### 1. primary_key_scan - Поиск по _id (КРИТИЧНЫЙ)

**Приоритет**: 🔴 ВЫСОКИЙ
**Сложность**: НИЗКАЯ (2-3 часа)
**Файлы**:
- `components/physical_plan/document_table/operators/scan/primary_key_scan.hpp`
- `components/physical_plan/document_table/operators/scan/primary_key_scan.cpp`

**Зачем**:
```javascript
// Сейчас: O(N) - сканирует всю коллекцию
db.users.findOne({_id: "507f1f77bcf86cd799439011"})

// С primary_key_scan: O(1) - прямой lookup через id_to_row_
db.users.findOne({_id: "507f1f77bcf86cd799439011"})
```

**Как реализовать**:

```cpp
class primary_key_scan : public base::operators::read_only_operator_t {
public:
    primary_key_scan(services::collection::context_collection_t* context);

    void set_document_ids(const std::pmr::vector<document::document_id_t>& ids);

private:
    void on_execute_impl(pipeline::context_t* pipeline_context) final;

    std::pmr::vector<document::document_id_t> document_ids_;
};
```

**Реализация**:
```cpp
void primary_key_scan::on_execute_impl(pipeline::context_t*) {
    auto& storage = context_->document_table_storage().storage();

    // Получаем типы колонок из схемы
    auto column_defs = storage.schema().to_column_definitions();
    std::pmr::vector<types::complex_logical_type> types(context_->resource());
    for (const auto& col_def : column_defs) {
        types.push_back(col_def.type());
    }

    output_ = base::operators::make_operator_data(context_->resource(), types);

    // Преобразуем document_id → row_id
    vector::vector_t row_ids(context_->resource(), logical_type::BIGINT);
    for (const auto& doc_id : document_ids_) {
        size_t row_id;
        if (storage.get_row_id(doc_id, row_id)) {
            row_ids.append_value(types::logical_value_t(static_cast<int64_t>(row_id)));
        }
    }

    // Fetch строк из таблицы
    if (row_ids.size() > 0) {
        std::vector<table::storage_index_t> column_indices;
        for (size_t i = 0; i < storage.table()->column_count(); ++i) {
            column_indices.emplace_back(i);
        }

        table::column_fetch_state state;
        storage.table()->fetch(output_->data_chunk(), column_indices, row_ids, row_ids.size(), state);
    }
}
```

**Активация в планировщике**:
```cpp
// В create_plan_match.cpp раскомментировать:
if (is_can_primary_key_find_by_predicate(expr->type()) && expr->key().as_string() == "_id") {
    return boost::intrusive_ptr(
        new components::document_table::operators::primary_key_scan(context_));
}
```

**Выигрыш**: findOne по _id в ~1000x быстрее на коллекции из 1M документов!

---

### 2. index_scan - Поиск по индексам (ВАЖНЫЙ)

**Приоритет**: 🟡 СРЕДНИЙ
**Сложность**: СРЕДНЯЯ (4-6 часов)
**Файлы**:
- `components/physical_plan/document_table/operators/scan/index_scan.hpp`
- `components/physical_plan/document_table/operators/scan/index_scan.cpp`

**Зачем**:
```javascript
// Создаем индекс
db.users.createIndex({email: 1})

// Сейчас: O(N) - игнорирует индекс, сканирует все
db.users.find({email: "alice@example.com"})

// С index_scan: O(log N) - использует B-tree индекс
db.users.find({email: "alice@example.com"})
```

**Как реализовать**:

```cpp
class index_scan : public base::operators::read_only_operator_t {
public:
    index_scan(services::collection::context_collection_t* context,
               const expressions::compare_expression_ptr& expression,
               logical_plan::limit_t limit);

private:
    void on_execute_impl(pipeline::context_t* pipeline_context) final;

    expressions::compare_expression_ptr expression_;
    logical_plan::limit_t limit_;
};
```

**Реализация** (адаптация table::operators::index_scan):
```cpp
void index_scan::on_execute_impl(pipeline::context_t* pipeline_context) {
    auto& storage = context_->document_table_storage().storage();

    // Используем существующую логику из table::operators::index_scan
    // Основное отличие: используем storage.table() вместо context_->table_storage().table()

    auto* index = context_->index_engine()->get_index(expression_->key_left());
    if (!index) {
        // Fallback на full_scan если индекс не найден
        full_scan fallback(context_, expression_, limit_);
        fallback.on_execute(pipeline_context);
        output_ = std::move(fallback.output_);
        return;
    }

    // Поиск через индекс (возвращает row_ids)
    auto row_ids = search_in_index(index, expression_, limit_, pipeline_context);

    // Fetch данных по row_ids
    auto column_defs = storage.schema().to_column_definitions();
    std::pmr::vector<types::complex_logical_type> types(context_->resource());
    for (const auto& col_def : column_defs) {
        types.push_back(col_def.type());
    }

    output_ = base::operators::make_operator_data(context_->resource(), types);

    std::vector<table::storage_index_t> column_indices;
    for (size_t i = 0; i < storage.table()->column_count(); ++i) {
        column_indices.emplace_back(i);
    }

    table::column_fetch_state state;
    storage.table()->fetch(output_->data_chunk(), column_indices, row_ids, row_ids.size(), state);
}
```

**Активация в планировщике**:
```cpp
// В create_plan_match.cpp раскомментировать:
if (is_can_index_find_by_predicate(expr->type()) &&
    components::index::search_index(context_->index_engine(), {expr->key_left()})) {
    return boost::intrusive_ptr(
        new components::document_table::operators::index_scan(context_, expr, limit));
}
```

**Выигрыш**: Запросы по индексированным полям в ~100x быстрее на больших коллекциях!

---

## Приоритеты реализации

1. ✅ **full_scan** (реализован) - работает для всех запросов
2. ✅ **primary_key_scan** (реализован) - O(1) поиск по _id
3. 🟡 **index_scan** - важно для производительности
4. ⚪ **transfer_scan** - не нужен для document_table

## Оценка усилий

- **primary_key_scan**: 2-3 часа (низкая сложность)
- **index_scan**: 4-6 часов (средняя сложность)
- **ИТОГО**: 6-9 часов для полной поддержки индексов

## Текущие ограничения

- ✅ findOne({_id: "..."}) работает за O(1) через primary_key_scan
- ❌ Индексы игнорируются (кроме _id), запросы по другим полям O(N)
- ✅ Функционально все работает
- ✅ Можно использовать для production (с учетом ограничения по индексам)

## Проверка необходимости

Реализовывать index/primary_key сканы стоит когда:
- Коллекции содержат > 10,000 документов
- Частые findOne по _id
- Создаются индексы на полях
- Требуется production-grade производительность

Для MVP/прототипа текущего full_scan достаточно!
