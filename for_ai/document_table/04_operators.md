# Document Table Operators - Спецификация операторов

## Обзор

Операторы для `document_table_storage` комбинируют подходы из `collection/operators` и `table/operators`:
- Используют колоночное хранение (`table_storage`) для производительности
- Работают с документами для удобства API
- Поддерживают динамическую схему

## Архитектура операторов

### Базовый класс

```cpp
// components/physical_plan/document_table/operators/base_operator.hpp

namespace components::document_table::operators {

class base_operator_t : public base::operators::operator_t {
public:
    base_operator_t(
        services::collection::context_collection_t* context,
        base::operators::operator_type type
    );

protected:
    // Доступ к document_table_storage
    document_table_storage_t* storage();

    // Конвертация между документами и rows
    vector::data_chunk_t documents_to_chunk(
        const std::pmr::vector<document::document_ptr>& docs
    );

    document::document_ptr chunk_row_to_document(
        const vector::data_chunk_t& chunk,
        size_t row_idx
    );
};

} // namespace components::document_table::operators
```

## Основные операторы

### 1. full_scan - полное сканирование

```cpp
// components/physical_plan/document_table/operators/scan/full_scan.hpp

namespace components::document_table::operators {

class full_scan : public base::operators::read_only_operator_t {
public:
    full_scan(
        services::collection::context_collection_t* context,
        const expressions::compare_expression_ptr& expression,
        logical_plan::limit_t limit
    );

private:
    void on_execute_impl(pipeline::context_t* pipeline_context) final;

    expressions::compare_expression_ptr expression_;
    logical_plan::limit_t limit_;
};

} // namespace components::document_table::operators
```

#### Реализация full_scan

```cpp
// components/physical_plan/document_table/operators/scan/full_scan.cpp

void full_scan::on_execute_impl(pipeline::context_t* pipeline_context) {
    auto* storage = context_->document_table_storage();

    // Получаем схему
    auto types = storage->current_schema().to_column_definitions();

    // Создаем output chunk
    output_ = base::operators::make_operator_data(context_->resource(), types);

    // Преобразуем expression в table_filter
    auto filter = transform_predicate(
        expression_,
        types,
        pipeline_context ? &pipeline_context->parameters : nullptr
    );

    // Сканируем таблицу
    table::table_scan_state state(context_->resource());

    // Получаем все колонки
    std::vector<table::storage_index_t> column_ids;
    for (size_t i = 0; i < types.size(); ++i) {
        column_ids.push_back(i);
    }

    storage->table()->initialize_scan(state, column_ids, filter.get());
    storage->table()->scan(output_->data_chunk(), state);

    // Применяем limit
    if (limit_.limit() >= 0) {
        output_->data_chunk().set_cardinality(
            std::min<size_t>(output_->data_chunk().size(), limit_.limit())
        );
    }
}
```

### 2. operator_insert - вставка документов

```cpp
// components/physical_plan/document_table/operators/operator_insert.hpp

class operator_insert final : public base::operators::read_write_operator_t {
public:
    explicit operator_insert(services::collection::context_collection_t* context);

private:
    void on_execute_impl(pipeline::context_t* pipeline_context) final;
};
```

#### Реализация operator_insert

```cpp
// components/physical_plan/document_table/operators/operator_insert.cpp

void operator_insert::on_execute_impl(pipeline::context_t* pipeline_context) {
    if (!left_ || !left_->output()) {
        return;
    }

    auto* storage = context_->document_table_storage();

    // Получаем документы из input
    const auto& input_docs = left_->output()->documents();

    // Batch эволюция схемы: собираем все новые колонки
    std::pmr::vector<extracted_path_t> all_new_paths(context_->resource());

    for (const auto& doc : input_docs) {
        auto new_paths = storage->schema()->extract_new_paths(doc);
        all_new_paths.insert(
            all_new_paths.end(),
            new_paths.begin(),
            new_paths.end()
        );
    }

    // Эволюция схемы один раз для всех новых колонок
    if (!all_new_paths.empty()) {
        storage->evolve_schema(all_new_paths);
    }

    // Конвертируем все документы в chunk
    auto chunk = documents_to_chunk(input_docs);

    // Batch insert в таблицу
    table::table_append_state state(context_->resource());
    storage->table()->append_lock(state);
    storage->table()->initialize_append(state);
    storage->table()->append(chunk, state);
    storage->table()->finalize_append(state);

    // Индексирование
    if (pipeline_context) {
        for (size_t i = 0; i < chunk.size(); ++i) {
            context_->index_engine()->insert_row(chunk, i, pipeline_context);
        }
    }

    // Создаем output
    output_ = base::operators::make_operator_data(
        context_->resource(),
        std::move(chunk)
    );
}
```

### 3. operator_delete - удаление документов

```cpp
// components/physical_plan/document_table/operators/operator_delete.hpp

class operator_delete final : public base::operators::read_write_operator_t {
public:
    explicit operator_delete(
        services::collection::context_collection_t* context,
        expressions::compare_expression_ptr expr = nullptr
    );

private:
    void on_execute_impl(pipeline::context_t* pipeline_context) final;

    expressions::compare_expression_ptr compare_expression_;
};
```

#### Реализация operator_delete

```cpp
// components/physical_plan/document_table/operators/operator_delete.cpp

void operator_delete::on_execute_impl(pipeline::context_t* pipeline_context) {
    auto* storage = context_->document_table_storage();

    vector::vector_t ids(context_->resource(), types::logical_type::UBIGINT);

    if (left_ && left_->output()) {
        // Удаление по результату join
        const auto& chunk_left = left_->output()->data_chunk();
        const auto& chunk_right = right_->output()->data_chunk();

        // Собираем row_ids для удаления
        for (size_t i = 0; i < chunk_left.size(); ++i) {
            for (size_t j = 0; j < chunk_right.size(); ++j) {
                // Проверяем предикат
                if (predicate_->check(chunk_left, chunk_right, i, j)) {
                    // Получаем row_id из левой таблицы
                    auto row_id = chunk_left.row_ids.get_value<size_t>(i);
                    ids.append_value(row_id);
                }
            }
        }
    } else {
        // Прямое удаление с предикатом
        auto predicate = predicates::create_predicate(
            compare_expression_,
            storage->table()->copy_types(),
            storage->table()->copy_types(),
            pipeline_context ? &pipeline_context->parameters : nullptr
        );

        // Сканируем таблицу для поиска строк
        table::table_scan_state scan_state(context_->resource());

        std::vector<table::storage_index_t> column_ids;
        for (size_t i = 0; i < storage->table()->column_count(); ++i) {
            column_ids.push_back(i);
        }

        storage->table()->initialize_scan(scan_state, column_ids);

        vector::data_chunk_t chunk(context_->resource(), storage->table()->copy_types());
        storage->table()->scan(chunk, scan_state);

        // Собираем row_ids для удаления
        for (size_t i = 0; i < chunk.size(); ++i) {
            if (predicate->check(chunk, chunk, i, i)) {
                ids.append_value(chunk.row_ids.get_value<size_t>(i));
            }
        }
    }

    // Удаляем строки batch операцией
    if (ids.size() > 0) {
        auto state = storage->table()->initialize_delete({});
        storage->table()->delete_rows(*state, ids, ids.size());

        // Обновляем индексы
        for (size_t i = 0; i < ids.size(); ++i) {
            size_t row_id = ids.get_value<size_t>(i);
            context_->index_engine()->delete_row(row_id, pipeline_context);
        }
    }

    // Создаем modified_ с удаленными row_ids
    modified_ = base::operators::make_operator_write_data<size_t>(context_->resource());
    for (size_t i = 0; i < ids.size(); ++i) {
        modified_->append(ids.get_value<size_t>(i));
    }
}
```

### 4. operator_update - обновление документов

```cpp
// components/physical_plan/document_table/operators/operator_update.hpp

class operator_update final : public base::operators::read_write_operator_t {
public:
    operator_update(
        services::collection::context_collection_t* context,
        std::pmr::vector<expressions::update_expr_ptr> updates,
        bool upsert,
        expressions::compare_expression_ptr comp_expr = nullptr
    );

private:
    void on_execute_impl(pipeline::context_t* pipeline_context) final;

    std::pmr::vector<expressions::update_expr_ptr> updates_;
    bool upsert_;
    expressions::compare_expression_ptr comp_expr_;
};
```

#### Реализация operator_update

```cpp
void operator_update::on_execute_impl(pipeline::context_t* pipeline_context) {
    auto* storage = context_->document_table_storage();

    if (upsert_) {
        // UPSERT: вставка с возможным расширением схемы
        // Аналогично operator_insert, но проверяем существование
        // TODO: реализация upsert
    } else {
        // Обычный UPDATE
        auto& chunk_left = left_->output()->data_chunk();

        // Вычисляем новые значения
        vector::data_chunk_t updated_chunk = evaluate_updates(
            chunk_left,
            updates_,
            pipeline_context
        );

        // Применяем предикат для фильтрации строк
        auto predicate = comp_expr_
            ? predicates::create_predicate(comp_expr_, ...)
            : predicates::create_all_true_predicate(context_->resource());

        vector::vector_t row_ids(context_->resource(), types::logical_type::UBIGINT);

        for (size_t i = 0; i < chunk_left.size(); ++i) {
            if (predicate->check(chunk_left, chunk_left, i, i)) {
                row_ids.append_value(chunk_left.row_ids.get_value<size_t>(i));
            }
        }

        // Batch update
        auto state = storage->table()->initialize_update({});
        storage->table()->update(*state, row_ids, updated_chunk);

        // Обновляем индексы
        for (size_t i = 0; i < row_ids.size(); ++i) {
            context_->index_engine()->update_row(
                updated_chunk,
                i,
                pipeline_context
            );
        }

        // Результат
        modified_ = base::operators::make_operator_write_data<size_t>(context_->resource());
        for (size_t i = 0; i < row_ids.size(); ++i) {
            modified_->append(row_ids.get_value<size_t>(i));
        }
    }
}
```

### 5. operator_group - группировка

```cpp
// components/physical_plan/document_table/operators/operator_group.hpp

class operator_group_t final : public base::operators::read_write_operator_t {
public:
    explicit operator_group_t(services::collection::context_collection_t* context);

    void add_key(const std::string& name, get::operator_get_ptr&& getter);
    void add_aggregate(aggregate::operator_aggregate_ptr&& aggregate);

private:
    void on_execute_impl(pipeline::context_t* pipeline_context) final;

    std::pmr::vector<std::pair<std::string, get::operator_get_ptr>> keys_;
    std::pmr::vector<aggregate::operator_aggregate_ptr> aggregates_;
};
```

#### Реализация operator_group

```cpp
void operator_group_t::on_execute_impl(pipeline::context_t* pipeline_context) {
    if (!left_ || !left_->output()) {
        return;
    }

    auto& input_chunk = left_->output()->data_chunk();

    // Используем такой же подход, как table::operators::operator_group
    // но работаем с данными из document_table_storage

    // 1. Группируем строки по ключам
    std::pmr::unordered_map<
        std::pmr::vector<logical_value_t>,  // ключи группы
        std::pmr::vector<size_t>             // индексы строк в группе
    > groups(context_->resource());

    for (size_t i = 0; i < input_chunk.size(); ++i) {
        std::pmr::vector<logical_value_t> key_values(context_->resource());

        for (const auto& [name, getter] : keys_) {
            key_values.push_back(getter->get(input_chunk, i));
        }

        groups[key_values].push_back(i);
    }

    // 2. Вычисляем агрегаты для каждой группы
    std::pmr::vector<types::complex_logical_type> result_types(context_->resource());

    // Добавляем типы для ключей
    for (const auto& [name, getter] : keys_) {
        result_types.push_back(getter->type());
    }

    // Добавляем типы для агрегатов
    for (const auto& agg : aggregates_) {
        result_types.push_back(agg->type());
    }

    // Создаем output chunk
    output_ = base::operators::make_operator_data(
        context_->resource(),
        result_types
    );

    auto& out_chunk = output_->data_chunk();
    out_chunk.set_cardinality(groups.size());

    size_t group_idx = 0;
    for (const auto& [key_values, row_indices] : groups) {
        // Записываем ключи
        for (size_t k = 0; k < keys_.size(); ++k) {
            out_chunk.data[k].set_value(group_idx, key_values[k]);
        }

        // Вычисляем агрегаты
        for (size_t a = 0; a < aggregates_.size(); ++a) {
            auto agg_value = aggregates_[a]->compute(input_chunk, row_indices);
            out_chunk.data[keys_.size() + a].set_value(group_idx, agg_value);
        }

        ++group_idx;
    }
}
```

## Вспомогательные компоненты

### document_chunk_converter - конвертация между форматами

```cpp
// components/document_table/document_chunk_converter.hpp

class document_chunk_converter {
public:
    // Документы → chunk
    static vector::data_chunk_t to_chunk(
        const std::pmr::vector<document::document_ptr>& docs,
        const dynamic_schema_t& schema,
        std::pmr::memory_resource* resource
    );

    // Chunk → документы
    static std::pmr::vector<document::document_ptr> to_documents(
        const vector::data_chunk_t& chunk,
        const dynamic_schema_t& schema,
        std::pmr::memory_resource* resource
    );

    // Один row → документ
    static document::document_ptr row_to_document(
        const vector::data_chunk_t& chunk,
        size_t row_idx,
        const dynamic_schema_t& schema,
        std::pmr::memory_resource* resource
    );
};
```

## Файловая структура

```
components/physical_plan/document_table/
├── operators/
│   ├── base_operator.hpp
│   ├── base_operator.cpp
│   ├── scan/
│   │   ├── full_scan.hpp
│   │   ├── full_scan.cpp
│   │   ├── index_scan.hpp
│   │   ├── index_scan.cpp
│   │   └── primary_key_scan.hpp
│   ├── operator_insert.hpp
│   ├── operator_insert.cpp
│   ├── operator_delete.hpp
│   ├── operator_delete.cpp
│   ├── operator_update.hpp
│   ├── operator_update.cpp
│   ├── operator_group.hpp
│   ├── operator_group.cpp
│   ├── operator_join.hpp
│   ├── operator_join.cpp
│   ├── operator_match.hpp
│   └── operator_match.cpp
└── document_chunk_converter.hpp
    document_chunk_converter.cpp
```

## Особенности реализации

### 1. Работа с operator_data_t

Операторы возвращают данные в формате `data_chunk_t`, но могут принимать документы:

```cpp
// Input: documents
if (left_->output()->uses_documents()) {
    auto docs = left_->output()->documents();
    auto chunk = documents_to_chunk(docs);
    // обработка chunk
}

// Output: всегда data_chunk_t
output_ = base::operators::make_operator_data(resource, chunk);
```

### 2. Эволюция схемы в операторах

Только `operator_insert` и `operator_update` (с upsert) могут вызывать эволюцию схемы:

```cpp
if (storage->needs_evolution(doc)) {
    storage->evolve_schema(doc);
    // схема изменилась, нужно пересоздать output
}
```

### 3. Индексирование

Все изменяющие операторы должны обновлять индексы:

```cpp
// Insert
context_->index_engine()->insert_row(chunk, row_id, pipeline_context);

// Delete
context_->index_engine()->delete_row(row_id, pipeline_context);

// Update
context_->index_engine()->update_row(chunk, row_id, pipeline_context);
```

## Тесты

### Unit тесты

1. `test_full_scan()` - сканирование с фильтрами
2. `test_insert_simple()` - простая вставка
3. `test_insert_with_evolution()` - вставка с расширением схемы
4. `test_delete()` - удаление
5. `test_update()` - обновление
6. `test_group()` - группировка
7. `test_join()` - join двух document_tables

### Интеграционные тесты

1. Полный цикл: insert → scan → update → delete
2. Эволюция схемы при batch insert
3. Производительность vs collection/operators
