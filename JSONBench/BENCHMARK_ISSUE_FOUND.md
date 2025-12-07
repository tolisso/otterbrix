# JSONBench Integration - Найденная проблема

## Статус
✅ **Данные успешно вставляются в document_table!**  
❌ **Ошибка при вызове size() после вставки**

## Результаты теста

### Успешно
1. ✅ Парсинг 100 JSON документов из test_sample.json
2. ✅ Создание таблицы с storage='document_table'
3. ✅ Вставка 100 записей через `dispatcher->insert_many()` 
   - Время: 177ms
   - Скорость: 565 rec/sec
4. ✅ Все 100 документов вставлены (видно в логах insert)

### Проблема
❌ После успешной вставки вызов `dispatcher->size()` вызывает assertion:

```
test_otterbrix: /home/tolisso/otterbrix/components/table/collection.cpp:153: 
uint64_t components::table::collection_t::calculate_size(): Assertion `row_group' failed.
```

## Анализ

### Код ошибки
```cpp
uint64_t collection_t::calculate_size() {
    uint64_t res = 0;
    auto row_group = row_groups_->root_segment();  // <-- returns nullptr
    assert(row_group);  // <-- FAILS HERE
    while (row_group) {
        res += row_group->calculate_size();
        row_group = row_groups_->next_segment(row_group);
    }
    return res;
}
```

### Причина
После вставки данных в `document_table` через `insert_many()`:
- Данные физически записываются (видно в DEBUG логах)
- Но `row_group` не инициализируется
- `row_groups_->root_segment()` возвращает `nullptr`

Это означает, что путь вставки для `document_table` не вызывает или неправильно вызывает `initialize_append()` и `finalize_append()`.

## Логи вставки

```
[DEBUG operator_insert] Inserting document with id=000000000000000000000000
[DEBUG operator_insert] Document inserted, row_id=0
...
[DEBUG operator_insert] Document inserted, row_id=99
[DEBUG operator_insert] Inserted 100 documents, modified_->size()=100
[DEBUG operator_insert] Creating output with capacity=100, column_count=33
[DEBUG operator_insert] Output created, size=0, uses_data_chunk=1
[DEBUG operator_insert] Scanning inserted rows, modified_->size()=100
[DEBUG operator_insert] temp_chunk.size()=100, modified_->size()=100
[DEBUG operator_insert] Copying rows from index 0
[DEBUG operator_insert] Output filled, final size=100
[DEBUG operator_insert] on_execute_impl completed, output size=100
```

```
[trace] executor::insert_document_impl DEBUG: storage_type=2, has_output=true, uses_documents=false, uses_data_chunk=true
[trace] executor::insert_document_impl: Using data_chunk path
[trace] executor::insert_document_impl: DOCUMENT_TABLE branch, output size=100
[trace] executor::insert_document_impl: output_chunk size=100, column_count=33
[trace] executor::insert_document_impl: Created chunk_for_disk, size=100
[trace] executor::insert_document_impl: Created chunk_for_cursor, size=100, creating cursor
[trace] executor::execute_plan_finish, success: true
[trace] executor::insert_document_impl: DOCUMENT_TABLE branch completed
```

## Следующие шаги

1. **Найти место**, где `document_table` должен вызывать `initialize_append()` перед вставкой
2. **Проверить**, вызывается ли `finalize_append()` после вставки
3. **Возможные места** для исправления:
   - `services/collection/executor.cpp` - метод `insert_document_impl()`
   - `components/document_table/document_table_storage.cpp` - методы append

## Тест для воспроизведения

Создан интеграционный тест:
- **Файл**: `/home/tolisso/otterbrix/integration/cpp/test/test_jsonbench.cpp`
- **Команда**: `./test_otterbrix "JSONBench - document_table load test"`
- **Данные**: `/home/tolisso/otterbrix/build/integration/cpp/test/test_sample.json`

## Workaround

Можно временно закомментировать вызов `size()` и проверить, работают ли SELECT запросы.

