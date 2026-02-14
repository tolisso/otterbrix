# Миграция document_table → table: пошаговый лог

## Шаг 1: INSERT preprocessing (выполнен)

**Цель:** INSERT в document_table идёт через стандартный table::operator_insert

**Изменения:**
- `document_table_storage.hpp/cpp` — добавлен `prepare_insert()`: schema evolution + documents→data_chunk + id_to_row update, без append
- `logical_plan/node_data.hpp/cpp` — добавлен `set_data_chunk()`: замена documents на data_chunk в variant
- `services/collection/collection.hpp` — добавлен `data_table()` accessor: единый доступ к data_table для TABLE_COLUMNS и DOCUMENT_TABLE
- `table/operators/operator_insert.cpp` — `table_storage().table()` → `data_table()`
- `services/collection/executor.cpp` — preprocessing в `execute_plan()`: при document_table INSERT с документами конвертирует docs→data_chunk через `prepare_insert()`, переключает routing на table planner

**Результат:** INSERT 100k docs: ~4 сек (было ~10 сек). Q1 query: ~50ms. Все тесты проходят.

## Шаг 2: Все table операторы через data_table() (выполнен)

**Цель:** Все table операторы используют `data_table()` вместо `table_storage().table()`, чтобы работать и с TABLE_COLUMNS, и с DOCUMENT_TABLE

**Изменения:**
- `table/operators/scan/full_scan.cpp` — `table_storage().table()` → `data_table()`
- `table/operators/scan/primary_key_scan.cpp` — `table_storage().table()` → `data_table()`
- `table/operators/scan/transfer_scan.cpp` — `table_storage().table()` → `data_table()`
- `table/operators/scan/index_scan.cpp` — `table_storage().table()` → `data_table()`
- `table/operators/operator_delete.cpp` — `table_storage().table()` → `data_table()`
- `table/operators/operator_update.cpp` — `table_storage().table()` → `data_table()`
- `services/collection/executor.cpp` — `table_storage().table()` → `data_table()` в aggregate/update/delete/insert_document_impl
- `services/memory_storage/memory_storage.cpp` — `table_storage().table()` → `data_table()` в size

**Результат:** Все table операторы теперь используют `data_table()` accessor и готовы работать с DOCUMENT_TABLE storage.

## Шаг 2.1: Попытка routing SELECT через table planner (откат)

**Проблема:** При переключении SELECT на table planner: Q1 деградировал с 50ms до 1300ms

**Причины:**
1. `table::operator_group` (~1100ms) vs `document_table::columnar_group` (7ms) — разница 150x
2. `table::full_scan` сканирует все 6 колонок (нет projection) vs `document_table::full_scan` сканирует только 1

**Решение:** SELECT/DELETE/UPDATE пока остаются через document_table planner. Нужно:
- Перенести `columnar_group` в table namespace (или заменить `operator_group`)
- Добавить projection support в `table::full_scan`

## Следующие шаги

1. Перенести `columnar_group` из document_table в table operators
2. Добавить projection в `table::full_scan`
3. Переключить SELECT через table planner
4. Переключить DELETE/UPDATE через table planner
5. Удалить `physical_plan/document_table/` операторы
6. Удалить `used_format_t::document_table`
