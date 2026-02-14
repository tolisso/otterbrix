# Унификация document_table → table: отказ от отдельного storage type

## Проблема

Сейчас `document_table` — это **третий тип хранилища** наряду с `documents` (B-tree) и `columns` (columnar). Это создаёт тройное дублирование кода:

- 3 ветки в `executor.cpp` (`execute_plan`)
- 3 ветки в `dispatcher.cpp` (`check_collections_format_`, `update_catalog`)
- 3 набора операторов (`physical_plan/collection/`, `physical_plan/table/`, `physical_plan/document_table/`)
- 3 планировщика в `create_plan.cpp` (по namespace)
- 3 конструктора в `context_collection_t`

При этом **document_table уже хранит данные в `data_table_t`** и все его операторы scan/group/sort выдают `data_chunk_t` — тот же формат, что и table. Фактически document_table отличается от table только тем, что:

1. **INSERT**: принимает документы → извлекает JSON paths → evolve schema → вставляет в data_table
2. **Schema**: динамическая (новые колонки добавляются при вставке), а не фиксированная
3. **Column naming**: JSON paths кодируются как `commit_dot_collection` вместо `commit.collection`

## Ключевая идея

**Убрать `document_table` как отдельный storage type. Вместо этого — один `table` (columnar) с двумя режимами:**

- **Фиксированная схема** (текущий `columns`): `CREATE TABLE t(id INT, name TEXT)`
- **Динамическая схема** (текущий `document_table`): `CREATE TABLE t()` — без колонок

Преобразование документов в строки таблицы происходит **на этапе парсинга/планирования**, до того как данные попадут в storage.

## Архитектурный анализ

### Текущий пайплайн (document_table)

```
SQL: INSERT INTO t VALUES ('{"name":"Alice","age":30}')
  → parser: raw parse tree
  → transformer: logical_plan (insert_t + data_t с документами)
  → dispatcher: определяет used_format = document_table
  → executor: вызывает document_table::planner::create_plan()
  → document_table::operator_insert:
      - извлекает JSON paths из документов
      - вызывает document_table_storage.batch_insert()
      - storage внутри evolves schema + вставляет в data_table
```

### Предлагаемый пайплайн (only table)

```
SQL: INSERT INTO t VALUES ('{"name":"Alice","age":30}')
  → parser: raw parse tree
  → transformer: logical_plan (insert_t + data_t с документами)
  → **NEW** document_to_table_transformer:
      - извлекает JSON paths из документов
      - evolves schema (добавляет колонки в catalog + в data_table)
      - конвертирует документы в data_chunk_t
      - подменяет node_data_t: documents → data_chunk
  → dispatcher: определяет used_format = columns (!!!)
  → executor: вызывает table::planner::create_plan()
  → table::operator_insert: стандартная вставка data_chunk в data_table
```

### Для SELECT

```
SQL: SELECT name, age FROM t WHERE name = 'Alice'
  → parser / transformer: logical_plan как обычно
  → dispatcher: used_format = columns
  → table::planner → table::full_scan + table::operator_match
  → Стандартное колоночное сканирование
```

Здесь **ничего не меняется** — document_table full_scan уже возвращает data_chunk, а table full_scan делает то же самое. Просто используем table operators напрямую.

## Детальный план реализации

### Этап 1: Перенос schema evolution из storage в transformer layer

**Что делаем:** Создаём компонент, который на уровне логического плана конвертирует документы в data_chunk.

**Файлы:**
- Новый: `components/sql/transformer/document_to_chunk_converter.hpp/cpp`
- Или расширение: `components/sql/transformer/impl/transform_insert.cpp`

**Логика:**

```cpp
class document_to_chunk_converter_t {
public:
    // На вход: логический план с node_data_t (documents)
    // На выход: логический план с node_data_t (data_chunk)
    // + обновлённый catalog (новые колонки)
    void convert(node_ptr& plan, catalog& catalog);

private:
    // Извлечь все JSON paths из всех документов
    // Обновить catalog schema (computed_schema)
    // Сконвертировать каждый документ в строку data_chunk
};
```

**Ключевой момент:** Сейчас `document_table_storage_t::batch_insert()` делает:
1. `evolve_from_document(doc)` — определяет новые колонки
2. `evolve_schema(new_columns)` — добавляет колонки в data_table (пересоздаёт data_table!)
3. `document_to_row(doc)` — конвертирует документ в data_chunk

Шаги 1-3 можно вынести в transformer/planner уровень. Тогда storage получает готовый data_chunk и просто делает append.

### Этап 2: Модификация dispatcher

**Файл:** `services/dispatcher/dispatcher.cpp`

**Изменения:**

```
Было:
  if (catalog_.table_computes(tid)) → document_table
  if (catalog_.table_exists(tid)) → columns

Стало:
  if (catalog_.table_computes(tid) && schema.is_dynamic()) {
    // Конвертируем documents → data_chunk прямо здесь
    document_to_chunk_converter.convert(logic_plan, catalog_);
    // Теперь plan содержит data_chunk вместо documents
  }
  used_format = columns; // Всегда!
```

В `dispatcher::execute_plan()` (строки 312-371) уже есть блок для document_table INSERT, где обновляется catalog schema. Эту логику нужно расширить до полной конвертации documents → data_chunk.

### Этап 3: Устранение document_table planner и operators

**Удаляемые файлы/директории:**
- `components/physical_plan/document_table/` — все операторы
- `components/physical_plan_generator/impl/document_table/` — все планировщики
- Namespace `services::document_table::planner` в `create_plan.cpp`

**Что остаётся:**
- `components/document_table/document_table_storage.hpp/cpp` — но сильно упрощённый
- `components/document_table/json_path_extractor.hpp/cpp` — используется для извлечения путей

### Этап 4: Упрощение context_collection_t

**Файл:** `services/collection/collection.hpp`

```
Было:
  storage_type_t: DOCUMENT_BTREE, TABLE_COLUMNS, DOCUMENT_TABLE
  3 конструктора, 3 storage поля

Стало:
  storage_type_t: DOCUMENT_BTREE, TABLE_COLUMNS
  2 конструктора, 2 storage поля
  TABLE_COLUMNS теперь обрабатывает и фиксированную, и динамическую схему
```

### Этап 5: Модификация catalog

**Текущее состояние:**
- `catalog.tables` — фиксированная схема (`table_metadata`)
- `catalog.computing` — динамическая схема (`computed_schema`) + `used_format_t`

**Предложение:** Объединить в одну систему с флагом `is_dynamic_schema`:
- Таблица с фиксированной схемой: `table_metadata` + `is_dynamic = false`
- Таблица с динамической схемой: `table_metadata` + `computed_schema` + `is_dynamic = true`

Оба случая используют `used_format_t::columns` для storage.

## Потенциальные проблемы и решения

### 1. Schema evolution при INSERT: пересоздание data_table

**Проблема:** Сейчас `evolve_schema()` в `document_table_storage.cpp` пересоздаёт `data_table_t` при добавлении новой колонки (создаёт новый data_table с расширенными column_definitions, копирует данные). Это дорого.

**Решение:** Реализовать `data_table_t::add_column()` — метод, который добавляет колонку без пересоздания всей таблицы. Это нужно в любом случае, вне зависимости от архитектуры. `collection_t` и `row_group_t` тоже должны поддерживать добавление колонок.

**Альтернатива:** Собрать все уникальные пути из ВСЕХ документов в batch → evolve schema один раз → потом вставить все документы. Один раз пересоздать таблицу вместо N раз.

### 2. Column naming: `_dot_` vs точка

**Проблема:** В SQL запросах пользователь пишет `commit_dot_collection`, а JSON path в документе — `commit.collection`. Нужна трансляция.

**Решение:** Это уже решено — `json_path_extractor` и column_info_t уже делают эту трансляцию. При конвертации documents → data_chunk в transformer используем ту же маппинг-логику.

### 3. Scan с фильтрацией по JSON paths

**Проблема:** document_table full_scan использует `transform_predicate()` для конвертации compare_expression в table_filter_t. Нужна та же логика в table full_scan.

**Решение:** table full_scan уже поддерживает `table_filter_t`. Нужно только убедиться, что имена колонок в expressions совпадают с именами колонок в data_table (т.е. используют `_dot_` нотацию).

### 4. Обратная совместимость: API вставка документов

**Проблема:** MongoDB-style API (`db.collection.insertMany(docs)`) передаёт документы, а не SQL. Сейчас document_table operator_insert сам конвертирует.

**Решение:** Конвертация documents → data_chunk должна происходить до operator_insert. Два варианта:
- В `dispatcher::execute_plan()` — перед отправкой в executor
- В `executor::execute_plan()` — перед вызовом planner

Лучше в dispatcher, т.к. там уже есть catalog и schema evolution логика.

### 5. id_to_row_ mapping для point lookups

**Проблема:** document_table хранит `id_to_row_` map для O(1) lookup по document_id. В чистом table этого нет.

**Решение:** Сделать это optional feature в data_table:
- Если таблица создана с динамической схемой (из document API), поддерживается mapping `_id → row_id`
- Для обычных таблиц — не нужно
- Или: реализовать как index на колонке `_id`

## Что выигрываем

1. **Один набор операторов** вместо трёх — меньше кода, меньше багов
2. **Один planner** — проще оптимизировать
3. **Единый scan path** — table full_scan для всех
4. **Проще добавлять оптимизации** — они автоматически работают для document_table
5. **Нет дублирования** aggregation/group/sort/join — они и сейчас переиспользуются, но через хрупкое делегирование

## Что теряем / усложняется

1. **Schema evolution в dispatcher/transformer** — это не trivial, особенно при concurrent inserts (хотя сейчас single-threaded через actor model)
2. **Конвертация documents → data_chunk** должна быть быстрой — это bottleneck для INSERT
3. **Primary key scan** по document_id — нужно отдельное решение (index или mapping)

## Порядок миграции (incremental)

1. **Сначала:** Реализовать `document_to_chunk_converter` рядом с текущим кодом, без удаления старого
2. **Потом:** Подключить converter в dispatcher для document_table INSERT
3. **Проверить:** Все тесты проходят (test_jsonbench, unit tests)
4. **Потом:** Переключить document_table SELECT на table operators (убрать document_table full_scan)
5. **Потом:** Убрать `used_format_t::document_table` — всё идёт через `columns`
6. **Финально:** Удалить `physical_plan/document_table/` и связанный код

## Мои мысли

### За

Идея имеет смысл. Главный аргумент: **document_table уже хранит данные в data_table_t и выдаёт data_chunk_t**. Разница между document_table и table — только в INSERT path (конвертация документов) и schema management. Эти различия можно вынести на уровень выше (dispatcher/transformer), оставив единый storage и execution engine.

Document_table planner уже сейчас делегирует GROUP BY, ORDER BY, JOIN в table planner. `columnar_group` — по сути дубликат table group. Это code smell, который подтверждает что разделение искусственное.

### Против / Риски

1. **Производительность INSERT.** Конвертация documents → data_chunk в dispatcher может быть медленнее, чем batch_insert в storage, потому что в storage мы можем оптимизировать (кэшировать пути, делать schema evolution лениво). В dispatcher мы должны сделать полную конвертацию до передачи в executor.

2. **Schema evolution atomicity.** Сейчас batch_insert в storage атомарен — если упадёт, schema не испортится. Если перенести schema evolution в dispatcher, а insert в executor, может быть ситуация когда schema обновлена, а данные не вставлены.

3. **Сложность data_table_t::add_column().** Если нет эффективного способа добавить колонку в существующую data_table (без пересоздания), то schema evolution будет дорогой операцией.

### Рекомендация

Начать с **самой простой версии**: конвертер documents → data_chunk, который:
- Один раз проходит по всем документам и собирает все уникальные paths
- Создаёт/обновляет schema (один evolve)
- Конвертирует все документы в data_chunk
- Заменяет node_data_t с documents на node_data_t с data_chunk

Это можно сделать как preprocessing step в dispatcher::execute_plan() для INSERT, не меняя остальную архитектуру. Если это заработает и будет быстро — постепенно мигрировать SELECT и остальные операции.
