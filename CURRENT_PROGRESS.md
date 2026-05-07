# Sparse computed-schema — текущий прогресс

Ветка: `sparse-computed-schema`. Цель — переписать computed-schema (динамические таблицы otterbrix) с «полная колонка с null'ами» на «main + side-таблицы (`row_id`, `value`)», чтобы не платить памятью за разреженные поля.

## Архитектура (что физически происходит)

### CREATE TABLE db.t()
- catalog: `create_computing_table(id)` — пустая `computed_schema`.
- storage: main table создаётся **с одной колонкой `row_id BIGINT NOT NULL`** (`dispatcher.cpp:566+`, `base_spaces.cpp:299-310`).

### INSERT INTO t (a, b) VALUES (...)
Логика в `dispatcher.cpp:446+` (case `node_type::insert_t` для computing tables):

1. Для каждого нового `(field, type)`:
   - Имя side: `_dyn_<main>__<field>__<type_id>` (`computed_schema::side_table_name`).
   - Если side ещё не существует — создать как обычный TABLE_COLUMNS с колонками `(row_id BIGINT NOT NULL, value <type> NOT NULL)`. Колонка `value` имеет `complex_logical_type::alias()` = field_name (важно для последующего разрешения ссылок).
   - `computed_schema::append(field, type)` — отметить.
2. `next_row_id = storage_total_rows(main)` — auto-increment.
3. Для каждого field — построить chunk `(row_id, value)` из NOT-NULL значений и сделать `execute_plan_impl` с `make_node_insert(side, chunk)` — отдельная транзакция на каждую side.
4. Для main: построить chunk с одной колонкой `row_id` (`next_row_id`, `next_row_id+1`, ...) и заменить `data_node` в исходном `logic_plan`. Затем `execute_plan_impl` основного INSERT.

**Атомарность**: каждая под-INSERT в свою транзакцию (main и каждая side). На crash в середине — inconsistency между main и side. Принято для MVP.

### SELECT * FROM t WHERE a > X — plan rewrite

В `components/planner/expand_computing.cpp::expand_computing_tables`. Вызывается в `dispatcher.cpp` ДО `validate_types` / `validate_schema` (только для default case — не для INSERT).

Алгоритм для `aggregate(t)` где `t` — computing:
1. Получить `column_order` из `latest_types_struct()`.
2. Построить chain LEFT JOIN: `raw_main(t).set_raw_computing_scan(true)` LEFT JOIN side_1 LEFT JOIN side_2 ... — все ON `main.row_id = side.row_id`.
3. Положить в subquery_aggregate(`{}`) с `result_alias = collection_name(t)` (чтобы внешний `t.field` резолвился).
4. Внутри subquery — `node_select_t` с проекцией ровно user-полей (скрывает row_id наружу).
5. Создать `new_outer = aggregate({})`. Перетащить детей старого aggregate (match/select/sort/group), причём `match` пересоздать с `collection = {}` — иначе filter pushdown создаст `full_scan(t, filter)` на main и упадёт в `row_group::get_column` (filter ссылается на user-field, но main physically = `[row_id]`).
6. Подвесить `subquery_agg` как `node_data` child.

Subquery помечен `is_computing_subquery_wrapper_ = true` — для post-validate fixup.

### Post-validate path fixup (`fixup_computing_paths`)

Validator резолвит все `key.path()` против самой глубокой схемы (JOIN-output: `[row_id, a, b]`). Но subquery_agg проектирует только user-поля (`[a, b]`). Без fixup внешний `WHERE a=X` имел бы `path=[1]` в JOIN-схеме, а в реально приходящем chunk-е `[a, b]` это бы дало `path=[1]=b` — silently incorrect — или out-of-bounds → SIGSEGV.

Fixup проходит по дереву (`expand_computing.cpp`):
1. На каждом aggregate, если есть child — `is_computing_subquery_wrapper`:
   - Из его `select_node` строит `path_map[old_path[0] = output_position]` (по позициям scalar-expressions в проекции).
   - Walks все sibling children (match, select, sort, group, having) и переписывает `key.path()[0]` через map.
2. Поддерживаемые типы expressions: `compare`, `scalar`, `function`, `aggregate`, `sort`. Для `aggregate_expression_t` пришлось добавить `key_t& key()` mutable accessor (раньше только const) в `components/expressions/aggregate_expression.{hpp,cpp}`.

### Validator патч

`services/dispatcher/validate_logical_plan.cpp:1062` — для `aggregate(t)` с `is_raw_computing_scan() == true` schema = `[row_id BIGINT]` (физическая, не latest_types_struct). Это раз-рекурсирует rewrite.

### column_pruning патч

`components/planner/optimizer/rules/column_pruning.cpp:138` — для raw-scan `resolve_column_count` возвращает 1.

## Изменённые файлы (актуальные на момент написания)

```
components/catalog/computed_schema.{hpp,cpp}        — side_table_name(main, field, type)
components/expressions/aggregate_expression.{hpp,cpp} — mutable key() accessor
components/logical_plan/node_aggregate.hpp           — флаги raw_computing_scan_, computing_subquery_wrapper_
components/planner/expand_computing.{hpp,cpp}        — НОВЫЙ файл: expand_computing_tables + fixup_computing_paths
components/planner/CMakeLists.txt                    — +expand_computing.cpp
components/planner/optimizer/rules/column_pruning.cpp — raw-scan branch
services/dispatcher/dispatcher.{hpp,cpp}             — INSERT split, вызов expand+fixup
services/dispatcher/validate_logical_plan.cpp        — raw-scan schema
integration/cpp/base_spaces.cpp                      — row_id колонка при load
integration/cpp/test/test_computed_schema.cpp        — новые тесты
CMakeLists.txt                                       — -Wno-mismatched-new-delete для GCC
```

## Покрытие тестами (`integration/cpp/test/test_computed_schema.cpp`)

| Тест | Сценарий | Статус |
|---|---|---|
| `basic_insert_and_select` | `SELECT *` / `SELECT field` после простого INSERT | ✅ |
| `evolving_schema` | Несколько INSERT с разными столбцами; WHERE на dynamic-поле | ✅ |
| `order_by` | `ORDER BY field ASC/DESC` | ✅ |
| `compound_where` | `WHERE a > X AND a < Y`, `WHERE a=X OR b=Y` | ✅ |
| `group_by` | `GROUP BY field`, plus `HAVING SUM(val) > X` (агрегат должен быть в SELECT-list — общее ограничение otterbrix) | ✅ |
| `table_qualified_no_alias` | `SELECT t.a FROM t` (qualified без AS) | ✅ |
| `table_alias` | `SELECT x.a FROM t AS x WHERE x.a=Y` | ✅ |
| `limit_offset` | `LIMIT 3` после ORDER BY | ✅ |
| `delete_rows` | `DELETE FROM t WHERE id <= 2` | ✅ |
| `multi_type_field` | Поле с двумя типами одновременно (`val::BIGINT` и `val::STRING`) | ❌ TODO |

## Что предстоит сделать

### 1. ~~DELETE на computing tables~~ ✅ работает

Реализовано в `dispatcher.cpp` — case `node_type::delete_t` для computing.
Алгоритм:
1. `build_select_row_ids` (новый helper в `expand_computing.cpp`) строит JOIN-цепочку
   с явной `[main.row_id]`-проекцией (без subquery wrapper, чтобы row_id выходил наружу).
2. Validate + fixup_paths + execute_plan_impl на этом select → cursor с row_ids.
3. Для каждого row_id и для каждой target (main + sides): build node_delete с
   match `row_id == $param`, path для row_id pre-resolved на column 0 (validator
   на DELETE computing main отдал бы `latest_types_struct` без row_id и упал бы),
   execute_plan_impl. O(N * (1 + sides)) DELETE-statements.

Не оптимально по перфомансу для больших batch-ей. Можно ускорить через compound
`OR row_id == r1 OR row_id == r2 ...` или специальный `IN (...)`-операнд, но это
не блокер для MVP.

### UPDATE на computing tables — TODO

UPDATE `SET field = X` на side нуждается в "upsert по row_id" — INSERT если row_id
отсутствует в этой side (новое значение для бывшего NULL), UPDATE если есть.
Существующий `intercept_dml_io_` в executor не умеет multi-table.
Подход аналогичный DELETE: SELECT row_id WHERE expr → для каждого row_id выполнить
upsert на side. Объём средний.

### 2. multi-type field (`val::BIGINT` и `val::STRING` одновременно)

Сейчас тест `multi_type_field` ожидает ошибку при `SELECT *` если поле имеет 2 типа, и SUCCESS на explicit cast (`SELECT id, val::string FROM t4`).

В нашей schema `(field, type)` — каждое — отдельная side-таблица. Significantly это уже работает на стороне INSERT: в INSERT side-tables `_dyn_t__val__14` (BIGINT) и `_dyn_t__val__35` (STRING) обе создаются. Но `latest_types_struct()` вернёт оба type-варианта.

**Что нужно:**
- В `expand_one_aggregate` обнаружить ситуацию multi-type: если `field` встречается с >1 типом — какой type выбирать?
- На уровне SQL: explicit cast `val::bigint` → проектировать только `_dyn_t__val__14`. Без cast — вернуть error «ambiguous field».
- Cast-cast handling в SQL уже работает на regular tables; нужно проверить что после моего expand он продолжает работать.

**Объём:** средний. Скорее всего достаточно изменить логику выбора side-таблицы по cast в expand_one_aggregate.

### 3. JOIN computing с regular / computing с computing

Сейчас `expand_recursive` рекурсивно меняет каждый `aggregate(computing_t)` в дереве на `new_outer({}) + subquery`. Если этот aggregate был child `node_join_t` (т.е. computing участвует в SQL-JOIN), мы превращаем JOIN child в обёртку. Это:
- Ломает `column_pruning::process_join` который ожидает aggregate child-а с известным `resolve_column_count`.
- Возможно ломает `validate_schema` для JOIN-узла (схема с `result_alias`, имя коллекции).

**Что нужно:**
- При expand: если parent — `node_join_t`, не оборачивать в `aggregate({})` + subquery — просто заменить aggregate child на subquery directly (без обёртки), либо сделать subquery сохраняющим инвариант «one collection-name visible to outer».
- Возможно нужны тесты конкретных сценариев JOIN-а: `SELECT t1.a, t2.b FROM computing_t1 JOIN regular_t2 ON ...`.

**Объём:** средний-большой. Нужно детально проследить как JOIN-валидатор/optimizer/operator резолвят references.

### 4. ~~HAVING с aggregate function~~ ✅ работает

При условии что aggregate появляется в SELECT-list (общее ограничение otterbrix —
`resolve_having_operand` ищет соответствующий aggregate в `group->expressions()`,
если не нашёл — возвращает literal-key с именем функции, что потом фейлит validator).
Это не моё ограничение, существует и для regular tables. Тест `group_by` покрывает.

Если когда-нибудь захотят поддержать HAVING без дублирования агрегата в SELECT —
работа в transformer / `resolve_having_operand`, не в моём rewrite.

### 5. Атомарность INSERT (main + sides)

Сейчас main-INSERT и каждая side-INSERT — независимые транзакции (executor закрывает txn после каждого `execute_plan_impl`). Crash между INSERT main и INSERT side оставит row_id в main без значений в sides, и наоборот.

**Что нужно:**
- Расширить `intercept_dml_io_` чтобы один DML логически писал в несколько collections под одной транзакцией. Это значимая переделка executor-а.
- Альтернатива: объединять main+side данные в один общий append через специальный operator, который за одну транзакцию пишет во все физические таблицы.

**Объём:** большой. Связан с задачей про DELETE/UPDATE — общая проблема multi-table DML.

### 6. Регрессии в существующих тестах

Под мои изменения попадают любые места где раньше использовалось `dispatcher->create_collection(...)` без колонок (= computing). Проблема — некоторые тесты выполняют сложные SELECT-ы (включая computing tables в JOIN-ах, `SELECT *, name, count, *` и т.п.) и крашатся.

Известно:
- `test_collection::sql::base` — крашится на `unhandled_exception()` в actor-zeta после INSERT 100 rows + cложный SELECT (`$aggregate: {$select: {*, name, count, *}}`). Не диагностирован глубже без gdb.
- `test_sql_features` — крашится на `IN with integers`. Аналогично, нужен backtrace.

**Что нужно:**
- Запустить эти тесты под gdb / ASan, получить точный stack trace.
- Скорее всего корни — в одном из (3, 4) выше.

### 7. Производительность: pushdown по полям

Сейчас expand JOIN-ит **все** side-таблицы независимо от того, упоминаются ли их поля в SELECT. Для широкой computing-таблицы (10+ полей) и SELECT всего одного — мы делаем 10 JOIN-ов вхолостую.

**Что нужно:**
- В `expand_one_aggregate`: до строительства JOIN-цепочки, обойти исходное aggregate-поддерево и собрать все упоминаемые поля (от expressions). Если есть `*` — все. Иначе — только упомянутые. JOIN-ить только нужные side-таблицы.
- Вспомогательный walker уже частично есть в `column_pruning.cpp::collect_cols_from_node` — можно использовать как образец.

**Объём:** средний. Аккуратное обхождение всех expression-форм + handling `*`.

## Дизайн-границы / открытые вопросы

- **Главное расхождение с `json-sparse-3-with-chunks`**: там used **threshold-based promotion** — sparse поля при наборе ≥N значений переезжают в main как обычная колонка через `storage_patch_column`. У нас всегда side, без promotion. Менее адаптивно но проще. См. `CONCURRENCY.md` для параллельной ветки.
- **`row_id` в side-таблицах** = position в main (через `storage_total_rows`). Стабильно пока нет VACUUM. Если delete + compact сдвинут row_ids — side-таблицы будут указывать на «не те» позиции.
- **WHERE pushdown в storage не работает** для dynamic-полей (filter применяется как post-scan operator_match_t). Это не оптимально по производительности но семантически верно. Pushdown через JOIN в otterbrix не реализован вообще, так что мы не теряем по сравнению с обычными JOIN-запросами.

## Как продолжить

Рекомендуемый порядок:
1. **UPDATE на computing** — DELETE сделан, UPDATE по аналогии (upsert по row_id для каждой side).
2. **Регрессии в существующих тестах (#6)** — диагностика под gdb.
3. **JOIN computing с regular (#3)** — следующий по полезности.
4. **multi-type field (#2)** — корнер-кейс.
5. **Performance pushdown (#7)** — оптимизация после функциональной полноты.
