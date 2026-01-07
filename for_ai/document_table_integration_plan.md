# Document Table Integration Plan

## Текущая архитектура интеграции

### Обзор

document_table - это гибридное хранилище, объединяющее:
- **Колоночное хранение** (как в аналитических БД) - быстрые агрегации
- **Динамическую схему** (как в документных БД) - schema evolution
- **Поиск по _id** через hash-map - O(1) доступ к документу

### Точки интеграции

```
┌─────────────────────────────────────────────────────────────────┐
│                         SQL Query                                │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Logical Plan                                  │
│  (содержит storage_format из CREATE TABLE ... WITH storage=...) │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│              memory_storage_t::create_collection_                │
│                                                                  │
│  if (storage_format == document_table)                          │
│      → context_collection_t(DOCUMENT_TABLE)                     │
│          → document_table_storage_wrapper_t                     │
│              → document_table_storage_t                         │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                   executor_t::execute_plan                       │
│                                                                  │
│  if (data_format == document_table)                             │
│      plan = document_table::planner::create_plan(...)           │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│              document_table::planner::create_plan                │
│                                                                  │
│  INSERT  → impl::create_plan_insert  → operator_insert          │
│  SELECT  → impl::create_plan_match   → full_scan/primary_key    │
│  DELETE  → impl::create_plan_delete  → operator_delete          │
│  UPDATE  → impl::create_plan_update  → operator_update          │
│  GROUP BY → impl::create_plan_aggregate → aggregation           │
│  SORT/JOIN → table::planner (переиспользование)                 │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                  Operator Execution                              │
│                                                                  │
│  operator_insert.on_execute()                                   │
│      → storage.batch_insert(documents)                          │
│          → schema_->evolve(doc)  // schema evolution            │
│          → document_to_row(doc)  // conversion                  │
│          → table_->append(row)   // columnar storage            │
└─────────────────────────────────────────────────────────────────┘
```

### Ключевые файлы

| Компонент | Файл | Описание |
|-----------|------|----------|
| Storage | `components/document_table/document_table_storage.hpp` | Основной класс хранилища |
| Wrapper | `services/collection/collection.hpp` | document_table_storage_wrapper_t |
| Creation | `services/memory_storage/memory_storage.cpp:240-283` | Создание коллекции |
| Executor | `services/collection/executor.cpp:86-126` | Выбор планера |
| Planner | `components/physical_plan_generator/create_plan.cpp:113-151` | Диспетчер планеров |
| Operators | `components/physical_plan/document_table/operators/` | Операторы |

### Поддерживаемые операции

| Операция | Оператор | Статус |
|----------|----------|--------|
| INSERT | operator_insert | ✓ batch_insert + schema evolution |
| SELECT | full_scan / primary_key_scan | ✓ с projection |
| DELETE | operator_delete | ✓ |
| UPDATE | operator_update | ✓ + UPSERT |
| GROUP BY | aggregation | ✓ COUNT, SUM, AVG, MIN, MAX, COUNT DISTINCT |
| ORDER BY | (table::planner) | ✓ переиспользуется |
| JOIN | (table::planner) | ✓ переиспользуется |
| LIMIT | (table::planner) | ✓ переиспользуется |

---

## Альтернативные варианты интеграции

### Вариант 1: Текущий (отдельный planner)

```
collection::planner      ← documents (B-tree)
table::planner          ← columns (строгая схема)
document_table::planner ← document_table (гибрид)
```

**Плюсы:**
- Чёткое разделение ответственности
- Можно оптимизировать под специфику storage
- Легко тестировать изолированно

**Минусы:**
- Дублирование кода (SORT, JOIN переиспользуются из table::planner)
- Три параллельные ветки в executor
- Сложнее поддерживать консистентность

---

### Вариант 2: Унифицированный planner с адаптерами

```
┌──────────────────────────────────────┐
│         unified_planner              │
│                                      │
│  create_plan(logical_plan, adapter)  │
│      adapter->create_scan()          │
│      adapter->create_insert()        │
│      adapter->create_aggregate()     │
└──────────────────────────────────────┘
           │
           ├── documents_adapter
           ├── columns_adapter
           └── document_table_adapter
```

**Плюсы:**
- Единая точка входа
- Меньше дублирования
- Проще добавлять новые storage types

**Минусы:**
- Сложнее оптимизировать под конкретный storage
- Больше абстракций
- Требует рефакторинг существующего кода

---

### Вариант 3: Storage как trait/interface

```cpp
class storage_interface_t {
    virtual scan_result_t scan(filter_t, projection_t) = 0;
    virtual void insert(documents_t) = 0;
    virtual void delete_(filter_t) = 0;
    virtual aggregate_result_t aggregate(group_by_t, aggregates_t) = 0;
};

class document_table_storage_t : public storage_interface_t {
    // Реализация для document_table
};
```

**Плюсы:**
- Чистый полиморфизм
- Единый интерфейс для всех операций
- Planner не знает о конкретном storage

**Минусы:**
- Потеря специфических оптимизаций (primary_key_scan)
- Виртуальные вызовы
- Сложнее реализовать push-down оптимизации

---

### Вариант 4: Document Table как default storage

Сделать document_table основным типом хранилища:

```
CREATE TABLE db.table();           -- document_table по умолчанию
CREATE TABLE db.table() WITH (storage='documents');  -- явно B-tree
CREATE TABLE db.table(col1 INT, col2 TEXT);         -- строгая схема
```

**Плюсы:**
- Лучший UX для пользователей (schema evolution из коробки)
- Единая кодовая база
- Можно постепенно убрать documents storage

**Минусы:**
- Больше памяти (колоночное хранение)
- Может быть медленнее для простых операций
- Breaking change для существующих пользователей

---

### Вариант 5: Lazy conversion между storage types

```
documents → document_table (при первой агрегации)
document_table → documents (при частых point queries)
```

**Плюсы:**
- Автоматическая оптимизация под workload
- Пользователь не думает о storage type
- Гибкость

**Минусы:**
- Сложная реализация
- Overhead на конвертацию
- Непредсказуемая производительность

---

## Рекомендация

Для текущего этапа развития рекомендуется **Вариант 1 (текущий)** с постепенным движением к **Варианту 4**:

1. **Краткосрочно**: Оставить текущую архитектуру, оптимизировать document_table
2. **Среднесрочно**: Сделать document_table дефолтным для новых таблиц без схемы
3. **Долгосрочно**: Унифицировать documents и document_table, оставив columns для OLAP

### План действий

1. ✓ Базовая интеграция document_table (сделано)
2. ✓ Поддержка агрегаций с GROUP BY (сделано)
3. ✓ COUNT DISTINCT fix (сделано)
4. □ Оптимизация full_scan (projection push-down)
5. □ Index support для document_table
6. □ Parallel scan
7. □ Сделать document_table дефолтным

---

## Диаграмма текущей интеграции

```
services/
├── memory_storage/
│   └── memory_storage.cpp        ← создание коллекции (выбор storage type)
├── collection/
│   ├── collection.hpp            ← document_table_storage_wrapper_t
│   └── executor.cpp              ← выбор планера, выполнение операторов

components/
├── document_table/
│   ├── document_table_storage.hpp/cpp  ← основной storage класс
│   └── dynamic_schema.hpp/cpp          ← эволюция схемы
├── physical_plan_generator/
│   ├── create_plan.cpp                 ← document_table::planner::create_plan
│   └── impl/document_table/
│       ├── create_plan_insert.cpp      ← планирование INSERT
│       ├── create_plan_match.cpp       ← планирование SELECT
│       ├── create_plan_aggregate.cpp   ← планирование GROUP BY
│       ├── create_plan_delete.cpp      ← планирование DELETE
│       └── create_plan_update.cpp      ← планирование UPDATE
├── physical_plan/
│   └── document_table/operators/
│       ├── operator_insert.cpp         ← выполнение INSERT
│       ├── operator_delete.cpp         ← выполнение DELETE
│       ├── operator_update.cpp         ← выполнение UPDATE
│       ├── aggregation.cpp             ← выполнение GROUP BY
│       └── scan/
│           ├── full_scan.cpp           ← полное сканирование
│           └── primary_key_scan.cpp    ← поиск по _id
└── table/
    └── storage/
        ├── buffer_pool.cpp             ← управление памятью
        ├── block_manager.cpp           ← управление блоками
        └── in_memory_block_manager.hpp ← in-memory режим
```
