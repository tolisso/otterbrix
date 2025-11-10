# Архитектура Otterbrix

Этот документ описывает внутреннюю архитектуру базы данных Otterbrix - от высокоуровневых компонентов до деталей реализации.

## Содержание

- [Обзор архитектуры](#обзор-архитектуры)
- [Поток обработки запроса](#поток-обработки-запроса)
- [Слои системы](#слои-системы)
- [Хранение данных](#хранение-данных)
- [Исполнение запросов](#исполнение-запросов)
- [Управление памятью](#управление-памятью)
- [Управление параллелизмом](#управление-параллелизмом)
- [Оптимизации производительности](#оптимизации-производительности)

## Обзор архитектуры

Otterbrix построен на модульной архитектуре с четким разделением ответственности между слоями:

```
┌─────────────────────────────────────────────────────────────┐
│                    Client Layer                              │
│              (Python API, SQL Clients)                       │
└──────────────────────────┬──────────────────────────────────┘
                           │
┌──────────────────────────▼──────────────────────────────────┐
│                    Query Interface Layer                     │
│  ┌──────────────────┐           ┌──────────────────┐       │
│  │   SQL Parser     │           │  DataFrame API   │       │
│  │  (PostgreSQL)    │           │                  │       │
│  └────────┬─────────┘           └────────┬─────────┘       │
└───────────┼─────────────────────────────┼─────────────────┘
            │                              │
            └────────────┬─────────────────┘
┌────────────────────────▼─────────────────────────────────────┐
│                   Query Processing Layer                      │
│  ┌──────────────────────────────────────────────────┐        │
│  │  1. SQL → AST (Abstract Syntax Tree)             │        │
│  │  2. AST → Logical Plan (Transformer)             │        │
│  │  3. Logical Plan → Physical Plan (Optimizer)     │        │
│  └──────────────────────────────────────────────────┘        │
└────────────────────────┬─────────────────────────────────────┘
                         │
┌────────────────────────▼─────────────────────────────────────┐
│                  Execution Layer                              │
│  ┌─────────────────────────────────────────────────────┐     │
│  │        Vectorized Execution Engine                  │     │
│  │  ┌──────────┐ ┌──────────┐ ┌──────────┐           │     │
│  │  │   Scan   │ │  Filter  │ │   Join   │  ...      │     │
│  │  └──────────┘ └──────────┘ └──────────┘           │     │
│  │  Операторы обрабатывают данные батчами (vectors)   │     │
│  └─────────────────────────────────────────────────────┘     │
└────────────────────────┬─────────────────────────────────────┘
                         │
┌────────────────────────▼─────────────────────────────────────┐
│                   Storage Layer                               │
│  ┌──────────────────┐  ┌──────────────────┐                 │
│  │  Catalog Manager │  │   Table Manager  │                 │
│  │  (Metadata)      │  │   (Data Storage) │                 │
│  └──────────────────┘  └────────┬─────────┘                 │
│                                  │                            │
│  ┌──────────────────────────────▼─────────────────────────┐ │
│  │         Columnar Storage (Row Groups)                  │ │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐   │ │
│  │  │ Column Seg 1│  │ Column Seg 2│  │ Column Seg N│   │ │
│  │  │ (Compressed)│  │ (Compressed)│  │ (Compressed)│   │ │
│  │  └─────────────┘  └─────────────┘  └─────────────┘   │ │
│  └────────────────────────────────────────────────────────┘ │
│                                                               │
│  ┌────────────────────────────────────────────────────────┐ │
│  │  MVCC Layer (Row Version Manager, Update Segments)    │ │
│  └────────────────────────────────────────────────────────┘ │
└────────────────────────┬─────────────────────────────────────┘
                         │
┌────────────────────────▼─────────────────────────────────────┐
│              Infrastructure Layer                             │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │
│  │Block Manager │  │Buffer Manager│  │ Memory Pool  │      │
│  └──────────────┘  └──────────────┘  └──────────────┘      │
└───────────────────────────────────────────────────────────────┘
```

## Поток обработки запроса

Рассмотрим полный путь SQL запроса от текста до результата:

```
   SQL Text
      │
      ▼
┌──────────────┐
│ 1. Parser    │ → Лексический и синтаксический анализ
└──────┬───────┘   Результат: AST (Abstract Syntax Tree)
       │
       ▼
┌──────────────┐
│ 2. Transform │ → Семантический анализ, проверка типов
└──────┬───────┘   Результат: Logical Plan (логический план)
       │
       ▼
┌──────────────┐
│ 3. Planner   │ → Оптимизация запроса
└──────┬───────┘   Результат: Optimized Logical Plan
       │
       ▼
┌──────────────┐
│ 4. PhysPlan  │ → Генерация исполняемого плана
│   Generator  │   Результат: Physical Plan (операторы)
└──────┬───────┘
       │
       ▼
┌──────────────┐
│ 5. Execute   │ → Векторизованное исполнение
└──────┬───────┘   Pipeline execution с batches
       │
       ▼
   Result Set
```

### Пример: SELECT запрос

```sql
SELECT name, SUM(amount)
FROM orders
WHERE status = 'completed'
GROUP BY name
HAVING SUM(amount) > 1000
ORDER BY SUM(amount) DESC
LIMIT 10;
```

#### Этап 1: Parsing (SQL → AST)

```
SelectStmt
├── targetList: [name, SUM(amount)]
├── fromClause: [orders]
├── whereClause: status = 'completed'
├── groupClause: [name]
├── havingClause: SUM(amount) > 1000
├── sortClause: [SUM(amount) DESC]
└── limitCount: 10
```

**Файлы**: `components/sql/parser/`

#### Этап 2: Transform (AST → Logical Plan)

```
LogicalPlan
├── LogicalLimit (10)
│   └── LogicalSort (SUM(amount) DESC)
│       └── LogicalFilter (SUM(amount) > 1000)
│           └── LogicalAggregate (GROUP BY name, SUM(amount))
│               └── LogicalFilter (status = 'completed')
│                   └── LogicalScan (orders)
```

**Файлы**: `components/sql/transformer/`, `components/logical_plan/`

#### Этап 3: Optimize (Logical Plan Optimization)

Оптимизатор применяет правила:
- **Predicate pushdown**: фильтр `status = 'completed'` спускается к Scan
- **Projection pushdown**: читаем только колонки `name`, `amount`, `status`
- **Statistics-based optimization**: использование статистик для оценки кардинальности

**Файлы**: `components/planner/`

#### Этап 4: Physical Plan Generation

```
PhysicalPlan (Pipeline)
├── PhysicalLimit (10)
│   └── PhysicalSort (in-memory quicksort)
│       └── PhysicalFilter (SUM > 1000)
│           └── PhysicalHashAggregate (hash table)
│               └── PhysicalFilter (status = 'completed', vectorized)
│                   └── PhysicalTableScan (parallel, column pruning)
```

**Файлы**: `components/physical_plan_generator/`, `components/physical_plan/`

#### Этап 5: Execution (Vectorized)

Операторы исполняются в **pipeline** модели:
1. **TableScan** читает батчи (2048 строк) из колоночного хранилища
2. **Filter** применяет предикат векторизованно
3. **HashAggregate** строит hash table для группировки
4. **Filter** применяет HAVING условие
5. **Sort** сортирует результаты
6. **Limit** возвращает первые 10

**Ключевые оптимизации**:
- Vectorized execution (обработка батчами)
- SIMD операции для фильтров
- Late materialization (отложенное чтение колонок)
- Parallel scan row groups

**Файлы**: `components/physical_plan/`, `components/vector/`

## Слои системы

### 1. Query Interface Layer

#### SQL Parser
- **Основа**: PostgreSQL parser (форк)
- **Грамматика**: YACC/Bison
- **Лексер**: Flex
- **Поддержка**: SELECT, INSERT, UPDATE, DELETE, DDL
- **Файлы**: `components/sql/parser/`

```cpp
// Интерфейс парсера
class parser_t {
public:
    std::unique_ptr<stmt_t> parse(std::string_view sql);
};
```

#### Transformer
- Преобразует AST в логический план
- Семантический анализ
- Type checking и resolution
- **Файлы**: `components/sql/transformer/`

```cpp
class transformer_t {
public:
    std::unique_ptr<logical_operator_t> transform(stmt_t* ast);
};
```

### 2. Query Processing Layer

#### Logical Plan
Абстрактное представление запроса в виде дерева операторов:

**Основные операторы**:
- `LogicalScan` - чтение таблицы
- `LogicalFilter` - фильтрация (WHERE)
- `LogicalProjection` - проекция (SELECT list)
- `LogicalJoin` - соединения
- `LogicalAggregate` - агрегация (GROUP BY)
- `LogicalSort` - сортировка (ORDER BY)
- `LogicalLimit` - ограничение (LIMIT)

**Файлы**: `components/logical_plan/`

#### Planner / Optimizer
Оптимизирует логический план:

**Правила оптимизации**:
1. **Predicate Pushdown**: спуск фильтров ближе к источнику данных
2. **Projection Pushdown**: чтение только необходимых колонок
3. **Join Reordering**: оптимальный порядок join'ов
4. **Filter Reordering**: применение селективных фильтров первыми
5. **Common Subexpression Elimination**: устранение дублирующихся вычислений
6. **Constant Folding**: вычисление констант на этапе планирования

**Файлы**: `components/planner/`

#### Physical Plan Generator
Генерирует исполняемый физический план:

**Задачи**:
- Выбор алгоритмов (hash join vs merge join)
- Определение параллелизма
- Размещение операторов в pipelines
- Оценка стоимости операций

**Файлы**: `components/physical_plan_generator/`

### 3. Execution Layer

#### Vectorized Execution Engine
Ключевая особенность Otterbrix - векторизованная обработка данных.

**Принципы**:
- Данные обрабатываются батчами (vectors) ~2048 строк
- Минимизация overhead на строку
- Эффективное использование CPU cache
- Возможность SIMD оптимизаций

**Векторные типы** (`components/vector/`):
- `FLAT` - плоский массив
- `CONSTANT` - константный вектор
- `DICTIONARY` - словарное кодирование
- `SEQUENCE` - арифметическая прогрессия

```cpp
class vector_t {
    physical_type type_;
    vector_type vector_type_;
    std::shared_ptr<vector_buffer_t> buffer_;
    validity_mask_t validity_;  // NULL маска
};

class data_chunk_t {
    std::vector<vector_t> columns_;
    uint64_t cardinality_;  // число строк в батче
};
```

#### Physical Operators

**Категории операторов**:

1. **Source Operators** (источники данных):
   - `PhysicalTableScan` - сканирование таблицы
   - `PhysicalIndexScan` - сканирование индекса

2. **Filter Operators**:
   - `PhysicalFilter` - векторизованная фильтрация

3. **Projection Operators**:
   - `PhysicalProjection` - вычисление выражений

4. **Join Operators**:
   - `PhysicalHashJoin` - hash join
   - `PhysicalNestedLoopJoin` - nested loop join
   - `PhysicalMergeJoin` - merge join (для отсортированных данных)

5. **Aggregate Operators**:
   - `PhysicalHashAggregate` - hash-based группировка
   - `PhysicalStreamingAggregate` - для отсортированных данных

6. **Sort Operators**:
   - `PhysicalSort` - in-memory/external сортировка

7. **Set Operators**:
   - `PhysicalUnion`, `PhysicalIntersect`, `PhysicalExcept`

**Общий интерфейс**:
```cpp
class physical_operator_t {
public:
    virtual void init(execution_context_t& context) = 0;
    virtual void execute(data_chunk_t& input, data_chunk_t& output) = 0;
    virtual void finalize() = 0;
};
```

**Файлы**: `components/physical_plan/`

#### Pipeline Execution Model

Операторы организованы в **pipelines** для эффективного исполнения:

```
Pipeline 1:                  Pipeline 2:
┌─────────┐                  ┌─────────┐
│  Scan   │                  │  Scan   │
└────┬────┘                  └────┬────┘
     │ data_chunk                 │
┌────▼────┐                  ┌────▼────┐
│ Filter  │                  │ Filter  │
└────┬────┘                  └────┬────┘
     │                            │
     └──────────┬─────────────────┘
                │
         ┌──────▼──────┐
         │  Hash Join  │  ← Pipeline breaker
         └──────┬──────┘
                │
         ┌──────▼──────┐
         │  Aggregate  │  ← Pipeline breaker
         └──────┬──────┘
                │
         ┌──────▼──────┐
         │    Sort     │  ← Pipeline breaker
         └──────┬──────┘
                │
            Result Set
```

**Pipeline breakers** (операторы, требующие материализации):
- Joins (строят hash table)
- Aggregates (строят hash table)
- Sorts (сортируют данные)
- Window functions

## Хранение данных

### Columnar Storage Architecture

Otterbrix использует колоночное хранилище с многоуровневой организацией:

```
Database
 └── Table (data_table_t)
      └── Collection (collection_t)
           └── Row Groups (row_group_t) [~122,880 строк]
                └── Column Data (column_data_t)
                     └── Column Segments (column_segment_t) [сжатые блоки]
```

#### Иерархия хранения

**1. Table** (`data_table_t`)
- Главная структура для хранения табличных данных
- Управляет row groups
- Обеспечивает CRUD операции
- **Файлы**: `components/table/data_table.hpp/cpp`

**2. Collection** (`collection_t`)
- Менеджер коллекции row groups
- Организация данных в блоки
- Параллельное сканирование
- **Файлы**: `components/table/collection.hpp/cpp`

**3. Row Group** (`row_group_t`)
- Фиксированный размер: 122,880 строк (по умолчанию)
- Колоночная организация внутри группы
- Min/max статистики для каждой колонки
- Поддержка zone maps для пропуска блоков
- **Файлы**: `components/table/row_group.hpp/cpp`

**4. Column Data** (`column_data_t`)
- Базовое хранилище для одной колонки
- Специализации для разных типов
- **Файлы**: `components/table/column_data.hpp/cpp`

**Типы колонок**:
- `standard_column_data_t` - примитивные типы
- `struct_column_data_t` - вложенные структуры
- `array_column_data_t` - массивы фиксированной длины
- `list_column_data_t` - списки переменной длины
- `validity_column_data_t` - NULL маски

**5. Column Segment** (`column_segment_t`)
- Сжатые сегменты данных
- Lazy loading (загрузка по требованию)
- Компрессия: dictionary, RLE, bit-packing
- **Файлы**: `components/table/column_segment.hpp/cpp`

### Компрессия данных

#### Dictionary Compression

Для строковых колонок и low-cardinality данных:

```
Row Data:          Dictionary:        Index Array:
"apple"       →    0: "apple"    →    [0, 1, 0, 2, 1]
"banana"           1: "banana"
"apple"            2: "cherry"
"cherry"
"banana"
```

**Структура**:
```cpp
struct dictionary_compression_header_t {
    uint32_t dict_size;
    uint32_t dict_end;
    uint32_t index_buffer_offset;
    uint32_t index_buffer_count;
    uint32_t bitpacking_width;  // биты на индекс
};
```

**Преимущества**:
- Значительная экономия для повторяющихся строк
- Быстрое сравнение (сравниваем индексы)
- Эффективная фильтрация

**Файлы**: `components/table/column_segment.cpp:32-73`

#### String Storage

**Малые строки** (< 4KB):
- Хранятся непосредственно в dictionary section
- Положительные смещения в index array

**Большие строки** (≥ 4KB):
- Хранятся в отдельных overflow blocks
- Отрицательные смещения в index array (marker)
- 8-byte marker: `block_id (4 bytes) + offset (4 bytes)`

```cpp
struct string_block_t {
    std::shared_ptr<storage::block_handle_t> block;
    uint64_t offset;
    uint64_t size;
    std::unique_ptr<string_block_t> next;  // linked list
};
```

**Файлы**: `components/table/column_state.hpp:204-241`

### MVCC (Multi-Version Concurrency Control)

Otterbrix использует MVCC для изоляции транзакций без блокировок на чтение.

#### Row Version Manager

Управление версиями строк:

```cpp
class row_version_manager_t {
    // Версии строк с transaction IDs
    std::vector<row_version_t> versions_;

    // Garbage collection для старых версий
    void cleanup(transaction_id_t oldest_active);
};

struct row_version_t {
    transaction_id_t txn_id;    // ID транзакции
    row_id_t row_id;             // ID строки
    operation_type op_type;      // INSERT/UPDATE/DELETE
    std::vector<value_t> data;   // Данные версии
};
```

**Файлы**: `components/table/row_version_manager.hpp/cpp`

#### Update Segments

Хранение обновлений:

```cpp
class update_segment_t {
    std::vector<row_id_t> row_ids_;        // Обновленные строки
    std::vector<column_id_t> column_ids_;  // Обновленные колонки
    std::vector<value_t> updates_;         // Новые значения
    core::string_heap_t heap_;             // Для строк
};
```

**Принципы**:
- Update segments хранят дельты изменений
- Периодическое слияние с основными данными
- Эффективное хранение обновлений

**Файлы**: `components/table/update_segment.hpp/cpp`

#### Transaction Isolation

Otterbrix поддерживает **Snapshot Isolation**:

1. Каждая транзакция получает `transaction_id`
2. Транзакция видит снимок БД на момент начала
3. Версии строк фильтруются по `transaction_id`
4. Write-write conflicts обнаруживаются при commit

### Индексы

#### B+ Tree Index

Основная индексная структура:

```cpp
class b_plus_tree_index_t {
    // Root узел дерева
    std::unique_ptr<node_t> root_;

    // Операции
    bool insert(key_t key, row_id_t row_id);
    bool remove(key_t key);
    std::vector<row_id_t> lookup(key_t key);
    std::vector<row_id_t> range_scan(key_t min, key_t max);
};
```

**Особенности**:
- Поддержка range queries
- Упорядоченное сканирование
- Concurrent access с latches
- Efficient point lookups

**Файлы**: `core/b_plus_tree/`, `components/index/`

#### Index Selection

Оптимизатор выбирает индекс на основе:
- Selectivity фильтра
- Cardinality индекса
- Тип операции (point lookup vs range scan)
- Cost model

## Управление памятью

### PMR (Polymorphic Memory Resources)

Otterbrix использует C++17 PMR для эффективного управления памятью:

```cpp
// String heap с arena allocator
class string_heap_t {
    std::pmr::monotonic_buffer_resource arena_allocator_;

public:
    void* insert(const void* data, size_t size);
    void reset();  // Быстрое освобождение всей памяти
};
```

**Преимущества**:
- Быстрая аллокация (O(1) в arena)
- Bulk deallocation
- Уменьшение фрагментации
- Cache-friendly размещение

**Файлы**: `core/string_heap/`, `core/pmr.hpp`

### Buffer Manager

Управление буферами страниц:

```cpp
class buffer_manager_t {
    // Buffer pool
    std::vector<buffer_frame_t> frames_;

    // Page eviction (LRU, Clock, etc.)
    eviction_policy_t policy_;

public:
    buffer_handle_t pin(page_id_t page_id);
    void unpin(buffer_handle_t handle);
    void flush_page(page_id_t page_id);
};
```

**Политики вытеснения**:
- LRU (Least Recently Used)
- Clock algorithm
- 2Q (two-queue)

### Memory Pools

Специализированные пулы для различных типов объектов:
- Vector buffers
- String heaps
- Index nodes
- Transaction metadata

## Управление параллелизмом

### Actor Model

Otterbrix использует **actor-zeta** framework для асинхронной обработки:

```cpp
class database_actor_t : public actor_zeta::actor::abstract_actor {
public:
    void handle_query(query_request_t request);
    void handle_update(update_request_t request);
};
```

**Преимущества**:
- Message-passing вместо shared state
- Natural parallelism
- Fault isolation

### Parallel Query Execution

#### Parallel Table Scan

Row groups сканируются параллельно:

```cpp
void parallel_scan(table_t& table, predicate_t predicate) {
    auto row_groups = table.get_row_groups();

    // Параллельное сканирование
    #pragma omp parallel for
    for (size_t i = 0; i < row_groups.size(); ++i) {
        auto chunk = scan_row_group(row_groups[i], predicate);
        // Обработка chunk
    }
}
```

#### Parallel Aggregation

**Фаза 1**: Локальная агрегация в каждом потоке
**Фаза 2**: Слияние локальных hash tables

```
Thread 1: hash_table_1
Thread 2: hash_table_2    → Merge → Final hash_table
Thread 3: hash_table_3
```

### Locking Strategies

**Read operations**: Lock-free благодаря MVCC

**Write operations**:
- Row-level locks для UPDATE/DELETE
- Table-level locks для DDL
- Intent locks для иерархических структур

## Оптимизации производительности

### 1. Vectorized Processing

**Batch processing**: ~2048 строк за раз
- Минимизация overhead на строку
- Amortized cost проверок
- Лучшая утилизация CPU cache

**SIMD**: Использование векторных инструкций CPU
- Параллельная обработка 4-16 элементов
- Векторизованные операции сравнения
- Векторизованные арифметические операции

### 2. Late Materialization

Откладываем чтение колонок до последнего момента:

```
1. Scan только колонки для фильтра (status)
2. Применяем фильтр → selection vector
3. Читаем остальные колонки (name, amount) только для прошедших фильтр строк
```

**Экономия**: меньше данных читается и передается между операторами

### 3. Zone Maps / Statistics

Каждый column segment хранит статистики:
- Min/Max значения
- Null count
- Distinct count estimate

**Использование**:
- Skip segments, где min > filter_value
- Skip segments, где max < filter_value
- Оценка selectivity для оптимизатора

### 4. Dictionary Encoding

Low-cardinality колонки кодируются словарем:
- **Экономия памяти**: 4-byte индекс вместо полной строки
- **Быстрое сравнение**: сравниваем индексы
- **Эффективная фильтрация**: создаем bitmap разрешенных индексов

### 5. Adaptive Execution

Исполнитель адаптируется к данным:
- Выбор hash join vs merge join
- Выбор in-memory vs external sort
- Выбор hash aggregate vs streaming aggregate

### 6. Cache-Conscious Algorithms

- Колоночный layout: последовательный доступ к памяти
- Blocking для операций: обработка кеш-размерных блоков
- Prefetching: предвыборка данных

### 7. Compression

Встроенная компрессия:
- Dictionary compression
- Run-Length Encoding (RLE)
- Bit-packing
- Delta encoding

**Trade-off**: CPU time для декомпрессии vs меньше I/O

## Расширяемость

### Custom Types

Добавление новых типов данных:

```cpp
class custom_type_t : public logical_type_t {
public:
    size_t get_size() const override;
    bool equals(const value_t& other) const override;
    std::string to_string() const override;
};
```

### Custom Functions

Регистрация UDF (User-Defined Functions):

```cpp
catalog.register_scalar_function(
    "my_func",
    {LogicalType::INTEGER},  // arg types
    LogicalType::INTEGER,     // return type
    &my_func_impl
);
```

### Custom Operators

Добавление новых физических операторов:

```cpp
class custom_physical_operator_t : public physical_operator_t {
public:
    void execute(data_chunk_t& input, data_chunk_t& output) override {
        // Ваша логика
    }
};
```

## Диагностика и мониторинг

### Query Profiling

```cpp
class query_profiler_t {
    std::vector<operator_stats_t> stats_;

    struct operator_stats_t {
        std::string name;
        uint64_t execution_time_us;
        uint64_t tuples_processed;
        uint64_t memory_usage;
    };
};
```

### Logging

Структурированное логирование с `spdlog`:

```cpp
log::info("Executing query",
    "query_id", query_id,
    "table", table_name,
    "rows_scanned", rows_scanned
);
```

## Заключение

Архитектура Otterbrix спроектирована для:
- **Производительности**: векторизация, параллелизм, оптимизации
- **Гибкости**: schema-free, полуструктурированные данные
- **Масштабируемости**: колоночное хранилище, эффективная компрессия
- **Расширяемости**: модульный дизайн, plugin system

Ключевые архитектурные решения:
1. **Vectorized execution** для эффективной обработки
2. **Columnar storage** для аналитических рабочих нагрузок
3. **MVCC** для изоляции без блокировок на чтение
4. **Hybrid memory layout** для балансировки flexibility и performance
5. **Cost-based optimization** для эффективных планов запросов

Для более детальной информации о конкретных компонентах см. [COMPONENTS.md](COMPONENTS.md).
