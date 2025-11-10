# Компоненты Otterbrix

Детальное описание всех компонентов системы Otterbrix.

## Содержание

- [Обзор компонентов](#обзор-компонентов)
- [SQL Layer](#sql-layer)
- [Query Processing](#query-processing)
- [Execution Engine](#execution-engine)
- [Storage Layer](#storage-layer)
- [Index Structures](#index-structures)
- [Core Infrastructure](#core-infrastructure)
- [Utilities](#utilities)

---

## Обзор компонентов

Otterbrix организован в модульную архитектуру со следующими основными категориями:

```
components/
├── sql/                    # SQL парсинг и трансформация
├── logical_plan/           # Логические планы запросов
├── physical_plan/          # Физические операторы
├── physical_plan_generator/# Генерация физических планов
├── planner/               # Оптимизация запросов
├── expressions/           # Выражения и функции
├── vector/                # Векторизованная обработка
├── table/                 # Колоночное хранилище
├── index/                 # Индексные структуры
├── catalog/               # Метаданные (schemas, tables)
├── session/               # Управление сессиями
├── context/               # Контекст выполнения
├── cursor/                # Курсоры для итерации
├── document/              # Документное хранилище
├── types/                 # Система типов
├── oid/                   # Object identifiers
├── base/                  # Базовые структуры
├── configuration/         # Конфигурация
├── log/                   # Логирование
├── serialization/         # Сериализация данных
└── tests/                 # Тесты компонентов
```

---

## SQL Layer

### components/sql/

SQL слой отвечает за обработку SQL запросов от текста до логического плана.

#### Структура

```
sql/
├── parser/              # PostgreSQL-based SQL парсер
│   ├── gram.hpp/cpp    # Грамматика (YACC/Bison)
│   ├── scan.cpp        # Лексический анализатор (Flex)
│   ├── parser.cpp      # Основной парсер
│   ├── keyword.cpp     # SQL ключевые слова
│   └── nodes/          # AST узлы
└── transformer/         # AST → Logical Plan
    ├── transformer.hpp/cpp
    ├── utils.hpp/cpp
    └── impl/           # Реализации трансформаций
        ├── transform_select.cpp
        ├── transform_insert.cpp
        ├── transform_update.cpp
        ├── transform_delete.cpp
        ├── transform_table.cpp
        ├── transform_database.cpp
        └── transform_index.cpp
```

#### Parser

**Назначение**: Преобразование SQL текста в Abstract Syntax Tree (AST)

**Основа**: Форк PostgreSQL parser для совместимости

**Процесс**:
1. **Лексический анализ** (`scan.cpp`): текст → токены
2. **Синтаксический анализ** (`gram.cpp`): токены → AST
3. **Построение узлов** (`nodes/`): создание структур данных AST

**Поддерживаемые SQL конструкции**:
- `SELECT` с `WHERE`, `JOIN`, `GROUP BY`, `HAVING`, `ORDER BY`, `LIMIT`
- `INSERT` с `VALUES` или подзапросами
- `UPDATE` с `SET` и `WHERE`
- `DELETE` с `WHERE`
- `CREATE/DROP DATABASE`
- `CREATE/DROP TABLE` (с типами колонок, constraints)
- `CREATE/DROP INDEX`
- Выражения: арифметические, логические, сравнения, CASE
- Функции: агрегатные (`SUM`, `AVG`, `COUNT`, `MIN`, `MAX`), скалярные
- Подзапросы в `FROM`, `WHERE`, `SELECT`

**Пример AST**:
```cpp
// SQL: SELECT name, age FROM users WHERE age > 18
SelectStmt {
    targetList: [
        ResTarget { name: "name" },
        ResTarget { name: "age" }
    ],
    fromClause: [
        RangeVar { relname: "users" }
    ],
    whereClause: A_Expr {
        kind: AEXPR_OP,
        name: ">",
        lexpr: ColumnRef { fields: ["age"] },
        rexpr: A_Const { val: 18 }
    }
}
```

**Ключевые классы**:
```cpp
// Базовый узел AST
struct Node {
    NodeTag type;
};

// SELECT statement
struct SelectStmt : Node {
    List* targetList;      // SELECT list
    List* fromClause;      // FROM clause
    Node* whereClause;     // WHERE clause
    List* groupClause;     // GROUP BY
    Node* havingClause;    // HAVING
    List* sortClause;      // ORDER BY
    Node* limitOffset;     // OFFSET
    Node* limitCount;      // LIMIT
};

// Колонка или выражение в SELECT
struct ResTarget : Node {
    Node* val;            // Выражение
    char* name;           // Алиас
};
```

**Файлы**:
- `parser.cpp:` - Главный интерфейс парсера
- `gram.cpp` - Грамматика (генерируется из `gram.y`)
- `scan.cpp` - Лексер (генерируется из `scan.l`)

#### Transformer

**Назначение**: Преобразование AST в логический план

**Процесс**:
1. Обход AST
2. Семантический анализ (проверка существования таблиц, колонок)
3. Type resolution (определение типов выражений)
4. Построение дерева логических операторов

**Основные трансформации**:

**SELECT → Logical Plan**:
```
SELECT name, age FROM users WHERE age > 18 ORDER BY age LIMIT 10

↓ Transform

LogicalPlan:
  LogicalLimit(10)
    └─ LogicalSort(age ASC)
        └─ LogicalProjection(name, age)
            └─ LogicalFilter(age > 18)
                └─ LogicalScan(users)
```

**JOIN → Logical Plan**:
```
SELECT u.name, o.amount
FROM users u
JOIN orders o ON u.id = o.user_id
WHERE o.amount > 100

↓ Transform

LogicalPlan:
  LogicalProjection(u.name, o.amount)
    └─ LogicalFilter(o.amount > 100)
        └─ LogicalJoin(INNER, u.id = o.user_id)
            ├─ LogicalScan(users AS u)
            └─ LogicalScan(orders AS o)
```

**Ключевые классы**:
```cpp
class transformer_t {
public:
    // Главная функция трансформации
    std::unique_ptr<logical_operator_t>
    transform(Node* ast_node);

private:
    // Специализированные трансформеры
    std::unique_ptr<logical_operator_t>
    transform_select(SelectStmt* select);

    std::unique_ptr<logical_operator_t>
    transform_insert(InsertStmt* insert);

    std::unique_ptr<logical_operator_t>
    transform_update(UpdateStmt* update);

    std::unique_ptr<logical_operator_t>
    transform_delete(DeleteStmt* delete_stmt);

    // Трансформация выражений
    std::unique_ptr<expression_t>
    transform_expression(Node* expr);

    // Контекст трансформации
    catalog_t& catalog_;
    session_t& session_;
};
```

**Файлы**:
- `transformer.hpp/cpp` - Главный класс
- `impl/transform_select.cpp` - SELECT запросы
- `impl/transform_insert.cpp` - INSERT операции
- `impl/transform_update.cpp` - UPDATE операции
- `impl/transform_delete.cpp` - DELETE операции
- `impl/transform_table.cpp` - DDL для таблиц
- `impl/transform_database.cpp` - DDL для БД
- `impl/transform_index.cpp` - DDL для индексов

---

## Query Processing

### components/logical_plan/

**Назначение**: Представление запроса в виде дерева логических операторов

Логический план - это абстрактное, независимое от реализации представление запроса. Он описывает **что** нужно сделать, но не **как**.

#### Логические операторы

**Базовый класс**:
```cpp
class logical_operator_t {
public:
    virtual ~logical_operator_t() = default;

    // Тип оператора
    virtual logical_operator_type type() const = 0;

    // Дочерние операторы
    std::vector<std::unique_ptr<logical_operator_t>> children;

    // Выражения (проекции, фильтры, и т.д.)
    std::vector<std::unique_ptr<expression_t>> expressions;
};
```

**Типы операторов**:

1. **LogicalScan** - Сканирование таблицы
```cpp
class logical_scan_t : public logical_operator_t {
    std::string table_name;
    std::vector<std::string> column_names;  // Проекция колонок
    // Опционально: index для index scan
};
```

2. **LogicalFilter** - Фильтрация (WHERE, HAVING)
```cpp
class logical_filter_t : public logical_operator_t {
    std::unique_ptr<expression_t> predicate;  // Условие фильтрации
};
```

3. **LogicalProjection** - Проекция (SELECT list)
```cpp
class logical_projection_t : public logical_operator_t {
    std::vector<std::unique_ptr<expression_t>> projections;
    std::vector<std::string> aliases;
};
```

4. **LogicalJoin** - Соединение таблиц
```cpp
enum class join_type {
    INNER,
    LEFT,
    RIGHT,
    FULL,
    CROSS
};

class logical_join_t : public logical_operator_t {
    join_type type;
    std::unique_ptr<expression_t> condition;  // ON clause
};
```

5. **LogicalAggregate** - Агрегация (GROUP BY)
```cpp
class logical_aggregate_t : public logical_operator_t {
    std::vector<std::unique_ptr<expression_t>> groups;      // GROUP BY
    std::vector<std::unique_ptr<expression_t>> aggregates;  // SUM, AVG, etc.
};
```

6. **LogicalSort** - Сортировка (ORDER BY)
```cpp
enum class sort_order {
    ASCENDING,
    DESCENDING
};

class logical_sort_t : public logical_operator_t {
    std::vector<std::unique_ptr<expression_t>> sort_keys;
    std::vector<sort_order> orders;
};
```

7. **LogicalLimit** - Ограничение результатов
```cpp
class logical_limit_t : public logical_operator_t {
    uint64_t limit;
    uint64_t offset;
};
```

8. **LogicalInsert** - Вставка данных
```cpp
class logical_insert_t : public logical_operator_t {
    std::string table_name;
    std::vector<std::string> column_names;
    std::vector<std::vector<value_t>> values;  // Для VALUES clause
    // Или child operator для INSERT INTO ... SELECT
};
```

9. **LogicalUpdate** - Обновление данных
```cpp
class logical_update_t : public logical_operator_t {
    std::string table_name;
    std::vector<std::string> column_names;
    std::vector<std::unique_ptr<expression_t>> expressions;
    std::unique_ptr<expression_t> predicate;  // WHERE clause
};
```

10. **LogicalDelete** - Удаление данных
```cpp
class logical_delete_t : public logical_operator_t {
    std::string table_name;
    std::unique_ptr<expression_t> predicate;  // WHERE clause
};
```

**Файлы**: `components/logical_plan/`

### components/planner/

**Назначение**: Оптимизация логического плана

Планировщик применяет набор правил оптимизации для улучшения производительности запроса.

#### Правила оптимизации

**1. Predicate Pushdown**

Спускаем фильтры ближе к источникам данных для ранней фильтрации:

```
До оптимизации:
  Filter(age > 18)
    └─ Join(users, orders)
        ├─ Scan(users)
        └─ Scan(orders)

После оптимизации:
  Join(users, orders)
    ├─ Filter(age > 18)
    │   └─ Scan(users)
    └─ Scan(orders)
```

**2. Projection Pushdown (Column Pruning)**

Читаем только необходимые колонки:

```
До оптимизации:
  Projection(name, age)
    └─ Scan(users)  ← читает все колонки

После оптимизации:
  Scan(users, columns=[name, age])  ← читает только name, age
```

**3. Join Reordering**

Изменяем порядок join'ов для минимизации промежуточных результатов:

```
До оптимизации:
  Join(A, B) [1M rows]
    └─ Join(result, C)

После оптимизации (если A Join C меньше):
  Join(A, C) [100K rows]
    └─ Join(result, B)
```

**4. Filter Reordering**

Применяем более селективные фильтры первыми:

```
До оптимизации:
  Filter(city = 'NY')        ← 10% selectivity
    └─ Filter(age > 18)      ← 80% selectivity

После оптимизации:
  Filter(age > 18)           ← применяем первым
    └─ Filter(city = 'NY')
```

**5. Common Subexpression Elimination**

Устраняем дублирующиеся вычисления:

```
SELECT price * 1.1, price * 1.1 * 0.05 FROM products

До оптимизации:
  - Вычисляем price * 1.1 дважды

После оптимизации:
  - temp = price * 1.1
  - Используем temp в обоих выражениях
```

**6. Constant Folding**

Вычисляем константные выражения на этапе планирования:

```
SELECT * FROM users WHERE age > 10 + 8

↓ Constant Folding

SELECT * FROM users WHERE age > 18
```

**Ключевые классы**:
```cpp
class planner_t {
public:
    // Оптимизация логического плана
    std::unique_ptr<logical_operator_t>
    optimize(std::unique_ptr<logical_operator_t> plan);

private:
    // Правила оптимизации
    void apply_predicate_pushdown(logical_operator_t* op);
    void apply_projection_pushdown(logical_operator_t* op);
    void apply_join_reordering(logical_operator_t* op);
    void apply_filter_reordering(logical_operator_t* op);
    void apply_cse(logical_operator_t* op);

    // Оценка стоимости
    double estimate_cost(const logical_operator_t* op);

    // Статистики для оптимизации
    catalog_t& catalog_;
};
```

**Файлы**: `components/planner/`

### components/physical_plan_generator/

**Назначение**: Генерация исполняемого физического плана из оптимизированного логического плана

Физический план определяет **как** именно будет выполнен запрос: выбор алгоритмов, параллелизм, размещение в pipelines.

#### Генерация физического плана

**Процесс**:
1. Обход оптимизированного логического плана
2. Для каждого логического оператора выбор физической реализации
3. Оценка стоимости альтернативных реализаций
4. Генерация физических операторов
5. Организация операторов в pipelines

**Пример выбора алгоритмов**:

**Join**:
- `Hash Join` - для больших таблиц без индексов
- `Merge Join` - для отсортированных данных
- `Nested Loop Join` - для маленьких таблиц или когда есть индекс

**Aggregate**:
- `Hash Aggregate` - для неотсортированных данных
- `Streaming Aggregate` - для отсортированных данных (более эффективно)

**Sort**:
- `In-Memory Sort` - если данные помещаются в память
- `External Sort` - для больших датасетов (merge sort с дисковыми блоками)

**Ключевые классы**:
```cpp
class physical_plan_generator_t {
public:
    // Генерация физического плана
    std::unique_ptr<physical_operator_t>
    generate(const logical_operator_t* logical_plan);

private:
    // Генераторы для конкретных операторов
    std::unique_ptr<physical_operator_t>
    generate_scan(const logical_scan_t* scan);

    std::unique_ptr<physical_operator_t>
    generate_join(const logical_join_t* join);

    std::unique_ptr<physical_operator_t>
    generate_aggregate(const logical_aggregate_t* agg);

    // Cost estimation
    double estimate_hash_join_cost(const logical_join_t* join);
    double estimate_merge_join_cost(const logical_join_t* join);

    // Статистики
    catalog_t& catalog_;
};
```

**Файлы**: `components/physical_plan_generator/`

---

## Execution Engine

### components/physical_plan/

**Назначение**: Исполняемые операторы для обработки данных

Физические операторы - это конкретные реализации алгоритмов обработки данных, работающие с векторами.

#### Базовый интерфейс

```cpp
class physical_operator_t {
public:
    virtual ~physical_operator_t() = default;

    // Инициализация оператора
    virtual void init(execution_context_t& context) = 0;

    // Выполнение: получает input chunk, производит output chunk
    virtual void execute(data_chunk_t& input, data_chunk_t& output) = 0;

    // Финализация (cleanup)
    virtual void finalize() = 0;

    // Получение следующего chunk (для source operators)
    virtual bool get_next(execution_context_t& context, data_chunk_t& output) {
        return false;
    }

protected:
    std::vector<std::unique_ptr<physical_operator_t>> children_;
};
```

#### Физические операторы

**1. PhysicalTableScan**

Сканирование таблицы с поддержкой параллелизма:

```cpp
class physical_table_scan_t : public physical_operator_t {
public:
    void init(execution_context_t& context) override;

    bool get_next(execution_context_t& context,
                  data_chunk_t& output) override;

private:
    data_table_t* table_;
    std::vector<column_id_t> column_ids_;  // Колонки для чтения
    std::unique_ptr<table_scan_state_t> scan_state_;

    // Параллельное сканирование row groups
    size_t current_row_group_;
};
```

**Особенности**:
- Parallel scan row groups
- Column pruning (читаем только нужные колонки)
- Zone map filtering (пропуск row groups по статистикам)
- Vectorized output (батчи ~2048 строк)

**2. PhysicalFilter**

Векторизованная фильтрация:

```cpp
class physical_filter_t : public physical_operator_t {
public:
    void execute(data_chunk_t& input, data_chunk_t& output) override;

private:
    std::unique_ptr<expression_t> predicate_;

    // Selection vector для избежания копирования
    selection_vector_t selection_;
};
```

**Процесс**:
1. Вычисляем predicate для всего batch → boolean vector
2. Создаем selection vector с индексами true значений
3. Используем selection vector для последующих операций (late materialization)

**3. PhysicalHashJoin**

Hash join для больших таблиц:

```cpp
class physical_hash_join_t : public physical_operator_t {
public:
    void init(execution_context_t& context) override;
    bool get_next(execution_context_t& context, data_chunk_t& output) override;

private:
    join_type type_;  // INNER, LEFT, RIGHT, FULL
    std::unique_ptr<expression_t> condition_;

    // Build phase: строим hash table из правой таблицы
    std::unordered_map<hash_t, std::vector<row_id_t>> hash_table_;

    // Probe phase: ищем matches из левой таблицы
    bool build_done_ = false;
};
```

**Фазы**:
1. **Build**: Читаем правую таблицу, строим hash table
2. **Probe**: Читаем левую таблицу, ищем matches в hash table
3. **Output**: Генерируем joined rows

**4. PhysicalHashAggregate**

Hash-based группировка:

```cpp
class physical_hash_aggregate_t : public physical_operator_t {
public:
    void init(execution_context_t& context) override;
    bool get_next(execution_context_t& context, data_chunk_t& output) override;

private:
    std::vector<std::unique_ptr<expression_t>> groups_;
    std::vector<std::unique_ptr<aggregate_expression_t>> aggregates_;

    // Hash table: group_key → aggregate_state
    std::unordered_map<hash_t, aggregate_state_t> hash_table_;

    bool finalized_ = false;
};
```

**Процесс**:
1. Для каждого input batch:
   - Вычисляем group keys
   - Обновляем агрегаты в hash table
2. Финализация: генерируем output из hash table

**Агрегатные функции**:
- `COUNT`: счетчик
- `SUM`: аккумулятор
- `AVG`: сумма + count, возврат sum/count
- `MIN/MAX`: текущий минимум/максимум
- `STDDEV`, `VARIANCE`: сложные агрегаты

**5. PhysicalSort**

Сортировка с поддержкой external sort:

```cpp
class physical_sort_t : public physical_operator_t {
public:
    void init(execution_context_t& context) override;
    bool get_next(execution_context_t& context, data_chunk_t& output) override;

private:
    std::vector<std::unique_ptr<expression_t>> sort_keys_;
    std::vector<sort_order> orders_;

    // In-memory sort
    std::vector<data_chunk_t> buffered_chunks_;

    // External sort (если не помещается в память)
    std::vector<std::string> temporary_files_;
    bool sorted_ = false;
};
```

**Алгоритмы**:
- **In-memory**: quicksort или radix sort
- **External**: merge sort с временными файлами

**6. PhysicalLimit**

Ограничение результатов:

```cpp
class physical_limit_t : public physical_operator_t {
public:
    bool get_next(execution_context_t& context, data_chunk_t& output) override;

private:
    uint64_t limit_;
    uint64_t offset_;
    uint64_t current_offset_ = 0;
};
```

**7. PhysicalProjection**

Вычисление выражений и проекция колонок:

```cpp
class physical_projection_t : public physical_operator_t {
public:
    void execute(data_chunk_t& input, data_chunk_t& output) override;

private:
    std::vector<std::unique_ptr<expression_t>> expressions_;
};
```

**Процесс**: Для каждого expression вычисляем результат векторизованно

**Файлы**: `components/physical_plan/`

### components/vector/

**Назначение**: Векторизованная обработка данных

Vector component реализует колоночные векторы и операции над ними для эффективной batch-обработки.

#### Vector

Основная структура данных:

```cpp
enum class vector_type {
    FLAT,       // Плоский массив
    CONSTANT,   // Константный вектор
    DICTIONARY, // Словарное кодирование
    SEQUENCE    // Арифметическая прогрессия
};

class vector_t {
public:
    vector_t(logical_type type, size_t capacity);

    // Тип данных
    logical_type get_type() const { return type_; }

    // Физическое представление
    vector_type get_vector_type() const { return vector_type_; }

    // Данные
    void* get_data() { return buffer_->get_data(); }

    // NULL маска
    validity_mask_t& get_validity() { return validity_; }

    // Размер
    size_t size() const { return size_; }

private:
    logical_type type_;
    vector_type vector_type_;
    std::shared_ptr<vector_buffer_t> buffer_;
    validity_mask_t validity_;
    size_t size_;
};
```

#### Vector Types

**1. FLAT Vector**

Стандартное плоское представление:

```cpp
// Пример: vector<int32_t>
int32_t* data = vector.get_data<int32_t>();
for (size_t i = 0; i < vector.size(); ++i) {
    if (vector.get_validity().is_valid(i)) {
        process(data[i]);
    }
}
```

**2. CONSTANT Vector**

Все элементы имеют одно значение:

```cpp
// Пример: вектор с константой 42
// Хранит только одно значение, экономит память
constant_vector_t vec(42, 1000);  // 1000 элементов, все = 42
```

**3. DICTIONARY Vector**

Словарное кодирование для low-cardinality данных:

```cpp
// Пример: ["apple", "banana", "apple", "cherry", "banana"]
// Dictionary: {0: "apple", 1: "banana", 2: "cherry"}
// Indices: [0, 1, 0, 2, 1]

dictionary_vector_t vec;
vec.dictionary_ = ["apple", "banana", "cherry"];
vec.indices_ = [0, 1, 0, 2, 1];
```

**4. SEQUENCE Vector**

Арифметическая прогрессия:

```cpp
// Пример: [10, 20, 30, 40, 50]
// Хранит только: start=10, increment=10, count=5

sequence_vector_t vec(10, 10, 5);
// vec[i] = start + i * increment
```

#### Data Chunk

Batch строк с несколькими колонками:

```cpp
class data_chunk_t {
public:
    data_chunk_t();

    // Добавление колонки
    void add_column(vector_t column);

    // Доступ к колонке
    vector_t& get_column(size_t index);

    // Число строк в chunk
    size_t cardinality() const { return cardinality_; }
    void set_cardinality(size_t count) { cardinality_ = count; }

    // Число колонок
    size_t column_count() const { return columns_.size(); }

    // Reset для переиспользования
    void reset();

private:
    std::vector<vector_t> columns_;
    size_t cardinality_;  // Обычно до 2048
};
```

**Использование**:
```cpp
// Создание chunk
data_chunk_t chunk;
chunk.add_column(vector_t(logical_type::INTEGER, 2048));
chunk.add_column(vector_t(logical_type::VARCHAR, 2048));
chunk.set_cardinality(1000);  // 1000 строк

// Чтение данных
auto& col0 = chunk.get_column(0);
int32_t* data = col0.get_data<int32_t>();
for (size_t i = 0; i < chunk.cardinality(); ++i) {
    std::cout << data[i] << std::endl;
}
```

#### Validity Mask

Битовая маска для NULL значений:

```cpp
class validity_mask_t {
public:
    // Проверка NULL
    bool is_valid(size_t index) const;
    bool is_null(size_t index) const { return !is_valid(index); }

    // Установка NULL
    void set_invalid(size_t index);
    void set_valid(size_t index);

    // Все NULL или все valid
    void set_all_invalid(size_t count);
    void set_all_valid(size_t count);

private:
    std::vector<uint64_t> bits_;  // Битовый массив
};
```

**Эффективность**: 1 bit на значение (64 значения в одном uint64_t)

#### Vector Operations

Векторизованные операции над векторами:

```cpp
namespace vector_operations {

// Арифметические операции
void add(vector_t& result, const vector_t& left, const vector_t& right);
void subtract(vector_t& result, const vector_t& left, const vector_t& right);
void multiply(vector_t& result, const vector_t& left, const vector_t& right);
void divide(vector_t& result, const vector_t& left, const vector_t& right);

// Операции сравнения
void equals(vector_t& result, const vector_t& left, const vector_t& right);
void not_equals(vector_t& result, const vector_t& left, const vector_t& right);
void greater_than(vector_t& result, const vector_t& left, const vector_t& right);
void less_than(vector_t& result, const vector_t& left, const vector_t& right);

// Логические операции
void logical_and(vector_t& result, const vector_t& left, const vector_t& right);
void logical_or(vector_t& result, const vector_t& left, const vector_t& right);
void logical_not(vector_t& result, const vector_t& input);

// Агрегатные функции
value_t sum(const vector_t& input);
value_t avg(const vector_t& input);
value_t min(const vector_t& input);
value_t max(const vector_t& input);
size_t count(const vector_t& input);

// Утилиты
void copy(vector_t& target, const vector_t& source);
void flatten(vector_t& vector);  // Преобразовать в FLAT
void filter(vector_t& result, const vector_t& input,
            const selection_vector_t& sel);

}  // namespace vector_operations
```

**Пример векторизованной операции**:
```cpp
// result = left + right (векторизованно)
void add(vector_t& result, const vector_t& left, const vector_t& right) {
    auto* result_data = result.get_data<int32_t>();
    auto* left_data = left.get_data<int32_t>();
    auto* right_data = right.get_data<int32_t>();

    // Векторизованный loop (может быть SIMD оптимизирован)
    for (size_t i = 0; i < left.size(); ++i) {
        if (left.get_validity().is_valid(i) &&
            right.get_validity().is_valid(i)) {
            result_data[i] = left_data[i] + right_data[i];
            result.get_validity().set_valid(i);
        } else {
            result.get_validity().set_invalid(i);
        }
    }
}
```

**Файлы**: `components/vector/`

### components/expressions/

**Назначение**: Выражения для вычислений в запросах

Expressions представляют вычисляемые значения: колонки, константы, арифметические операции, функции, и т.д.

#### Базовый класс

```cpp
enum class expression_type {
    COLUMN_REF,      // Ссылка на колонку
    CONSTANT,        // Константное значение
    COMPARISON,      // Сравнение (=, <, >, ...)
    CONJUNCTION,     // AND, OR
    OPERATOR_EXPR,   // Арифметика (+, -, *, /)
    FUNCTION,        // Скалярная функция
    AGGREGATE,       // Агрегатная функция (SUM, AVG, ...)
    CASE,            // CASE WHEN
    CAST             // Type cast
};

class expression_t {
public:
    virtual ~expression_t() = default;

    // Тип выражения
    virtual expression_type type() const = 0;

    // Возвращаемый тип
    virtual logical_type return_type() const = 0;

    // Вычисление выражения (векторизованно)
    virtual void execute(execution_context_t& context,
                        data_chunk_t& input,
                        vector_t& result) = 0;

    // Дочерние выражения
    std::vector<std::unique_ptr<expression_t>> children;
};
```

#### Типы выражений

**1. Column Reference**

Ссылка на колонку таблицы:

```cpp
class column_ref_expression_t : public expression_t {
public:
    column_ref_expression_t(std::string table_name,
                           std::string column_name,
                           size_t column_index)
        : table_name_(std::move(table_name)),
          column_name_(std::move(column_name)),
          column_index_(column_index) {}

    void execute(execution_context_t& context,
                data_chunk_t& input,
                vector_t& result) override {
        // Просто копируем колонку из input
        result = input.get_column(column_index_);
    }

private:
    std::string table_name_;
    std::string column_name_;
    size_t column_index_;
};
```

**2. Constant Expression**

Константное значение:

```cpp
class constant_expression_t : public expression_t {
public:
    constant_expression_t(value_t value)
        : value_(std::move(value)) {}

    void execute(execution_context_t& context,
                data_chunk_t& input,
                vector_t& result) override {
        // Создаем CONSTANT вектор
        result.set_vector_type(vector_type::CONSTANT);
        result.set_value(value_);
    }

private:
    value_t value_;
};
```

**3. Comparison Expression**

Операции сравнения (=, <, >, <=, >=, !=):

```cpp
enum class comparison_type {
    EQUALS,
    NOT_EQUALS,
    LESS_THAN,
    LESS_THAN_OR_EQUAL,
    GREATER_THAN,
    GREATER_THAN_OR_EQUAL
};

class comparison_expression_t : public expression_t {
public:
    void execute(execution_context_t& context,
                data_chunk_t& input,
                vector_t& result) override {
        // Вычисляем левое и правое выражения
        vector_t left_result, right_result;
        children[0]->execute(context, input, left_result);
        children[1]->execute(context, input, right_result);

        // Векторизованное сравнение
        switch (type_) {
            case comparison_type::EQUALS:
                vector_operations::equals(result, left_result, right_result);
                break;
            case comparison_type::LESS_THAN:
                vector_operations::less_than(result, left_result, right_result);
                break;
            // и т.д.
        }
    }

private:
    comparison_type type_;
};
```

**4. Operator Expression**

Арифметические операции (+, -, *, /):

```cpp
enum class operator_type {
    ADD,
    SUBTRACT,
    MULTIPLY,
    DIVIDE,
    MODULO
};

class operator_expression_t : public expression_t {
public:
    void execute(execution_context_t& context,
                data_chunk_t& input,
                vector_t& result) override {
        vector_t left_result, right_result;
        children[0]->execute(context, input, left_result);
        children[1]->execute(context, input, right_result);

        switch (type_) {
            case operator_type::ADD:
                vector_operations::add(result, left_result, right_result);
                break;
            // и т.д.
        }
    }

private:
    operator_type type_;
};
```

**5. Function Expression**

Скалярные функции (UPPER, LOWER, SUBSTRING, и т.д.):

```cpp
class function_expression_t : public expression_t {
public:
    function_expression_t(std::string name,
                         scalar_function_t function)
        : name_(std::move(name)),
          function_(function) {}

    void execute(execution_context_t& context,
                data_chunk_t& input,
                vector_t& result) override {
        // Вычисляем аргументы
        std::vector<vector_t> args;
        for (auto& child : children) {
            vector_t arg;
            child->execute(context, input, arg);
            args.push_back(std::move(arg));
        }

        // Вызываем функцию
        function_(args, result);
    }

private:
    std::string name_;
    scalar_function_t function_;
};
```

**6. Aggregate Expression**

Агрегатные функции (SUM, AVG, COUNT, MIN, MAX):

```cpp
class aggregate_expression_t : public expression_t {
public:
    aggregate_expression_t(aggregate_type type)
        : type_(type) {}

    // Для агрегатов используется другой интерфейс:
    // init, update, finalize

    void init(aggregate_state_t& state);
    void update(aggregate_state_t& state, data_chunk_t& input);
    void finalize(aggregate_state_t& state, value_t& result);

private:
    aggregate_type type_;
};
```

**Файлы**: `components/expressions/`

---

## Storage Layer

### components/table/

**Назначение**: Колоночное хранилище табличных данных с MVCC

Table component - ядро хранилища Otterbrix, реализующее эффективное колоночное хранение с поддержкой транзакций.

#### Иерархия хранения

```
data_table_t
  └─ collection_t
      └─ row_group_t (122,880 строк)
          └─ column_data_t
              └─ column_segment_t (сжатые блоки)
```

#### Data Table

Главная структура таблицы:

```cpp
class data_table_t {
public:
    data_table_t(table_id_t id, schema_t schema);

    // CRUD операции
    void append(data_chunk_t& chunk);
    void update(const std::vector<row_id_t>& row_ids,
                const std::vector<column_id_t>& column_ids,
                data_chunk_t& updates);
    void remove(const std::vector<row_id_t>& row_ids);

    // Сканирование
    void scan(execution_context_t& context,
              table_scan_state_t& state,
              data_chunk_t& result);

    // Метаданные
    table_id_t get_id() const { return id_; }
    const schema_t& get_schema() const { return schema_; }
    size_t get_row_count() const;

private:
    table_id_t id_;
    schema_t schema_;
    std::unique_ptr<collection_t> collection_;
    std::unique_ptr<row_version_manager_t> version_manager_;
};
```

**Файлы**: `components/table/data_table.hpp/cpp`

#### Collection

Менеджер row groups:

```cpp
class collection_t {
public:
    // Добавление данных
    void append(data_chunk_t& chunk);

    // Получение row group
    row_group_t& get_row_group(size_t index);
    size_t get_row_group_count() const;

    // Параллельное сканирование
    void parallel_scan(std::function<void(row_group_t&)> callback);

private:
    std::vector<std::unique_ptr<row_group_t>> row_groups_;
    size_t row_group_size_ = 122880;  // 2^17 - 2048
};
```

**Файлы**: `components/table/collection.hpp/cpp`

#### Row Group

Фиксированный блок строк:

```cpp
class row_group_t {
public:
    row_group_t(size_t start_row, size_t capacity);

    // Append в row group
    void append(data_chunk_t& chunk);

    // Сканирование row group
    void scan(table_scan_state_t& state, data_chunk_t& result);

    // Статистики для zone maps
    const statistics_t& get_statistics(column_id_t column_id) const;

    // Метаданные
    size_t get_start_row() const { return start_row_; }
    size_t get_row_count() const { return row_count_; }
    size_t get_capacity() const { return capacity_; }
    bool is_full() const { return row_count_ >= capacity_; }

private:
    size_t start_row_;
    size_t row_count_;
    size_t capacity_;

    // Колонки row group
    std::vector<std::unique_ptr<column_data_t>> columns_;

    // Статистики (min/max для каждой колонки)
    std::vector<statistics_t> statistics_;
};
```

**Файлы**: `components/table/row_group.hpp/cpp`

#### Column Data

Базовое хранилище колонки:

```cpp
class column_data_t {
public:
    virtual ~column_data_t() = default;

    // Append данных
    virtual void append(vector_t& data) = 0;

    // Scan данных
    virtual void scan(column_scan_state_t& state, vector_t& result) = 0;

    // Fetch конкретных строк
    virtual void fetch(const std::vector<row_id_t>& row_ids,
                      vector_t& result) = 0;

    // Update строк
    virtual void update(const std::vector<row_id_t>& row_ids,
                       vector_t& updates) = 0;

    // Тип колонки
    virtual logical_type get_type() const = 0;

protected:
    // Сегменты данных
    std::vector<std::unique_ptr<column_segment_t>> segments_;
};
```

**Специализации**:

**1. Standard Column Data** - для примитивных типов:
```cpp
class standard_column_data_t : public column_data_t {
    // INTEGER, DOUBLE, VARCHAR, и т.д.
};
```

**2. Struct Column Data** - для вложенных структур:
```cpp
class struct_column_data_t : public column_data_t {
    // Вложенные колонки для каждого поля
    std::vector<std::unique_ptr<column_data_t>> child_columns_;
};
```

**3. List Column Data** - для списков переменной длины:
```cpp
class list_column_data_t : public column_data_t {
    // Offset вектор: границы списков
    std::unique_ptr<column_data_t> offset_column_;
    // Child вектор: элементы всех списков
    std::unique_ptr<column_data_t> child_column_;
};
```

**4. Array Column Data** - для массивов фиксированной длины:
```cpp
class array_column_data_t : public column_data_t {
    size_t array_size_;  // Фиксированный размер
    std::unique_ptr<column_data_t> child_column_;
};
```

**Файлы**:
- `components/table/column_data.hpp/cpp`
- `components/table/standard_column_data.hpp/cpp`
- `components/table/struct_column_data.hpp/cpp`
- `components/table/list_column_data.hpp/cpp`
- `components/table/array_column_data.hpp/cpp`

#### Column Segment

Сжатые сегменты данных:

```cpp
class column_segment_t {
public:
    column_segment_t(logical_type type, size_t capacity);

    // Append данных (с компрессией)
    void append(vector_t& data);

    // Scan данных (с декомпрессией)
    void scan(size_t offset, size_t count, vector_t& result);

    // Статистики
    const statistics_t& get_statistics() const { return stats_; }

    // Метаданные
    size_t get_row_count() const { return row_count_; }
    size_t get_compressed_size() const;

private:
    logical_type type_;
    size_t row_count_;
    size_t capacity_;

    // Сжатые данные
    std::unique_ptr<buffer_t> data_;

    // Компрессия
    compression_type compression_;

    // Статистики (min, max, null_count, distinct_count)
    statistics_t stats_;
};
```

**Типы компрессии**:
- **Dictionary Compression** - для low-cardinality данных
- **RLE (Run-Length Encoding)** - для повторяющихся значений
- **Bit-packing** - для small integers
- **Delta Encoding** - для монотонных последовательностей

**Файлы**: `components/table/column_segment.hpp/cpp`

#### MVCC Support

**Row Version Manager**:

```cpp
class row_version_manager_t {
public:
    // Создание новой версии строки
    void create_version(transaction_id_t txn_id,
                       row_id_t row_id,
                       operation_type op_type);

    // Получение видимой версии для транзакции
    std::optional<row_version_t>
    get_visible_version(transaction_id_t txn_id, row_id_t row_id);

    // Garbage collection старых версий
    void cleanup(transaction_id_t oldest_active_txn);

private:
    // Версии строк
    std::unordered_map<row_id_t, std::vector<row_version_t>> versions_;

    // Lock для конкурентного доступа
    std::shared_mutex mutex_;
};
```

**Update Segment** - хранение обновлений:

```cpp
class update_segment_t {
public:
    // Добавление обновления
    void add_update(row_id_t row_id,
                   column_id_t column_id,
                   value_t new_value);

    // Получение обновленного значения
    std::optional<value_t>
    get_update(row_id_t row_id, column_id_t column_id);

    // Слияние с основными данными
    void flush_to_column_data(column_data_t& column);

private:
    // Map: row_id → column updates
    std::unordered_map<row_id_t,
                      std::unordered_map<column_id_t, value_t>> updates_;

    // String heap для строковых значений
    core::string_heap_t heap_;
};
```

**Файлы**:
- `components/table/row_version_manager.hpp/cpp`
- `components/table/update_segment.hpp/cpp`

---

## Index Structures

### components/index/

**Назначение**: Индексные структуры для быстрого поиска

#### B+ Tree Index

Основная индексная структура:

```cpp
class b_plus_tree_index_t {
public:
    b_plus_tree_index_t(column_id_t column_id, logical_type key_type);

    // Вставка ключа
    bool insert(const value_t& key, row_id_t row_id);

    // Удаление ключа
    bool remove(const value_t& key, row_id_t row_id);

    // Point lookup
    std::vector<row_id_t> lookup(const value_t& key);

    // Range scan
    std::vector<row_id_t> range_scan(const value_t& min_key,
                                     const value_t& max_key);

    // Ordered scan
    void scan(std::function<void(value_t, row_id_t)> callback);

private:
    struct node_t {
        bool is_leaf;
        std::vector<value_t> keys;
        std::vector<node_t*> children;  // Для internal nodes
        std::vector<row_id_t> values;   // Для leaf nodes
        node_t* next;                   // Для leaf nodes (linked list)
    };

    node_t* root_;
    column_id_t column_id_;
    logical_type key_type_;
};
```

**Особенности**:
- Ordered storage для range queries
- Листья связаны в linked list для последовательного сканирования
- Concurrent access с read/write latches

**Файлы**: `core/b_plus_tree/`, `components/index/`

---

## Core Infrastructure

### components/catalog/

**Назначение**: Управление метаданными (schemas, tables, indexes)

Catalog хранит информацию о структуре базы данных.

```cpp
class catalog_t {
public:
    // Database operations
    void create_database(const std::string& name);
    void drop_database(const std::string& name);
    database_entry_t* get_database(const std::string& name);

    // Schema operations
    void create_schema(const std::string& db_name,
                      const std::string& schema_name);
    schema_entry_t* get_schema(const std::string& db_name,
                              const std::string& schema_name);

    // Table operations
    void create_table(const std::string& schema_path,
                     const std::string& table_name,
                     const schema_t& schema);
    void drop_table(const std::string& schema_path,
                   const std::string& table_name);
    table_entry_t* get_table(const std::string& schema_path,
                            const std::string& table_name);

    // Index operations
    void create_index(const std::string& table_path,
                     const std::string& index_name,
                     const std::vector<std::string>& column_names);
    index_entry_t* get_index(const std::string& index_name);

    // Statistics
    void update_statistics(const std::string& table_path);
    const table_statistics_t& get_statistics(const std::string& table_path);

private:
    std::unordered_map<std::string, std::unique_ptr<database_entry_t>> databases_;
    std::shared_mutex mutex_;
};
```

**Catalog Entries**:

```cpp
struct database_entry_t {
    std::string name;
    std::unordered_map<std::string, std::unique_ptr<schema_entry_t>> schemas;
};

struct schema_entry_t {
    std::string name;
    database_entry_t* database;
    std::unordered_map<std::string, std::unique_ptr<table_entry_t>> tables;
};

struct table_entry_t {
    std::string name;
    schema_entry_t* schema;
    std::unique_ptr<data_table_t> table;
    std::vector<std::unique_ptr<index_entry_t>> indexes;
    table_statistics_t statistics;
};

struct table_statistics_t {
    size_t row_count;
    size_t block_count;
    std::unordered_map<column_id_t, column_statistics_t> column_stats;
};

struct column_statistics_t {
    value_t min_value;
    value_t max_value;
    size_t null_count;
    size_t distinct_count;
    histogram_t histogram;
};
```

**Файлы**: `components/catalog/`

### components/session/

**Назначение**: Управление сессиями и транзакциями

```cpp
class session_t {
public:
    session_t(session_id_t id, catalog_t& catalog);

    // Transaction management
    void begin_transaction();
    void commit_transaction();
    void rollback_transaction();

    transaction_id_t get_transaction_id() const { return txn_id_; }
    transaction_state get_state() const { return state_; }

    // Session info
    session_id_t get_id() const { return id_; }
    catalog_t& get_catalog() { return catalog_; }

    // Prepared statements
    void prepare_statement(const std::string& name,
                          const std::string& sql);
    prepared_statement_t* get_prepared_statement(const std::string& name);

private:
    session_id_t id_;
    transaction_id_t txn_id_;
    transaction_state state_;
    catalog_t& catalog_;

    std::unordered_map<std::string, std::unique_ptr<prepared_statement_t>> prepared_statements_;
};
```

**Файлы**: `components/session/`

### components/context/

**Назначение**: Контекст выполнения запроса

```cpp
class execution_context_t {
public:
    execution_context_t(session_t& session);

    // Access
    session_t& get_session() { return session_; }
    catalog_t& get_catalog() { return session_.get_catalog(); }
    transaction_id_t get_transaction_id() { return session_.get_transaction_id(); }

    // Memory management
    memory_pool_t& get_memory_pool() { return memory_pool_; }

    // Profiling
    query_profiler_t& get_profiler() { return profiler_; }

private:
    session_t& session_;
    memory_pool_t memory_pool_;
    query_profiler_t profiler_;
};
```

**Файлы**: `components/context/`

---

## Utilities

### components/types/

**Назначение**: Система типов данных

```cpp
enum class logical_type_id {
    INVALID,
    BOOLEAN,
    TINYINT,
    SMALLINT,
    INTEGER,
    BIGINT,
    FLOAT,
    DOUBLE,
    DECIMAL,
    VARCHAR,
    BLOB,
    DATE,
    TIME,
    TIMESTAMP,
    INTERVAL,
    STRUCT,
    LIST,
    ARRAY,
    MAP,
    JSON
};

class logical_type {
public:
    logical_type_id id() const;
    size_t size() const;
    bool is_numeric() const;
    bool is_integral() const;
    // ...
};
```

**Файлы**: `components/types/`

### components/serialization/

**Назначение**: Сериализация данных (msgpack)

**Файлы**: `components/serialization/`

### components/log/

**Назначение**: Структурированное логирование (spdlog)

**Файлы**: `components/log/`

---

## Заключение

Компоненты Otterbrix образуют модульную и расширяемую архитектуру для высокопроизводительной обработки полуструктурированных данных.

**Ключевые takeaways**:
- **Модульность**: Четкое разделение ответственности между компонентами
- **Векторизация**: Batch-обработка для эффективности
- **Колоночное хранилище**: Оптимизация для аналитических запросов
- **MVCC**: Изоляция транзакций без блокировок на чтение
- **Расширяемость**: Легко добавлять новые типы, функции, операторы

Для понимания того, как компоненты взаимодействуют, см. [ARCHITECTURE.md](ARCHITECTURE.md).
