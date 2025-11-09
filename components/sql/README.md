# SQL Component

## Назначение

Компонент `sql` обеспечивает парсинг SQL запросов и преобразование их в логические планы. Включает PostgreSQL-совместимый парсер и трансформер.

## Структура директорий

```
sql/
├── parser/           # PostgreSQL-based SQL парсер
└── transformer/      # Преобразование AST в логический план
```

## Основные файлы

### Parser (PostgreSQL-based)

**Основные файлы:**
- `gram.hpp/cpp` - Грамматика SQL (YACC/Bison)
- `scan.cpp` - Лексический анализатор (Flex)
- `parser.cpp` - Основной парсер
- `keyword.cpp` - SQL ключевые слова
- `scansup.cpp` - Вспомогательные функции сканера
- `pg_functions.cpp` - PostgreSQL функции
- `pg_std_list.cpp` - Списковые структуры PostgreSQL
- `guc.cpp` - Grand Unified Configuration
- `expr_location.cpp` - Отслеживание позиций в выражениях

**Узлы AST:**
- `nodes/nodes.cpp` - Базовые узлы AST
- `nodes/makefuncs.cpp` - Функции создания узлов
- `nodes/value.cpp` - Значения в AST

### Transformer

**Основные файлы:**
- `transformer.hpp/cpp` - Основной класс трансформера
- `transform_result.hpp/cpp` - Результат трансформации
- `utils.hpp/cpp` - Утилиты преобразования

**Реализации преобразований:**
- `impl/transform_select.cpp` - SELECT запросы
- `impl/transform_insert.cpp` - INSERT операции
- `impl/transform_update.cpp` - UPDATE операции
- `impl/transform_delete.cpp` - DELETE операции
- `impl/transform_database.cpp` - операции с БД (CREATE/DROP DATABASE)
- `impl/transform_table.cpp` - операции с таблицами (CREATE/DROP TABLE)
- `impl/transform_index.cpp` - операции с индексами (CREATE/DROP INDEX)
- `impl/transfrom_common.cpp` - Общие утилиты преобразования

## Ключевые классы и функциональность

### Парсер

Парсер преобразует текст SQL запроса в Abstract Syntax Tree (AST):

**Поддерживаемые конструкции:**
- SELECT с WHERE, JOIN, GROUP BY, ORDER BY, LIMIT
- INSERT с VALUES или подзапросами
- UPDATE с SET и WHERE
- DELETE с WHERE
- CREATE/DROP DATABASE
- CREATE/DROP TABLE
- CREATE/DROP INDEX
- Выражения: арифметические, логические, сравнения
- Функции: агрегатные и скалярные
- Подзапросы

### Трансформер

- **`transformer`** - Класс, преобразующий AST в логический план:

**Преобразования запросов:**

1. **SELECT** → логический план с узлами:
   - `node_data` - источник данных
   - `node_match` - фильтрация (WHERE)
   - `node_join` - соединения
   - `node_group` - группировка
   - `node_sort` - сортировка
   - `node_limit` - ограничение результатов

2. **INSERT** → `node_insert` с данными

3. **UPDATE** → `node_update` с выражениями обновления

4. **DELETE** → `node_delete` с фильтрами

5. **DDL операции** → соответствующие узлы создания/удаления

**Преобразование выражений:**
- WHERE условия → compare_expression, scalar_expression
- JOIN условия → compare_expression
- Агрегатные функции → aggregate_expression
- Скалярные функции → scalar_expression

### Поддержка параметризованных запросов

- Параметры в формате `$1`, `$2`, и т.д.
- Безопасная подстановка значений
- Предотвращение SQL injection

## Использование

SQL компонент является входной точкой обработки запросов:

```
SQL текст → Parser → AST → Transformer → Logical Plan
  → Planner → Physical Plan Generator → Execution
```

**Процесс обработки:**

1. **Лексический анализ** - разбиение текста на токены
2. **Синтаксический анализ** - построение AST по грамматике
3. **Семантический анализ** - проверка корректности (в трансформере)
4. **Преобразование** - создание логического плана

```cpp
// Пример использования
std::string sql = "SELECT * FROM users WHERE age > 18";

// Парсинг
auto ast = parse_sql(sql);

// Трансформация
transformer trans;
auto logical_plan = trans.transform(ast);
```

Компонент обеспечивает совместимость с PostgreSQL SQL диалектом, что позволяет использовать знакомый синтаксис и мигрировать приложения.
