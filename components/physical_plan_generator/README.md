# Physical Plan Generator Component

## Назначение

Компонент `physical_plan_generator` конвертирует логические планы запросов в исполняемые физические планы, выбирая подходящие операторы.

## Основные файлы

- `create_plan.hpp/cpp` - Основная функция генерации физического плана

## Ключевые классы и функциональность

### Основная функция

- **`create_plan()`** - Главная функция преобразования:
  - Принимает логический план (дерево узлов)
  - Возвращает физический план (дерево операторов)
  - Имеет отдельные реализации для collection и table хранилищ

### Процесс генерации

1. **Анализ логического узла** - определение типа операции
2. **Выбор стратегии** - выбор оптимального оператора:
   - Выбор стратегии сканирования (full scan, index scan, primary key)
   - Выбор алгоритма соединения
   - Выбор метода агрегации
3. **Создание физического оператора** - инстанцирование соответствующего класса оператора
4. **Рекурсивная обработка** - генерация операторов для дочерних узлов

### Поддерживаемые преобразования

**DDL операции:**
- CREATE DATABASE → operator_create_database
- DROP DATABASE → operator_drop_database
- CREATE COLLECTION/TABLE → operator_create_collection
- DROP COLLECTION/TABLE → operator_drop_collection
- CREATE INDEX → operator_add_index
- DROP INDEX → operator_drop_index

**DML операции:**
- INSERT → operator_insert
- UPDATE → operator_update
- DELETE → operator_delete

**Query операции:**
- SELECT (filter) → operator_match
- JOIN → operator_join
- GROUP BY → operator_group
- ORDER BY → operator_sort
- LIMIT → встраивается в операторы

**Источники данных:**
- TABLE/COLLECTION → operator_data с выбором стратегии сканирования

### Оптимизации

- Выбор индексного сканирования при наличии подходящих индексов
- Оптимизация предикатов для push-down фильтрации
- Выбор эффективных алгоритмов соединения
- Векторизация для табличных операций

## Использование

Генератор физического плана вызывается после оптимизации логического плана:

```
SQL → Parser → Transformer → Logical Plan → Planner (optimizer)
  → Physical Plan Generator → Physical Plan → Execution
```

Генератор учитывает:
- Тип хранилища (collection vs table)
- Доступные индексы
- Статистику данных (если доступна)
- Характеристики операторов
