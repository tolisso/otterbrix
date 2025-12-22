# Otterbrix AI Documentation

Документация текущей реализации document_table storage для быстрого понимания архитектуры.

## Быстрый старт

**Начните с:**
1. `FINAL_SUMMARY.md` - текущее состояние проекта, результаты тестов, известные проблемы
2. `storage_architecture.md` - общая архитектура storage layer
3. `document_table_key_points.md` - ключевые моменты document_table

## Структура документации

### Общие документы
- `storage_architecture.md` - архитектура storage layer (document vs document_table)
- `document_table_key_points.md` - ключевые моменты и отличия document_table

### Document Table Implementation
- `document_table/00_summary.md` - общий обзор document_table
- `document_table/01_design.md` - дизайн и архитектура
- `document_table/02_json_path_extractor.md` - работа с JSON путями
- `document_table/03_dynamic_schema.md` - динамическая схема
- `document_table/04_operators.md` - операторы (scan, filter, aggregate, etc.)
- `document_table/README_UNION_TYPES.md` - поддержка union types
- `document_table/FINAL_SUMMARY.md` - итоговый отчет с результатами тестов

## Текущее состояние (из FINAL_SUMMARY.md)

### Реализовано
- ✅ primary_key_scan - O(1) lookup по _id (33.6x ускорение)
- ✅ Колоночный формат с Arrow
- ✅ Динамическая схема (schema evolution)
- ✅ Union types для гибкой типизации
- ✅ Базовые операторы (scan, filter, aggregate, group_by, project)

### Производительность (1000 records)
- Projection queries: 1.2x медленнее чем document (почти паритет!)
- Aggregation: 2.3x медленнее чем document
- INSERT: 47x медленнее чем document (OLTP слабость)

### Известные проблемы
- ⚠️ Crash при фильтрации по вложенным полям (commit.operation)
- ⚠️ Требуется явная генерация _id в document storage

### Следующие шаги
1. Исправить nullptr crash при вложенных полях
2. Реализовать projection pushdown
3. Late materialization для фильтров
4. Vectorized execution для аналитики

## Когда использовать document_table

**Хорошо для:**
- ✅ Analytical queries (GROUP BY, aggregations)
- ✅ Projection queries (SELECT a, b, c)
- ✅ Wide schemas (много полей, нужны немногие)
- ✅ Dynamic schemas (частые изменения структуры)
- ✅ HTAP workloads

**Плохо для:**
- ❌ High-throughput INSERT
- ❌ Simple CRUD (< 10ms latency)
- ❌ Queries возвращающие полные документы
