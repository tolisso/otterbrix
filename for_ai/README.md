# Otterbrix AI Documentation

Документация текущей реализации document_table storage для быстрого понимания архитектуры.

## Быстрый старт

**Начните с:**
1. **`document_table/CURRENT_STATE.md`** - АКТУАЛЬНОЕ состояние (json-3 branch)
2. `storage_architecture.md` - общая архитектура storage layer
3. `document_table_integration_plan.md` - план интеграции с catalog

## Структура документации

### Актуальные документы
- **`document_table/CURRENT_STATE.md`** - текущее состояние после рефакторинга
- `storage_architecture.md` - архитектура storage layer
- `document_table_integration_plan.md` - интеграция с catalog

### Устаревшие документы (для справки)
> ⚠️ Следующие документы описывают состояние ДО рефакторинга (коммит 0276d40)

- `document_table/00_summary.md` - упоминает dynamic_schema (удалён)
- `document_table/01_design.md` - старая архитектура
- `document_table/02_json_path_extractor.md` - актуален частично
- `document_table/03_dynamic_schema.md` - **ПОЛНОСТЬЮ УСТАРЕЛ** (класс удалён)
- `document_table/04_operators.md` - не отражает текущие операторы
- `document_table/README_UNION_TYPES.md` - **УСТАРЕЛ** (union types убраны)
- `document_table/FINAL_SUMMARY.md` - результаты до оптимизаций

## Текущее состояние (json-3 branch)

### Архитектура после рефакторинга
- ✅ dynamic_schema убран - логика встроена в document_table_storage_t
- ✅ Union types убраны - упрощена типизация
- ✅ Синхронизация схемы с catalog
- ✅ Batch INSERT оптимизирован

### Реализовано
- ✅ primary_key_scan - O(1) lookup по _id
- ✅ Колоночный формат
- ✅ Schema evolution (автоматическое расширение схемы)
- ✅ Операторы: full_scan, insert, delete, update, columnar_group, aggregation
- ✅ Projection pushdown
- ✅ COUNT DISTINCT

### Производительность (JSONBench, 10000 records)
| Запрос | document_table | document (B-tree) | Winner |
|--------|----------------|-------------------|--------|
| GROUP BY | 92ms | 175ms | **DT 1.9x** |
| GROUP BY + COUNT DISTINCT | 102ms | 317ms | **DT 3.1x** |
| WHERE filter | 3ms | 2ms | Паритет |
| Projection | 71ms | 251ms | **DT 3.5x** |
| GROUP BY + MIN | 95ms | 328ms | **DT 3.4x** |

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
