# Document Table - Текущее состояние (json-3 branch)

## Дата: 15.01.2026
## Ветка: json-3

---

## Архитектура

### Упрощённая структура (после рефакторинга)

В ходе разработки была проведена значительная оптимизация архитектуры:

1. **Убран dynamic_schema_t** - вся логика схемы встроена в `document_table_storage_t`
2. **Убраны union types** - упрощена типизация, каждая колонка имеет один тип
3. **Добавлена синхронизация с catalog** - схема теперь отражается в catalog

### Ключевые компоненты

```
components/document_table/
├── document_table_storage.hpp/cpp  # Основное хранилище
├── json_path_extractor.hpp/cpp     # Извлечение путей из JSON
└── test/                           # Тесты

components/physical_plan/document_table/operators/
├── scan/
│   ├── full_scan.hpp/cpp           # Полное сканирование
│   └── primary_key_scan.hpp/cpp    # O(1) lookup по _id
├── operator_insert.hpp/cpp         # Вставка
├── operator_delete.hpp/cpp         # Удаление
├── operator_update.hpp/cpp         # Обновление
├── columnar_group.hpp/cpp          # GROUP BY
└── aggregation.hpp/cpp             # Агрегации

components/physical_plan_generator/impl/document_table/
├── create_plan_match.hpp/cpp       # Планирование SELECT
├── create_plan_insert.hpp/cpp      # Планирование INSERT
├── create_plan_delete.hpp/cpp      # Планирование DELETE
├── create_plan_update.hpp/cpp      # Планирование UPDATE
└── create_plan_aggregate.hpp/cpp   # Планирование агрегаций
```

### document_table_storage_t

```cpp
class document_table_storage_t {
    // Колонки (встроенная схема, заменяет dynamic_schema)
    std::pmr::vector<column_info_t> columns_;
    std::pmr::unordered_map<std::string, size_t> path_to_index_;
    std::unique_ptr<json_path_extractor_t> extractor_;

    // Данные
    std::unique_ptr<table::data_table_t> table_;

    // O(1) lookup по document_id
    std::pmr::unordered_map<document_id_t, size_t> id_to_row_;
    size_t next_row_id_;
};
```

### column_info_t

```cpp
struct column_info_t {
    std::string json_path;               // JSON path (e.g., "user.address.city")
    types::complex_logical_type type;    // Тип колонки (один тип, без union)
    size_t column_index = 0;             // Индекс в таблице
    bool is_array_element = false;       // Элемент массива?
    size_t array_index = 0;              // Индекс в массиве
};
```

---

## Интеграция

### storage_type_t в context_collection_t

```cpp
enum class storage_type_t : uint8_t {
    DOCUMENT_BTREE = 0,  // B-tree документы
    TABLE_COLUMNS = 1,   // Колоночная таблица со схемой
    DOCUMENT_TABLE = 2   // Гибридное хранилище (NEW)
};
```

### Синхронизация с Catalog

Схема document_table теперь синхронизируется с catalog:
- При INSERT новые колонки добавляются в catalog
- GET SCHEMA возвращает актуальную схему
- Catalog является источником истины для метаданных

---

## Производительность (JSONBench, 10000 записей)

| Запрос | document_table | document (B-tree) | Результат |
|--------|----------------|-------------------|-----------|
| Q1: GROUP BY kind | **92ms** | 175ms | **DT 1.9x быстрее** |
| Q2: GROUP BY + COUNT DISTINCT | **102ms** | 317ms | **DT 3.1x быстрее** |
| Q3: WHERE filter | **3ms** | 2ms | Паритет |
| Q4: Projection | **71ms** | 251ms | **DT 3.5x быстрее** |
| Q5: GROUP BY + MIN | **95ms** | 328ms | **DT 3.4x быстрее** |

**Вывод:** document_table выигрывает в аналитических запросах (GROUP BY, агрегации, projection).

---

## Что работает

- [x] CREATE COLLECTION с STORAGE=DOCUMENT_TABLE
- [x] INSERT с автоматической эволюцией схемы
- [x] SELECT с фильтрами (WHERE)
- [x] Projection (SELECT a, b, c)
- [x] GROUP BY
- [x] Агрегации (COUNT, SUM, AVG, MIN, MAX)
- [x] COUNT DISTINCT
- [x] primary_key_scan (O(1) lookup по _id)
- [x] DELETE
- [x] UPDATE
- [x] Batch INSERT
- [x] Синхронизация схемы с catalog

---

## Известные ограничения

1. **Вложенные поля в фильтрах** - поддержка через dot-notation (commit.operation), но требует правильного маппинга имён
2. **Массивы** - разворачиваются в отдельные колонки (tags[0], tags[1], ...)
3. **INSERT медленнее B-tree** - ожидаемо для колоночного формата

---

## Отличия от документации

Следующие документы содержат **устаревшую информацию**:

| Документ | Устаревшая информация |
|----------|----------------------|
| 00_summary.md | Упоминает dynamic_schema как отдельный класс |
| 01_design.md | Описывает архитектуру до рефакторинга |
| 03_dynamic_schema.md | Полностью устарел - dynamic_schema удалён |
| 04_operators.md | Не отражает текущие операторы |
| README_UNION_TYPES.md | Union types убраны из реализации |
| FINAL_SUMMARY.md | Результаты до оптимизаций |

---

## История изменений (ключевые коммиты)

```
ad08990 test_jsonbench
87ebc88 now all queries faster
0276d40 get rid of unions and dynamic schema    # <-- Ключевой рефакторинг
986a55a migration on catalog
c67b5f7 sync with catalog added
14040cd batch insert done
69c3118 union types added
```

---

## Рекомендации по использованию

**Используйте document_table для:**
- Аналитических запросов (GROUP BY, агрегации)
- Projection queries (SELECT конкретных полей)
- Wide schemas (много полей, нужны немногие)
- HTAP workloads

**Используйте document (B-tree) для:**
- High-throughput INSERT
- Simple CRUD с низкой latency
- Запросов, возвращающих полные документы
