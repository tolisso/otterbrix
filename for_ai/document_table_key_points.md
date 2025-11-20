# Document Table Storage - Ключевые моменты

## Что это?

Гибридное хранилище, которое хранит JSON документы в колоночном формате с автоматическим извлечением JSON paths в колонки и динамическим расширением схемы.

## Зачем?

**Проблема:**
- B-tree (document_storage) - гибкий, но медленный для аналитики
- data_table (table_storage) - быстрый, но требует фиксированной схемы

**Решение:**
Document Table = гибкость документов + производительность колонок

## Архитектура (кратко)

```
Документ JSON
    ↓ json_path_extractor
Список JSON paths с типами
    ↓ dynamic_schema
Схема таблицы (динамически расширяемая)
    ↓ document_to_row
Data chunk (колонки)
    ↓ data_table
Колоночное хранилище
```

## Ключевые компоненты

1. **json_path_extractor_t** - извлечение путей из JSON
2. **dynamic_schema_t** - управление схемой с авто-расширением
3. **document_table_storage_t** - само хранилище
4. **document_table/operators** - физические операторы

## SQL

```sql
-- Создание
CREATE COLLECTION users STORAGE=DOCUMENT_TABLE;

-- Вставка (схема расширяется автоматически)
INSERT INTO users VALUES ('{"name": "Alice", "age": 30}');
INSERT INTO users VALUES ('{"name": "Bob", "age": 25, "city": "Moscow"}');
-- Схема теперь: [_id, name, age, city]

-- Запросы (прямой доступ к вложенным полям)
SELECT * FROM users WHERE age > 25;
SELECT name, city FROM users;
```

## Пример эволюции схемы

```
Doc1: {"id": 1, "name": "Alice"}
Схема: [_id, id, name]

Doc2: {"id": 2, "name": "Bob", "city": "Moscow"}
Схема расширяется: [_id, id, name, city]
Doc1 получает NULL для city

Doc3: {"id": 3, "name": "Charlie", "email": "c@example.com"}
Схема: [_id, id, name, city, email]
Doc1: NULL для city и email
Doc2: NULL для email
```

## JSON Paths

```json
{
  "user": {
    "profile": {
      "name": "Alice",
      "age": 30
    }
  },
  "tags": ["dev", "golang"]
}
```

Извлеченные колонки:
- `user.profile.name` (VARCHAR)
- `user.profile.age` (INTEGER)
- `tags[0]` (VARCHAR)
- `tags[1]` (VARCHAR)

## Интеграция

### Новый enum

```cpp
enum class storage_type_t {
    DOCUMENT_BTREE = 0,    // B-tree
    TABLE_COLUMNS = 1,     // data_table
    DOCUMENT_TABLE = 2     // NEW
};
```

### В context_collection_t

```cpp
class context_collection_t {
    document_storage_t document_storage_;
    table_storage_t table_storage_;
    document_table_storage_t document_table_storage_;  // NEW
    storage_type_t storage_type_;  // NEW
};
```

### В executor

```cpp
if (format == used_format_t::document_table) {
    plan = document_table::planner::create_plan(...);
}
```

## Файлы

**Новые:**
```
components/document_table/
  ├── json_path_extractor.{hpp,cpp}
  ├── dynamic_schema.{hpp,cpp}
  └── document_table_storage.{hpp,cpp}

components/physical_plan/document_table/operators/
  ├── full_scan.{hpp,cpp}
  ├── operator_insert.{hpp,cpp}
  ├── operator_delete.{hpp,cpp}
  └── operator_update.{hpp,cpp}

components/physical_plan_generator/
  └── document_table_planner.{hpp,cpp}
```

**Изменяемые:**
```
services/collection/collection.{hpp,cpp}
services/collection/executor.cpp
services/dispatcher/dispatcher.cpp
components/catalog/table_metadata.hpp
```

## Use Cases

✅ **Идеально:**
- Аналитика по полуструктурированным данным
- IoT сенсоры с похожей, но не фиксированной структурой
- Логи событий с разными метаданными
- Документы с относительно стабильной схемой

❌ **Не подходит:**
- Экстремально динамичная структура (каждый документ уникален)
- Очень глубокая вложенность (>10 уровней)
- Частые изменения схемы
- Большие массивы (>100 элементов)

## Производительность

**Преимущества:**
- Колоночное сжатие → меньше I/O
- Векторизация → SIMD
- Batch операции → меньше накладных расходов

**Компромиссы:**
- Эволюция схемы → копирование данных
- Разреженные данные → много NULL
- Сложность обработки массивов

## Этапы реализации

1. ✅ Дизайн архитектуры
2. ✅ JSON path extractor
3. ✅ Dynamic schema
4. ✅ Document table storage
5. ✅ Операторы
6. ✅ План интеграции
7. ⏳ Реализация кода
8. ⏳ Тесты

## Ссылки на детали

Подробные спецификации в `/for_ai/document_table/`:
- `00_summary.md` - полное резюме
- `01_design.md` - дизайн архитектуры
- `02_json_path_extractor.md` - извлечение путей
- `03_dynamic_schema.md` - динамическая схема
- `04_operators.md` - операторы
- `05_integration.md` - интеграция с системой
