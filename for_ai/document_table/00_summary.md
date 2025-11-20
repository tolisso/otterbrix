# Document Table Storage - Итоговое резюме

## Что это?

**Document Table Storage** - новый тип хранения для Otterbrix, который:
- Хранит JSON документы в колоночном формате (как `data_table`)
- Автоматически извлекает JSON paths и создает колонки
- Динамически расширяет схему при появлении новых полей
- Обеспечивает производительность колоночного хранения с гибкостью документов

## Ключевые преимущества

### vs document_storage (B-tree)
✅ Быстрее аналитические запросы (колоночный формат)
✅ Лучшее сжатие данных
✅ Эффективная фильтрация по колонкам
✅ Векторизация операций

### vs table_storage (columns)
✅ Не требует заранее определенной схемы
✅ Автоматическая адаптация к изменениям
✅ Поддержка вложенных структур JSON
✅ Гибкость документной модели

## Архитектура

### Три типа хранения в Otterbrix

```
context_collection_t
├─ document_storage_ (B-tree)          # Для schema-less документов
├─ table_storage_ (data_table)         # Для фиксированной схемы
└─ document_table_storage_ (NEW!)     # Для документов в колонках
```

### Компоненты document_table_storage

```cpp
document_table_storage_t
├─ dynamic_schema_t                    # Динамически расширяемая схема
│  ├─ columns_ (vector)                # Список всех колонок
│  ├─ path_to_index_ (map)             # Маппинг JSON path → column
│  └─ json_path_extractor_t            # Извлечение путей из документов
│
├─ data_table_t                        # Колоночное хранилище (от table_storage)
│  └─ collection_t (row_groups)        # Данные по row groups
│
└─ id_to_row_ (map)                    # Маппинг document_id → row_id
```

## Основные классы

### 1. json_path_extractor_t
Извлекает все JSON paths из документа и определяет типы полей.

**Пример:**
```json
{"user": {"name": "Alice", "age": 30}}
```
↓
```
[
  { path: "user.name", type: VARCHAR },
  { path: "user.age", type: INTEGER }
]
```

### 2. dynamic_schema_t
Управляет схемой таблицы, динамически добавляя новые колонки.

**Методы:**
- `add_column()` - добавить колонку
- `evolve()` - эволюция схемы из документа
- `to_column_definitions()` - конвертация для data_table

### 3. document_table_storage_t
Основное хранилище документов в колоночном формате.

**Методы:**
- `insert()` - вставка с автоматической эволюцией
- `get()` - получение документа по ID
- `scan()` - сканирование таблицы
- `evolve_schema()` - расширение схемы

### 4. Операторы (document_table/operators)
Физические операторы для работы с document_table.

**Основные операторы:**
- `full_scan` - сканирование с предикатами
- `operator_insert` - вставка с эволюцией схемы
- `operator_delete` - удаление
- `operator_update` - обновление
- `operator_group` - группировка
- `operator_join` - join

## Поток данных

### Вставка документа

```
1. Документ
   {"id": 1, "name": "Alice", "age": 30}

2. JSON Path Extraction
   → ["id", "name", "age"]

3. Schema Evolution (если нужно)
   Было: [_id]
   Стало: [_id, id, name, age]

4. Document → Row Conversion
   Chunk: [1, "Alice", 30]

5. Batch Insert в data_table
   → Колоночное хранение

6. Сохранение маппинга
   document_id → row_id
```

### Запрос документов

```
1. SQL Query
   SELECT * FROM users WHERE age > 25

2. Logical Plan
   node_match(predicate: age > 25)

3. Physical Plan (document_table planner)
   full_scan(expression: age > 25)

4. Execution
   → data_table.scan() с фильтром
   → Колоночная фильтрация

5. Result (data_chunk_t)
   Chunk с отфильтрованными строками

6. Row → Document Conversion (опционально)
   Chunk → [document1, document2, ...]
```

## SQL Syntax

### Создание

```sql
-- Вариант 1: Через опцию STORAGE
CREATE COLLECTION users STORAGE=DOCUMENT_TABLE;

-- Вариант 2: Специальная команда
CREATE DOCUMENT_TABLE products;

-- Вариант 3: Через WITH опцию
CREATE TABLE orders() WITH (storage='document_table');
```

### Использование

```sql
-- Вставка документов
INSERT INTO users VALUES ('{
  "id": 1,
  "name": "Alice",
  "profile": {
    "age": 30,
    "city": "Moscow"
  }
}');

-- Запросы (прямой доступ к вложенным полям)
SELECT * FROM users WHERE profile.age > 25;

SELECT name, profile.city FROM users;

-- Агрегация (эффективная благодаря колоночному формату)
SELECT profile.city, COUNT(*), AVG(profile.age)
FROM users
GROUP BY profile.city;

-- Join
SELECT u.name, o.total
FROM users u
JOIN orders o ON u.id = o.user_id;
```

## Эволюция схемы

### Автоматическое расширение

```sql
-- Документ 1
INSERT INTO products VALUES ('{"id": 1, "name": "Laptop", "price": 1000}');
-- Схема: [_id, id, name, price]

-- Документ 2 с новыми полями
INSERT INTO products VALUES ('{
  "id": 2,
  "name": "Mouse",
  "price": 50,
  "brand": "Logitech",
  "warranty_months": 12
}');
-- Схема автоматически расширена: [_id, id, name, price, brand, warranty_months]
-- Документ 1 теперь имеет NULL для brand и warranty_months
```

### Стратегия миграции

При добавлении новых колонок:
1. Создается новая таблица с расширенной схемой
2. Копируются существующие данные
3. Новые колонки заполняются NULL для старых строк
4. Старая таблица заменяется новой

## Обработка массивов

### Стратегия 1: Разворачивание (по умолчанию)

```json
{"tags": ["golang", "rust", "python"]}
```
↓
```
Колонки: tags[0], tags[1], tags[2]
Значения: "golang", "rust", "python"
```

### Стратегия 2: Отдельная таблица

```
main_table: [id, name, ...]
array_table: [doc_id, path, index, value]
```

### Стратегия 3: JSON строка

```
Колонка: tags (VARCHAR)
Значение: '["golang", "rust", "python"]'
```

## Производительность

### Преимущества
- ✅ Колоночное сжатие → меньше I/O
- ✅ Векторизация операций → SIMD
- ✅ Эффективная фильтрация → меньше данных читается
- ✅ Batch операции → меньше накладных расходов

### Компромиссы
- ⚠️ Эволюция схемы → копирование данных
- ⚠️ Много колонок → больше метаданных
- ⚠️ Разреженные данные → много NULL значений
- ⚠️ Сложнее обработка массивов

### Рекомендуемые сценарии

**Идеально для:**
- Аналитических запросов по документам
- Документов с относительно стабильной структурой
- Агрегаций и группировок
- Больших объемов данных с редкими изменениями схемы

**Не подходит для:**
- Документов с экстремально динамической структурой
- Частых изменений схемы (много новых полей)
- Очень вложенных структур (>10 уровней)
- Больших массивов (>100 элементов)

## Интеграция

### Изменения в коде

**Новые файлы:**
```
components/document_table/
├── json_path_extractor.{hpp,cpp}
├── dynamic_schema.{hpp,cpp}
├── document_table_storage.{hpp,cpp}
└── document_chunk_converter.{hpp,cpp}

components/physical_plan/document_table/operators/
├── base_operator.{hpp,cpp}
├── scan/full_scan.{hpp,cpp}
├── operator_insert.{hpp,cpp}
├── operator_delete.{hpp,cpp}
├── operator_update.{hpp,cpp}
└── operator_group.{hpp,cpp}

components/physical_plan_generator/
├── document_table_planner.hpp
└── document_table_planner.cpp
```

**Измененные файлы:**
```
services/collection/collection.{hpp,cpp}
services/collection/executor.{hpp,cpp}
services/dispatcher/dispatcher.cpp
services/memory_storage/memory_storage.cpp
components/catalog/table_metadata.hpp
components/logical_plan/node_create_collection.hpp
components/sql/transformer/impl/transform_table.cpp
```

### Порядок реализации

1. ✅ Дизайн (файл 01_design.md)
2. ✅ JSON path extractor (файл 02_json_path_extractor.md)
3. ✅ Dynamic schema (файл 03_dynamic_schema.md)
4. ✅ Операторы (файл 04_operators.md)
5. ✅ Интеграция (файл 05_integration.md)
6. ⏳ Реализация кода
7. ⏳ Тесты
8. ⏳ Оптимизация
9. ⏳ Документация

## Примеры использования

### IoT данные

```sql
CREATE DOCUMENT_TABLE sensor_data;

INSERT INTO sensor_data VALUES ('{
  "sensor_id": "temp_001",
  "timestamp": "2025-01-18T10:00:00Z",
  "readings": {
    "temperature": 23.5,
    "humidity": 65,
    "pressure": 1013
  },
  "location": {
    "building": "A",
    "floor": 3,
    "room": "301"
  }
}');

-- Эффективный аналитический запрос
SELECT
  location.building,
  AVG(readings.temperature) as avg_temp,
  MAX(readings.humidity) as max_humidity
FROM sensor_data
WHERE readings.temperature > 20
GROUP BY location.building;
```

### E-commerce логи

```sql
CREATE DOCUMENT_TABLE user_events;

INSERT INTO user_events VALUES ('{
  "event_id": "evt_123",
  "user_id": 456,
  "event_type": "purchase",
  "timestamp": "2025-01-18T10:30:00Z",
  "product": {
    "id": "prod_789",
    "name": "Laptop",
    "price": 1000,
    "category": "electronics"
  },
  "metadata": {
    "source": "web",
    "campaign": "winter_sale"
  }
}');

-- Анализ конверсий
SELECT
  metadata.campaign,
  COUNT(*) as events,
  SUM(product.price) as revenue
FROM user_events
WHERE event_type = 'purchase'
GROUP BY metadata.campaign;
```

## Мониторинг и метрики

### Статистика схемы

```cpp
struct schema_stats_t {
    size_t total_columns;        // Количество колонок
    size_t evolution_count;      // Сколько раз расширялась схема
    size_t migration_time_ms;    // Время последней миграции
    float null_ratio;            // Процент NULL значений
    size_t sparse_columns;       // Колонки с >50% NULL
};
```

### Рекомендации по оптимизации

1. **Большой процент NULL?** → Возможно, лучше использовать B-tree
2. **Частая эволюция схемы?** → Рассмотреть batch вставки
3. **Много разреженных колонок?** → Настроить max_array_size
4. **Медленные сканирования?** → Добавить индексы

## Дальнейшие улучшения

### Фаза 2
- [ ] Ленивая миграция (без копирования всех данных)
- [ ] Compression для колонок с NULL
- [ ] Автоматическое удаление неиспользуемых колонок
- [ ] Статистика для query optimizer

### Фаза 3
- [ ] Партиционирование по JSON paths
- [ ] Bloom filters для разреженных колонок
- [ ] Adaptive schema (анализ паттернов и оптимизация)
- [ ] Materialized views для часто используемых JSON paths

## Заключение

Document Table Storage - это мост между документной и колоночной моделями, который:
- Сохраняет гибкость JSON документов
- Обеспечивает производительность колоночного хранения
- Автоматически адаптируется к изменениям структуры данных
- Оптимален для аналитики по полуструктурированным данным

Этот подход делает Otterbrix универсальной платформой для обработки как структурированных, так и полуструктурированных данных с высокой производительностью.
