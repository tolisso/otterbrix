# Технические требования для интеграции Otterbrix в JSONBench

## Требования к API Otterbrix

### 1. Установка и инициализация

```python
# Установка
pip install otterbrix==1.0.1a9

# Инициализация
from otterbrix import Client

client = Client()
# или с указанием пути
client = Client("/path/to/data")
```

### 2. Создание базы данных и коллекции

```python
# Создание/получение базы данных
db = client["database_name"]

# Создание коллекции (таблицы)
collection = db["collection_name"]
```

### 3. Вставка данных

**Требуется поддержка:**
- Вставка одного JSON-документа
- Batch-вставка множества документов
- Обработка NDJSON формата

```python
# Одна запись
collection.insert({"field": "value"})

# Множество записей
collection.insert_many([
    {"field": "value1"},
    {"field": "value2"}
])
```

### 4. Выполнение SQL-запросов

**Требуется поддержка:**
- SELECT с GROUP BY
- Агрегатные функции: COUNT(), MIN(), MAX()
- COUNT(DISTINCT)
- Работа с JSON-полями (доступ к вложенным полям)
- Функции даты/времени
- ORDER BY и LIMIT
- Фильтрация WHERE с составными условиями

```python
# Выполнение запроса
cursor = client.execute("SELECT * FROM schema.table WHERE count = 1000")

# Получение результатов
for row in cursor:
    print(row)

cursor.close()
```

## SQL-диалект Otterbrix

### Необходимые возможности

#### 1. Работа с JSON

**Доступ к вложенным полям:**
```sql
-- Вариант 1: Точечная нотация
SELECT data.commit.collection FROM bluesky;

-- Вариант 2: JSON path
SELECT data->'commit'->'collection' FROM bluesky;
SELECT data->>'commit.collection' FROM bluesky;

-- Вариант 3: JSON функции
SELECT JSON_EXTRACT(data, '$.commit.collection') FROM bluesky;
```

**Пример из DuckDB для справки:**
```sql
SELECT j->>'$.commit.collection' AS event FROM bluesky;
```

**Пример из PostgreSQL для справки:**
```sql
SELECT data -> 'commit' ->> 'collection' AS event FROM bluesky;
```

**Пример из ClickHouse для справки:**
```sql
SELECT data.commit.collection AS event FROM bluesky;
```

#### 2. Агрегатные функции

**Обязательные:**
- `COUNT()` - подсчет записей
- `COUNT(DISTINCT field)` - подсчет уникальных значений
- `MIN()` - минимальное значение
- `MAX()` - максимальное значение

**Примеры:**
```sql
-- Простой COUNT
SELECT COUNT(*) FROM bluesky;
SELECT COUNT() FROM bluesky;  -- ClickHouse стиль

-- COUNT DISTINCT
SELECT COUNT(DISTINCT data.did) FROM bluesky;

-- MIN/MAX
SELECT MIN(data.time_us) FROM bluesky;
SELECT MAX(data.time_us) FROM bluesky;
```

#### 3. Работа с датой и временем

**Необходимые функции:**

Данные хранятся как `time_us` (микросекунды с epoch). Нужны функции:

```sql
-- Конвертация из микросекунд в timestamp
FROM_UNIXTIME_MICRO(time_us)
TO_TIMESTAMP(time_us / 1000000)

-- Извлечение часа
HOUR(timestamp)
EXTRACT(HOUR FROM timestamp)

-- Разница между датами в миллисекундах
DATEDIFF('millisecond', ts1, ts2)
TIMESTAMPDIFF(MILLISECOND, ts1, ts2)
```

**Примеры из разных БД:**

ClickHouse:
```sql
toHour(fromUnixTimestamp64Micro(data.time_us))
date_diff('milliseconds', min(...), max(...))
```

DuckDB:
```sql
hour(TO_TIMESTAMP(CAST(j->>'$.time_us' AS BIGINT) / 1000000))
date_diff('milliseconds', TO_TIMESTAMP(...), TO_TIMESTAMP(...))
```

PostgreSQL:
```sql
EXTRACT(HOUR FROM TO_TIMESTAMP((data->>'time_us')::BIGINT / 1000000))
EXTRACT(EPOCH FROM (MAX(...) - MIN(...))) * 1000
```

#### 4. Фильтрация и группировка

```sql
-- WHERE с множественными условиями
WHERE data.kind = 'commit' 
  AND data.commit.operation = 'create' 
  AND data.commit.collection IN ('app.bsky.feed.post', 'app.bsky.feed.repost')

-- GROUP BY
GROUP BY event, hour_of_day

-- ORDER BY
ORDER BY count DESC
ORDER BY first_post_date ASC

-- LIMIT
LIMIT 3
```

#### 5. Приведение типов

```sql
-- К строке
CAST(field AS STRING)
field::String

-- К числу
CAST(field AS BIGINT)
field::BIGINT
```

## Структура данных

### Схема таблицы

**Вариант 1: JSON-колонка (как DuckDB)**
```sql
CREATE TABLE bluesky (j JSON);
```

**Вариант 2: JSONB-колонка (как PostgreSQL)**
```sql
CREATE TABLE bluesky (data JSONB);
```

**Вариант 3: Динамический тип (как ClickHouse)**
```sql
CREATE TABLE bluesky (data JSON) ENGINE = MergeTree ORDER BY tuple();
```

### Индексы

**Желательно поддерживать:**

```sql
-- Индекс на JSON-поля для ускорения фильтрации
CREATE INDEX idx_bluesky ON bluesky (
    data.kind,
    data.commit.operation,
    data.commit.collection,
    data.did,
    data.time_us
);
```

**Требования JSONBench:**
- Разрешены кластерные индексы (сортировка таблицы)
- Разрешены некластерные индексы (B-tree и т.д.)
- Индексы должны быть документированы

## 5 Стандартных запросов JSONBench

### Q1: Группировка по типу события

**Что делает:** Подсчет событий каждого типа

**DuckDB версия:**
```sql
SELECT j->>'$.commit.collection' AS event,
       count() AS count 
FROM bluesky 
GROUP BY event 
ORDER BY count DESC;
```

**ClickHouse версия:**
```sql
SELECT data.commit.collection AS event,
       count() AS count 
FROM bluesky 
GROUP BY event 
ORDER BY count DESC;
```

**Требования к Otterbrix:**
- Доступ к вложенному полю `commit.collection`
- `COUNT()` или `count()`
- `GROUP BY` и `ORDER BY`

### Q2: Группировка с COUNT DISTINCT

**Что делает:** Подсчет событий и уникальных пользователей по типам событий с фильтрацией

**DuckDB версия:**
```sql
SELECT j->>'$.commit.collection' AS event,
       count() AS count,
       count(DISTINCT j->>'$.did') AS users 
FROM bluesky 
WHERE (j->>'$.kind' = 'commit') 
  AND (j->>'$.commit.operation' = 'create') 
GROUP BY event 
ORDER BY count DESC;
```

**ClickHouse версия:**
```sql
SELECT data.commit.collection AS event,
       count() AS count,
       uniqExact(data.did) AS users 
FROM bluesky 
WHERE data.kind = 'commit' 
  AND data.commit.operation = 'create' 
GROUP BY event 
ORDER BY count DESC;
```

**Требования к Otterbrix:**
- `COUNT(DISTINCT field)` или эквивалент (`uniqExact`)
- Фильтрация с AND
- Доступ к нескольким JSON-полям

### Q3: Группировка по времени суток

**Что делает:** Распределение событий по часам дня для определенных типов

**DuckDB версия:**
```sql
SELECT j->>'$.commit.collection' AS event,
       hour(TO_TIMESTAMP(CAST(j->>'$.time_us' AS BIGINT) / 1000000)) as hour_of_day,
       count() AS count 
FROM bluesky 
WHERE (j->>'$.kind' = 'commit') 
  AND (j->>'$.commit.operation' = 'create') 
  AND (j->>'$.commit.collection' in ['app.bsky.feed.post', 'app.bsky.feed.repost', 'app.bsky.feed.like']) 
GROUP BY event, hour_of_day 
ORDER BY hour_of_day, event;
```

**ClickHouse версия:**
```sql
SELECT data.commit.collection AS event,
       toHour(fromUnixTimestamp64Micro(data.time_us)) as hour_of_day,
       count() AS count 
FROM bluesky 
WHERE data.kind = 'commit' 
  AND data.commit.operation = 'create' 
  AND data.commit.collection in ['app.bsky.feed.post', 'app.bsky.feed.repost', 'app.bsky.feed.like'] 
GROUP BY event, hour_of_day 
ORDER BY hour_of_day, event;
```

**Требования к Otterbrix:**
- Конвертация микросекунд в timestamp
- Извлечение часа из timestamp
- `IN` clause
- Группировка по двум полям

### Q4: Первые посты пользователей

**Что делает:** Находит 3 пользователей, которые создали посты раньше всех

**DuckDB версия:**
```sql
SELECT j->>'$.did'::String as user_id,
       TO_TIMESTAMP(CAST(MIN(j->>'$.time_us') AS BIGINT) / 1000000) AS first_post_date 
FROM bluesky 
WHERE (j->>'$.kind' = 'commit') 
  AND (j->>'$.commit.operation' = 'create')   
  AND (j->>'$.commit.collection' = 'app.bsky.feed.post') 
GROUP BY user_id 
ORDER BY first_post_date ASC 
LIMIT 3;
```

**ClickHouse версия:**
```sql
SELECT data.did::String as user_id,
       min(fromUnixTimestamp64Micro(data.time_us)) as first_post_ts 
FROM bluesky 
WHERE data.kind = 'commit' 
  AND data.commit.operation = 'create' 
  AND data.commit.collection = 'app.bsky.feed.post' 
GROUP BY user_id 
ORDER BY first_post_ts ASC 
LIMIT 3;
```

**Требования к Otterbrix:**
- `MIN()` агрегация
- Приведение типов (::String)
- `LIMIT`

### Q5: Активность пользователей

**Что делает:** Находит 3 пользователей с наибольшим временным промежутком между первым и последним постом

**DuckDB версия:**
```sql
SELECT j->>'$.did'::String as user_id,
       date_diff('milliseconds', 
                 TO_TIMESTAMP(CAST(MIN(j->>'$.time_us') AS BIGINT) / 1000000),
                 TO_TIMESTAMP(CAST(MAX(j->>'$.time_us') AS BIGINT) / 1000000)) AS activity_span 
FROM bluesky 
WHERE (j->>'$.kind' = 'commit') 
  AND (j->>'$.commit.operation' = 'create') 
  AND (j->>'$.commit.collection' = 'app.bsky.feed.post') 
GROUP BY user_id 
ORDER BY activity_span DESC 
LIMIT 3;
```

**ClickHouse версия:**
```sql
SELECT data.did::String as user_id,
       date_diff('milliseconds', 
                 min(fromUnixTimestamp64Micro(data.time_us)), 
                 max(fromUnixTimestamp64Micro(data.time_us))) AS activity_span 
FROM bluesky 
WHERE data.kind = 'commit' 
  AND data.commit.operation = 'create' 
  AND data.commit.collection = 'app.bsky.feed.post' 
GROUP BY user_id 
ORDER BY activity_span DESC 
LIMIT 3;
```

**Требования к Otterbrix:**
- `MIN()` и `MAX()` в одном запросе
- Функция разности дат
- Вложенные функции

## Необходимые метрики

### 1. Размер данных

**Что нужно:**
- Общий размер БД (в байтах)
- Размер данных (без индексов)
- Размер индексов

**Как получить (нужно уточнить для Otterbrix):**
```python
# Примерный API
db.get_stats()
collection.get_size()
collection.get_index_size()
```

### 2. Количество записей

```python
cursor = client.execute("SELECT COUNT(*) FROM bluesky")
count = cursor.fetchone()[0]
```

### 3. Время выполнения запросов

**Требования:**
- Точность до миллисекунд
- Измерение "cold" и "hot" runs
- 3 прогона каждого запроса

```python
import time

start = time.time()
cursor = client.execute(query)
results = cursor.fetchall()
end = time.time()

execution_time = end - start  # в секундах
```

### 4. Использование памяти

**Желательно, но не обязательно:**
- Peak memory usage
- Current memory usage

### 5. Query plans (планы выполнения)

**Если поддерживается:**
```sql
EXPLAIN SELECT ... FROM bluesky ...;
EXPLAIN ANALYZE SELECT ... FROM bluesky ...;
```

## Производительность и масштабируемость

### Ожидаемые характеристики

**Для 1M записей (~425 MB):**
- Загрузка: < 2 минут
- Каждый запрос: < 5 секунд

**Для 10M записей (~4.25 GB):**
- Загрузка: < 20 минут
- Каждый запрос: < 30 секунд

**Для 100M записей (~42.5 GB):**
- Загрузка: < 3 часов
- Каждый запрос: < 5 минут

**Для 1000M записей (~425 GB):**
- Загрузка: < 24 часов
- Каждый запрос: < 30 минут

### Требования к ресурсам

**Минимум (для 1M-10M):**
- RAM: 8 GB
- Disk: 100 GB
- CPU: 4 cores

**Рекомендуется (для 100M-1000M):**
- RAM: 128 GB
- Disk: 1 TB (10 TB для безопасности)
- CPU: 32 cores
- Instance: AWS m6i.8xlarge или аналог

## Совместимость с JSONBench

### Принципы JSONBench (обязательно соблюдать)

1. **Воспроизводимость**: Все должно быть автоматизировано
2. **Реалистичность**: Использовать реальные данные без модификаций
3. **Честность**: 
   - Использовать настройки по умолчанию
   - Не использовать специфичные для бенчмарка оптимизации
   - Допустимы только индексы (должны быть документированы)
   - Нельзя кешировать результаты запросов

### Формат результатов

**Файл: `results/<machine>_bluesky_<size>m.json`**

```json
{
  "system": "Otterbrix",
  "version": "1.0.1a9",
  "date": "2024-12-01",
  "machine": "m6i.8xlarge",
  "cluster_size": 1,
  "tags": ["Embedded", "C++", "OLAP", "OLTP", "Column-oriented"],
  "load_time": 1234.56,
  "data_size": 123456789,
  "index_size": 12345678,
  "result": [
    [1.23, 1.15, 1.12],  # Q1: cold, hot1, hot2
    [2.34, 2.20, 2.18],  # Q2
    [3.45, 3.30, 3.28],  # Q3
    [0.98, 0.85, 0.83],  # Q4
    [1.76, 1.65, 1.62]   # Q5
  ]
}
```

## Контрольный список готовности

### Функциональность
- [ ] Установка через pip работает
- [ ] Создание БД и коллекции работает
- [ ] Вставка JSON-документов работает
- [ ] SQL-запросы выполняются
- [ ] Доступ к вложенным JSON-полям работает
- [ ] COUNT() работает
- [ ] COUNT(DISTINCT) работает
- [ ] MIN()/MAX() работают
- [ ] Работа с timestamp (конвертация, извлечение часа) работает
- [ ] date_diff или аналог работает
- [ ] GROUP BY работает
- [ ] ORDER BY работает
- [ ] LIMIT работает
- [ ] WHERE с AND и IN работает

### Производительность
- [ ] Загрузка 1M записей работает без ошибок
- [ ] Все запросы возвращают результаты за разумное время
- [ ] Система стабильна при больших объемах данных
- [ ] Память используется эффективно

### Интеграция
- [ ] Все скрипты написаны и протестированы
- [ ] Результаты соответствуют формату JSONBench
- [ ] Документация написана
- [ ] README.md в директории otterbrix/ создан

## Полезные ссылки

- **JSONBench**: https://github.com/ClickHouse/JSONBench
- **Примеры реализации**: 
  - DuckDB: https://github.com/ClickHouse/JSONBench/tree/main/duckdb
  - PostgreSQL: https://github.com/ClickHouse/JSONBench/tree/main/postgresql
  - ClickHouse: https://github.com/ClickHouse/JSONBench/tree/main/clickhouse

