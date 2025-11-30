# Руководство по переводу запросов JSONBench для Otterbrix

## Обзор

Этот документ содержит детальное сравнение запросов JSONBench для разных СУБД и рекомендации по адаптации их для Otterbrix.

## Структура JSON документа

Пример события Bluesky:
```json
{
  "did": "did:plc:ab3cd4ef5gh6ij7kl8mn9op0",
  "kind": "commit",
  "time_us": 1704067200000000,
  "commit": {
    "collection": "app.bsky.feed.post",
    "operation": "create",
    "record": { ... }
  }
}
```

## Query 1: Группировка по типу события

### Цель
Подсчитать количество событий каждого типа (`commit.collection`).

### Варианты реализации

#### DuckDB
```sql
SELECT j->>'$.commit.collection' AS event,
       count() AS count 
FROM bluesky 
GROUP BY event 
ORDER BY count DESC;
```

**Особенности:**
- `j->>'$.commit.collection'` - JSON path с оператором `->>` для извлечения текста
- `count()` без параметров (вместо `COUNT(*)`)

#### PostgreSQL
```sql
SELECT data -> 'commit' ->> 'collection' AS event,
       COUNT(*) as count 
FROM bluesky 
GROUP BY event 
ORDER BY count DESC;
```

**Особенности:**
- `data -> 'commit' ->> 'collection'` - цепочка операторов (-> для объекта, ->> для текста)
- `COUNT(*)` стандартный

#### ClickHouse
```sql
SELECT data.commit.collection AS event,
       count() AS count 
FROM bluesky 
GROUP BY event 
ORDER BY count DESC;
```

**Особенности:**
- `data.commit.collection` - точечная нотация (самая простая)
- `count()` без параметров

#### MongoDB (aggregation pipeline)
```javascript
db.bluesky.aggregate([
  { $group: {
      _id: "$commit.collection",
      count: { $sum: 1 }
  }},
  { $sort: { count: -1 }},
  { $project: {
      event: "$_id",
      count: 1,
      _id: 0
  }}
])
```

### Рекомендация для Otterbrix

**Вариант 1 (предпочтительно, если поддерживается):**
```sql
SELECT data.commit.collection AS event,
       COUNT(*) AS count 
FROM bluesky 
GROUP BY event 
ORDER BY count DESC;
```

**Вариант 2 (если точечная нотация не работает):**
```sql
SELECT data->>'commit.collection' AS event,
       COUNT(*) AS count 
FROM bluesky 
GROUP BY event 
ORDER BY count DESC;
```

**Вариант 3 (если нужна функция):**
```sql
SELECT JSON_EXTRACT(data, '$.commit.collection') AS event,
       COUNT(*) AS count 
FROM bluesky 
GROUP BY event 
ORDER BY count DESC;
```

## Query 2: COUNT DISTINCT

### Цель
Подсчитать события и уникальных пользователей для операций создания.

### Варианты реализации

#### DuckDB
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

#### PostgreSQL
```sql
SELECT data -> 'commit' ->> 'collection' AS event,
       COUNT(*) as count,
       COUNT(DISTINCT data ->> 'did') AS users 
FROM bluesky 
WHERE data ->> 'kind' = 'commit' 
  AND data -> 'commit' ->> 'operation' = 'create' 
GROUP BY event 
ORDER BY count DESC;
```

#### ClickHouse
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

**Особенности:**
- `uniqExact()` вместо `COUNT(DISTINCT)` - ClickHouse-специфичная функция

### Рекомендация для Otterbrix

```sql
SELECT data.commit.collection AS event,
       COUNT(*) AS count,
       COUNT(DISTINCT data.did) AS users 
FROM bluesky 
WHERE data.kind = 'commit' 
  AND data.commit.operation = 'create' 
GROUP BY event 
ORDER BY count DESC;
```

**Важно проверить:**
- Поддерживает ли Otterbrix `COUNT(DISTINCT field)`
- Если нет, возможно есть альтернативная функция типа `uniq()` или `approx_count_distinct()`

## Query 3: Временные функции

### Цель
Распределение событий по часам дня.

### Проблема: Работа с timestamp

Данные хранятся как `time_us` - микросекунды с Unix epoch.
Нужно:
1. Конвертировать в timestamp
2. Извлечь час

### Варианты реализации

#### DuckDB
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

**Шаги:**
1. `j->>'$.time_us'` - извлечь как строку
2. `CAST(... AS BIGINT)` - конвертировать в число
3. `/ 1000000` - конвертировать микросекунды в секунды
4. `TO_TIMESTAMP(...)` - создать timestamp
5. `hour(...)` - извлечь час

#### PostgreSQL
```sql
SELECT data->'commit'->>'collection' AS event,
       EXTRACT(HOUR FROM TO_TIMESTAMP((data->>'time_us')::BIGINT / 1000000)) AS hour_of_day,
       COUNT(*) AS count 
FROM bluesky 
WHERE data->>'kind' = 'commit' 
  AND data->'commit'->>'operation' = 'create' 
  AND data->'commit'->>'collection' IN ('app.bsky.feed.post', 'app.bsky.feed.repost', 'app.bsky.feed.like') 
GROUP BY event, hour_of_day 
ORDER BY hour_of_day, event;
```

**Особенности:**
- `EXTRACT(HOUR FROM ...)` вместо `hour()`
- `::BIGINT` для приведения типа

#### ClickHouse
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

**Особенности:**
- `fromUnixTimestamp64Micro()` - специальная функция для микросекунд
- `toHour()` для извлечения часа

### Рекомендация для Otterbrix

**Проверить в порядке приоритета:**

**1. ClickHouse-style (если Otterbrix поддерживает эти функции):**
```sql
SELECT data.commit.collection AS event,
       toHour(fromUnixTimestamp64Micro(data.time_us)) as hour_of_day,
       COUNT(*) AS count 
FROM bluesky 
WHERE data.kind = 'commit' 
  AND data.commit.operation = 'create' 
  AND data.commit.collection IN ('app.bsky.feed.post', 'app.bsky.feed.repost', 'app.bsky.feed.like') 
GROUP BY event, hour_of_day 
ORDER BY hour_of_day, event;
```

**2. Standard SQL (более вероятно):**
```sql
SELECT data.commit.collection AS event,
       EXTRACT(HOUR FROM TO_TIMESTAMP(data.time_us / 1000000)) as hour_of_day,
       COUNT(*) AS count 
FROM bluesky 
WHERE data.kind = 'commit' 
  AND data.commit.operation = 'create' 
  AND data.commit.collection IN ('app.bsky.feed.post', 'app.bsky.feed.repost', 'app.bsky.feed.like') 
GROUP BY event, hour_of_day 
ORDER BY hour_of_day, event;
```

**3. DuckDB-style:**
```sql
SELECT data.commit.collection AS event,
       hour(TO_TIMESTAMP(data.time_us / 1000000)) as hour_of_day,
       COUNT(*) AS count 
FROM bluesky 
WHERE data.kind = 'commit' 
  AND data.commit.operation = 'create' 
  AND data.commit.collection IN ('app.bsky.feed.post', 'app.bsky.feed.repost', 'app.bsky.feed.like') 
GROUP BY event, hour_of_day 
ORDER BY hour_of_day, event;
```

## Query 4: MIN с ORDER BY

### Цель
Найти 3 пользователей, создавших посты раньше всех.

### Варианты реализации

#### DuckDB
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

**Особенности:**
- `j->>'$.did'::String` - приведение типа через `::`
- `MIN(j->>'$.time_us')` - агрегация минимума

#### PostgreSQL
```sql
SELECT data->>'did' AS user_id,
       MIN(TIMESTAMP WITH TIME ZONE 'epoch' + INTERVAL '1 microsecond' * (data->>'time_us')::BIGINT) AS first_post_ts 
FROM bluesky 
WHERE data->>'kind' = 'commit' 
  AND data->'commit'->>'operation' = 'create' 
  AND data->'commit'->>'collection' = 'app.bsky.feed.post' 
GROUP BY user_id 
ORDER BY first_post_ts ASC 
LIMIT 3;
```

**Особенности:**
- `TIMESTAMP WITH TIME ZONE 'epoch' + INTERVAL '1 microsecond' * ...` - специфичный для PostgreSQL способ

#### ClickHouse
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

### Рекомендация для Otterbrix

```sql
SELECT CAST(data.did AS VARCHAR) AS user_id,
       TO_TIMESTAMP(MIN(data.time_us) / 1000000) AS first_post_date 
FROM bluesky 
WHERE data.kind = 'commit' 
  AND data.commit.operation = 'create'   
  AND data.commit.collection = 'app.bsky.feed.post' 
GROUP BY user_id 
ORDER BY first_post_date ASC 
LIMIT 3;
```

**Альтернатива (если `::` поддерживается):**
```sql
SELECT data.did::String AS user_id,
       TO_TIMESTAMP(MIN(data.time_us) / 1000000) AS first_post_date 
FROM bluesky 
WHERE data.kind = 'commit' 
  AND data.commit.operation = 'create'   
  AND data.commit.collection = 'app.bsky.feed.post' 
GROUP BY user_id 
ORDER BY first_post_date ASC 
LIMIT 3;
```

## Query 5: DATEDIFF

### Цель
Найти пользователей с наибольшим временным промежутком между первым и последним постом.

### Проблема: Разница между датами

Нужна функция для вычисления разницы в миллисекундах между двумя timestamp.

### Варианты реализации

#### DuckDB
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

**Функция:** `date_diff('milliseconds', ts1, ts2)`

#### PostgreSQL
```sql
SELECT data->>'did' AS user_id,
       EXTRACT(EPOCH FROM (
           MAX(TIMESTAMP WITH TIME ZONE 'epoch' + INTERVAL '1 microsecond' * (data->>'time_us')::BIGINT) - 
           MIN(TIMESTAMP WITH TIME ZONE 'epoch' + INTERVAL '1 microsecond' * (data->>'time_us')::BIGINT)
       )) * 1000 AS activity_span 
FROM bluesky 
WHERE data->>'kind' = 'commit' 
  AND data->'commit'->>'operation' = 'create' 
  AND data->'commit'->>'collection' = 'app.bsky.feed.post' 
GROUP BY user_id 
ORDER BY activity_span DESC 
LIMIT 3;
```

**Подход:**
- Вычитание timestamp дает interval
- `EXTRACT(EPOCH FROM interval)` дает секунды
- `* 1000` для конвертации в миллисекунды

#### ClickHouse
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

**Функция:** `date_diff('milliseconds', ts1, ts2)` (похоже на DuckDB)

### Рекомендация для Otterbrix

**Вариант 1 (если есть date_diff):**
```sql
SELECT CAST(data.did AS VARCHAR) AS user_id,
       DATEDIFF('millisecond',
                TO_TIMESTAMP(MIN(data.time_us) / 1000000),
                TO_TIMESTAMP(MAX(data.time_us) / 1000000)) AS activity_span 
FROM bluesky 
WHERE data.kind = 'commit' 
  AND data.commit.operation = 'create' 
  AND data.commit.collection = 'app.bsky.feed.post' 
GROUP BY user_id 
ORDER BY activity_span DESC 
LIMIT 3;
```

**Вариант 2 (если есть TIMESTAMPDIFF):**
```sql
SELECT CAST(data.did AS VARCHAR) AS user_id,
       TIMESTAMPDIFF(MILLISECOND,
                     TO_TIMESTAMP(MIN(data.time_us) / 1000000),
                     TO_TIMESTAMP(MAX(data.time_us) / 1000000)) AS activity_span 
FROM bluesky 
WHERE data.kind = 'commit' 
  AND data.commit.operation = 'create' 
  AND data.commit.collection = 'app.bsky.feed.post' 
GROUP BY user_id 
ORDER BY activity_span DESC 
LIMIT 3;
```

**Вариант 3 (математический, если функций нет):**
```sql
SELECT CAST(data.did AS VARCHAR) AS user_id,
       (MAX(data.time_us) - MIN(data.time_us)) / 1000 AS activity_span 
FROM bluesky 
WHERE data.kind = 'commit' 
  AND data.commit.operation = 'create' 
  AND data.commit.collection = 'app.bsky.feed.post' 
GROUP BY user_id 
ORDER BY activity_span DESC 
LIMIT 3;
```

## Сводная таблица функций

| Функция | DuckDB | PostgreSQL | ClickHouse | Otterbrix |
|---------|--------|------------|------------|-----------|
| **JSON доступ** | `j->>'$.field'` | `data->>'field'` | `data.field` | `?` |
| **COUNT** | `count()` | `COUNT(*)` | `count()` | `?` |
| **COUNT DISTINCT** | `count(DISTINCT field)` | `COUNT(DISTINCT field)` | `uniqExact(field)` | `?` |
| **Timestamp из μs** | `TO_TIMESTAMP(us/1000000)` | `TO_TIMESTAMP(us/1000000)` | `fromUnixTimestamp64Micro(us)` | `?` |
| **Извлечение часа** | `hour(ts)` | `EXTRACT(HOUR FROM ts)` | `toHour(ts)` | `?` |
| **Разница дат** | `date_diff('ms', ts1, ts2)` | `EXTRACT(EPOCH FROM (ts2-ts1))*1000` | `date_diff('ms', ts1, ts2)` | `?` |
| **Приведение типа** | `field::String` | `field::VARCHAR` | `field::String` | `?` |

## Скрипт для тестирования

Создайте `test_syntax.py` для проверки синтаксиса:

```python
#!/usr/bin/env python3
from otterbrix import Client
import json

def test_syntax():
    client = Client()
    
    # Создать тестовую БД
    client.execute("CREATE DATABASE IF NOT EXISTS test_syntax")
    client.execute("USE test_syntax")
    client.execute("DROP TABLE IF EXISTS test_table")
    client.execute("CREATE TABLE test_table (data JSON)")
    
    # Вставить тестовые данные
    test_data = {
        "did": "user123",
        "kind": "commit",
        "time_us": 1704067200000000,
        "commit": {
            "collection": "app.bsky.feed.post",
            "operation": "create"
        }
    }
    
    json_str = json.dumps(test_data).replace("'", "''")
    client.execute(f"INSERT INTO test_table VALUES ('{json_str}')")
    
    # Тесты
    tests = {
        "JSON access (dot)": "SELECT data.did FROM test_table",
        "JSON access (arrow)": "SELECT data->'did' FROM test_table",
        "JSON access (path)": "SELECT data->>'$.did' FROM test_table",
        "Nested field (dot)": "SELECT data.commit.collection FROM test_table",
        "COUNT(*)": "SELECT COUNT(*) FROM test_table",
        "count()": "SELECT count() FROM test_table",
        "TO_TIMESTAMP": "SELECT TO_TIMESTAMP(data.time_us / 1000000) FROM test_table",
        "hour()": "SELECT hour(TO_TIMESTAMP(data.time_us / 1000000)) FROM test_table",
        "EXTRACT HOUR": "SELECT EXTRACT(HOUR FROM TO_TIMESTAMP(data.time_us / 1000000)) FROM test_table",
        "CAST VARCHAR": "SELECT CAST(data.did AS VARCHAR) FROM test_table",
        "::String": "SELECT data.did::String FROM test_table",
    }
    
    results = {}
    for name, query in tests.items():
        try:
            cursor = client.execute(query)
            result = cursor.fetchall()
            cursor.close()
            results[name] = ("✓", result)
        except Exception as e:
            results[name] = ("✗", str(e))
    
    # Вывод результатов
    print("\n=== Syntax Test Results ===\n")
    for name, (status, result) in results.items():
        print(f"{status} {name}")
        if status == "✓":
            print(f"  Result: {result}")
        else:
            print(f"  Error: {result}")
    
    # Очистка
    client.execute("DROP DATABASE test_syntax")

if __name__ == "__main__":
    test_syntax()
```

## Чек-лист проверки

Для каждого из 5 запросов проверить:

- [ ] Q1: Запрос выполняется без ошибок
- [ ] Q1: Результаты выглядят корректно (есть типы событий и их количество)
- [ ] Q1: Сортировка работает (DESC по count)
- [ ] Q2: COUNT DISTINCT работает
- [ ] Q2: Фильтрация WHERE работает
- [ ] Q3: Конвертация timestamp работает
- [ ] Q3: Извлечение часа работает
- [ ] Q3: IN clause работает
- [ ] Q3: Группировка по 2 полям работает
- [ ] Q4: MIN работает
- [ ] Q4: ORDER BY ASC работает
- [ ] Q4: LIMIT работает
- [ ] Q5: MIN и MAX в одном запросе работают
- [ ] Q5: DATEDIFF или альтернатива работает
- [ ] Q5: Результат в миллисекундах

## Примечания

1. **Производительность**: Точечная нотация (`data.field`) обычно быстрее, чем JSON path (`data->>'$.field'`)
2. **Типы данных**: JSON поля могут требовать явного приведения типов для некоторых операций
3. **Индексы**: Для ускорения запросов, создайте индексы на часто используемые JSON-поля
4. **Результаты**: Убедитесь, что результаты соответствуют ожидаемым (сравните с DuckDB или ClickHouse)

