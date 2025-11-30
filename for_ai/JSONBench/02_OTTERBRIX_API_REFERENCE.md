# Справочник по API Otterbrix для JSONBench

## Python API

### Базовый пример использования

```python
from otterbrix import Client

# Создание клиента
client = Client()

# Выполнение SQL
cursor = client.execute("SELECT * FROM schema.table WHERE count = 1000;")

# Обработка результатов
for row in cursor:
    print(row)

# Закрытие курсора
cursor.close()
```

### Классы и методы

#### Client

**Инициализация:**
```python
class Client:
    def __init__(self, path: str = None):
        """
        Создает клиента Otterbrix.
        
        Args:
            path: Путь к директории с данными (опционально)
        """
        pass
```

**Методы:**
```python
def __getitem__(self, name: str) -> Database:
    """Получить или создать базу данных"""
    pass

def database_names(self) -> list:
    """Получить список имен баз данных"""
    pass

def execute(self, query: str) -> Cursor:
    """
    Выполнить SQL-запрос.
    
    Args:
        query: SQL-запрос
        
    Returns:
        Cursor с результатами
    """
    pass
```

#### Database

```python
class Database:
    def collection_names(self) -> list:
        """Получить список имен коллекций"""
        pass
    
    def drop_collection(self, name: str) -> None:
        """Удалить коллекцию"""
        pass
    
    def __getitem__(self, name: str) -> Collection:
        """Получить или создать коллекцию"""
        pass
```

#### Collection

```python
class Collection:
    def insert(self, document: dict) -> None:
        """Вставить один документ"""
        pass
    
    def insert_many(self, documents: list) -> None:
        """Вставить множество документов"""
        pass
    
    # Дополнительные методы (если есть):
    def find(self, query: dict) -> Cursor:
        """Найти документы"""
        pass
    
    def count(self) -> int:
        """Подсчитать документы"""
        pass
```

#### Cursor

```python
class Cursor:
    def __iter__(self):
        """Итератор по результатам"""
        pass
    
    def fetchone(self) -> tuple:
        """Получить одну строку"""
        pass
    
    def fetchall(self) -> list:
        """Получить все строки"""
        pass
    
    def close(self) -> None:
        """Закрыть курсор"""
        pass
```

## SQL-диалект Otterbrix

### Создание базы данных и таблиц

```sql
-- Создание базы данных
CREATE DATABASE bluesky_db;

-- Использование базы данных
USE bluesky_db;

-- Создание таблицы/коллекции с JSON
CREATE TABLE bluesky (
    data JSON
);

-- Или более специфично (если поддерживается)
CREATE COLLECTION bluesky;
```

### Вставка данных

```sql
-- Вставка одной записи
INSERT INTO bluesky VALUES ('{"did": "did:plc:123", "kind": "commit"}');

-- Вставка из файла (если поддерживается)
COPY bluesky FROM '/path/to/file.json' FORMAT JSON;
```

### Запросы к JSON

#### Доступ к полям

**Нужно выяснить какой синтаксис поддерживается:**

```sql
-- Вариант 1: Точечная нотация (ClickHouse-style)
SELECT data.did FROM bluesky;
SELECT data.commit.collection FROM bluesky;

-- Вариант 2: Стрелочная нотация (PostgreSQL-style)
SELECT data->'commit'->'collection' FROM bluesky;
SELECT data->>'did' FROM bluesky;

-- Вариант 3: JSON Path (DuckDB-style)
SELECT data->>'$.did' FROM bluesky;
SELECT data->>'$.commit.collection' FROM bluesky;

-- Вариант 4: Функция
SELECT JSON_EXTRACT(data, '$.did') FROM bluesky;
```

**Тестовый запрос для проверки:**
```sql
-- Создать тестовую таблицу
CREATE TABLE test (j JSON);

-- Вставить тестовые данные
INSERT INTO test VALUES ('{"a": {"b": {"c": 123}}}');

-- Проверить какой синтаксис работает
SELECT j.a.b.c FROM test;           -- Вариант 1
SELECT j->'a'->'b'->'c' FROM test;  -- Вариант 2  
SELECT j->>'$.a.b.c' FROM test;     -- Вариант 3
SELECT JSON_EXTRACT(j, '$.a.b.c') FROM test;  -- Вариант 4
```

### Агрегатные функции

```sql
-- COUNT
SELECT COUNT(*) FROM bluesky;
SELECT COUNT() FROM bluesky;  -- Если поддерживается ClickHouse-style

-- COUNT DISTINCT
SELECT COUNT(DISTINCT data.did) FROM bluesky;

-- MIN, MAX
SELECT MIN(data.time_us), MAX(data.time_us) FROM bluesky;

-- GROUP BY
SELECT data.commit.collection, COUNT(*) 
FROM bluesky 
GROUP BY data.commit.collection;
```

### Функции даты и времени

**Нужно выяснить какие функции поддерживаются:**

```sql
-- Конвертация микросекунд в timestamp
SELECT FROM_UNIXTIME(data.time_us / 1000000) FROM bluesky;
SELECT TO_TIMESTAMP(data.time_us / 1000000) FROM bluesky;
SELECT fromUnixTimestamp64Micro(data.time_us) FROM bluesky;  -- ClickHouse

-- Извлечение компонентов даты
SELECT HOUR(timestamp_column) FROM bluesky;
SELECT EXTRACT(HOUR FROM timestamp_column) FROM bluesky;
SELECT toHour(timestamp_column) FROM bluesky;  -- ClickHouse

-- Разница между датами
SELECT DATEDIFF('millisecond', ts1, ts2) FROM bluesky;
SELECT TIMESTAMPDIFF(MILLISECOND, ts1, ts2) FROM bluesky;
SELECT date_diff('milliseconds', ts1, ts2) FROM bluesky;  -- ClickHouse
```

### Приведение типов

```sql
-- К строке
SELECT CAST(data.did AS VARCHAR) FROM bluesky;
SELECT data.did::String FROM bluesky;

-- К числу
SELECT CAST(data.time_us AS BIGINT) FROM bluesky;
SELECT data.time_us::BIGINT FROM bluesky;
```

### Фильтрация

```sql
-- WHERE с условиями
SELECT * FROM bluesky 
WHERE data.kind = 'commit' 
  AND data.commit.operation = 'create';

-- IN clause
SELECT * FROM bluesky 
WHERE data.commit.collection IN ('app.bsky.feed.post', 'app.bsky.feed.like');
```

### Сортировка и лимиты

```sql
-- ORDER BY
SELECT data.did, COUNT(*) as cnt 
FROM bluesky 
GROUP BY data.did 
ORDER BY cnt DESC;

-- LIMIT
SELECT * FROM bluesky LIMIT 10;
```

## Индексы

**Нужно выяснить синтаксис создания индексов:**

```sql
-- Вариант 1: Обычный индекс
CREATE INDEX idx_kind ON bluesky(data.kind);

-- Вариант 2: Составной индекс на JSON-поля
CREATE INDEX idx_commit ON bluesky(
    data.kind,
    data.commit.operation,
    data.commit.collection
);

-- Вариант 3: Индекс с использованием функций
CREATE INDEX idx_time ON bluesky(
    TO_TIMESTAMP(data.time_us / 1000000)
);

-- Вариант 4: Сортировка таблицы (кластерный индекс, ClickHouse-style)
CREATE TABLE bluesky (
    data JSON
) 
ENGINE = MergeTree 
ORDER BY (data.kind, data.commit.collection);
```

## Полезные системные запросы

### Информация о базах данных

```sql
-- Список баз данных
SHOW DATABASES;

-- Текущая база данных
SELECT DATABASE();
```

### Информация о таблицах

```sql
-- Список таблиц
SHOW TABLES;

-- Схема таблицы
DESCRIBE bluesky;
DESC bluesky;
SHOW CREATE TABLE bluesky;
```

### Статистика

```sql
-- Размер таблицы
SELECT 
    table_name,
    table_size,
    data_size,
    index_size
FROM system.tables 
WHERE table_name = 'bluesky';

-- Количество строк
SELECT COUNT(*) FROM bluesky;
```

### EXPLAIN

```sql
-- План выполнения
EXPLAIN SELECT * FROM bluesky WHERE data.kind = 'commit';

-- Детальный план
EXPLAIN ANALYZE SELECT COUNT(*) FROM bluesky;
```

## Примеры для JSONBench

### Пример скрипта загрузки данных (load_data.py)

```python
#!/usr/bin/env python3
import gzip
import json
import sys
from otterbrix import Client

def load_data(data_directory, db_name, table_name, max_files):
    """
    Загрузка данных из NDJSON файлов в Otterbrix.
    
    Args:
        data_directory: Путь к директории с .json.gz файлами
        db_name: Имя базы данных
        table_name: Имя таблицы
        max_files: Максимальное количество файлов для загрузки
    """
    # Создание клиента
    client = Client()
    
    # Создание БД и таблицы через SQL
    client.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
    client.execute(f"USE {db_name}")
    client.execute(f"CREATE TABLE IF NOT EXISTS {table_name} (data JSON)")
    
    # Или через API
    db = client[db_name]
    collection = db[table_name]
    
    # Загрузка файлов
    import glob
    files = sorted(glob.glob(f"{data_directory}/*.json.gz"))[:max_files]
    
    for file_path in files:
        print(f"Loading {file_path}...")
        
        batch = []
        batch_size = 10000
        
        with gzip.open(file_path, 'rt', encoding='utf-8') as f:
            for line in f:
                try:
                    doc = json.loads(line)
                    batch.append(doc)
                    
                    if len(batch) >= batch_size:
                        # Вставка через SQL
                        for doc in batch:
                            json_str = json.dumps(doc)
                            client.execute(
                                f"INSERT INTO {table_name} VALUES ('{json_str}')"
                            )
                        
                        # Или через API (если поддерживается)
                        # collection.insert_many(batch)
                        
                        batch = []
                        
                except json.JSONDecodeError as e:
                    print(f"Error parsing JSON: {e}", file=sys.stderr)
                    continue
        
        # Вставка остатка
        if batch:
            for doc in batch:
                json_str = json.dumps(doc)
                client.execute(
                    f"INSERT INTO {table_name} VALUES ('{json_str}')"
                )
    
    print("Data loading completed")

if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Usage: load_data.py <data_dir> <db_name> <table_name> <max_files>")
        sys.exit(1)
    
    load_data(sys.argv[1], sys.argv[2], sys.argv[3], int(sys.argv[4]))
```

### Пример выполнения запросов (run_queries.py)

```python
#!/usr/bin/env python3
import sys
import time
from otterbrix import Client

def run_queries(db_name, queries_file):
    """
    Выполнение запросов с измерением времени.
    
    Args:
        db_name: Имя базы данных
        queries_file: Файл с запросами (один запрос на строку)
    """
    client = Client()
    client.execute(f"USE {db_name}")
    
    # Чтение запросов
    with open(queries_file, 'r') as f:
        queries = [line.strip() for line in f if line.strip()]
    
    # Выполнение запросов
    for i, query in enumerate(queries, 1):
        print(f"Running query {i}: {query[:60]}...")
        
        # 3 прогона: 1 cold + 2 hot
        times = []
        for run in range(3):
            # Очистка кеша (если возможно)
            # ...
            
            start = time.time()
            cursor = client.execute(query)
            results = cursor.fetchall()  # Важно: получить все результаты
            cursor.close()
            end = time.time()
            
            elapsed = end - start
            times.append(elapsed)
            print(f"  Run {run + 1}: {elapsed:.3f} seconds")
        
        print(f"Query {i} times: {times}")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: run_queries.py <db_name> <queries_file>")
        sys.exit(1)
    
    run_queries(sys.argv[1], sys.argv[2])
```

## Тестирование API

### Скрипт для проверки возможностей

```python
#!/usr/bin/env python3
"""
Скрипт для проверки возможностей Otterbrix API.
Запустить перед началом интеграции.
"""

from otterbrix import Client
import json

def test_basic_operations():
    """Тест базовых операций"""
    print("Testing basic operations...")
    
    client = Client()
    
    # Создание БД
    db = client["test_db"]
    print("✓ Database creation")
    
    # Создание коллекции
    collection = db["test_collection"]
    print("✓ Collection creation")
    
    # Вставка документа
    collection.insert({"test": "value"})
    print("✓ Single insert")
    
    # Вставка множества
    collection.insert_many([
        {"a": 1, "b": {"c": 2}},
        {"a": 3, "b": {"c": 4}}
    ])
    print("✓ Batch insert")
    
    # SQL запрос
    cursor = client.execute("SELECT * FROM test_db.test_collection")
    results = cursor.fetchall()
    print(f"✓ SQL query (got {len(results)} rows)")
    cursor.close()

def test_json_access():
    """Тест доступа к JSON полям"""
    print("\nTesting JSON field access...")
    
    client = Client()
    db = client["test_db"]
    
    # Создать тестовые данные
    client.execute("CREATE TABLE test_json (data JSON)")
    client.execute("""
        INSERT INTO test_json VALUES ('{"did": "user123", "commit": {"collection": "posts"}}')
    """)
    
    # Тестировать разные синтаксисы
    syntaxes = [
        ("Dot notation", "SELECT data.did FROM test_json"),
        ("Arrow notation", "SELECT data->'did' FROM test_json"),
        ("JSON path", "SELECT data->>'$.did' FROM test_json"),
        ("JSON function", "SELECT JSON_EXTRACT(data, '$.did') FROM test_json"),
    ]
    
    for name, query in syntaxes:
        try:
            cursor = client.execute(query)
            result = cursor.fetchall()
            cursor.close()
            print(f"✓ {name}: {result}")
        except Exception as e:
            print(f"✗ {name}: {e}")

def test_aggregations():
    """Тест агрегатных функций"""
    print("\nTesting aggregations...")
    
    client = Client()
    
    tests = [
        ("COUNT(*)", "SELECT COUNT(*) FROM test_json"),
        ("COUNT()", "SELECT COUNT() FROM test_json"),
        ("COUNT DISTINCT", "SELECT COUNT(DISTINCT data.did) FROM test_json"),
        ("MIN/MAX", "SELECT MIN(data.time), MAX(data.time) FROM test_json"),
        ("GROUP BY", "SELECT data.did, COUNT(*) FROM test_json GROUP BY data.did"),
    ]
    
    for name, query in tests:
        try:
            cursor = client.execute(query)
            result = cursor.fetchall()
            cursor.close()
            print(f"✓ {name}")
        except Exception as e:
            print(f"✗ {name}: {e}")

def test_datetime_functions():
    """Тест функций даты/времени"""
    print("\nTesting datetime functions...")
    
    client = Client()
    
    # Вставить тестовые данные с timestamp
    client.execute("""
        INSERT INTO test_json VALUES ('{"time_us": 1234567890123456}')
    """)
    
    tests = [
        ("FROM_UNIXTIME", "SELECT FROM_UNIXTIME(data.time_us / 1000000) FROM test_json"),
        ("TO_TIMESTAMP", "SELECT TO_TIMESTAMP(data.time_us / 1000000) FROM test_json"),
        ("HOUR", "SELECT HOUR(TO_TIMESTAMP(data.time_us / 1000000)) FROM test_json"),
        ("EXTRACT", "SELECT EXTRACT(HOUR FROM TO_TIMESTAMP(data.time_us / 1000000)) FROM test_json"),
        ("DATEDIFF", "SELECT DATEDIFF('millisecond', NOW(), NOW()) FROM test_json"),
    ]
    
    for name, query in tests:
        try:
            cursor = client.execute(query)
            result = cursor.fetchall()
            cursor.close()
            print(f"✓ {name}: {result}")
        except Exception as e:
            print(f"✗ {name}: {e}")

if __name__ == "__main__":
    try:
        test_basic_operations()
        test_json_access()
        test_aggregations()
        test_datetime_functions()
        print("\n✓ All tests completed")
    except Exception as e:
        print(f"\n✗ Tests failed: {e}")
```

## Важные заметки

### Особенности Otterbrix

1. **Гибридная модель хранения**: Otterbrix использует комбинацию Arrow и tuple форматов
2. **Schema-free**: Не требует предварительного определения схемы
3. **Embedded**: Может работать как встроенная БД
4. **C++ движок**: Высокая производительность на низком уровне

### Потенциальные проблемы

1. **Производительность вставки**: Batch-вставки критически важны для больших объемов
2. **Управление памятью**: На 1B записей может потребоваться много памяти
3. **SQL-диалект**: Может отличаться от стандартных БД
4. **Индексы**: Нужно понять как создавать и использовать индексы

### Следующие шаги

1. Запустить `test_api.py` для проверки возможностей
2. Определить точный синтаксис для JSON-запросов
3. Проверить поддержку datetime-функций
4. Создать прототип загрузки данных на 1000 записей
5. Написать 5 запросов для JSONBench

