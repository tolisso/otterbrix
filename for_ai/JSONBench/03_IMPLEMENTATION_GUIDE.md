# Руководство по реализации интеграции Otterbrix в JSONBench

## Пошаговая инструкция

### Шаг 1: Создание директории otterbrix/

```bash
cd JSONBench/
mkdir otterbrix
cd otterbrix
```

### Шаг 2: Скопировать шаблон из duckdb/

```bash
# Скопировать структуру
cp ../duckdb/*.sh .
cp ../duckdb/*.sql .

# Файлы которые будут изменены:
# - install.sh          (установка Otterbrix)
# - uninstall.sh        (удаление)
# - ddl.sql             (DDL для Otterbrix)
# - queries.sql         (5 запросов)
# - load_data.sh        (загрузка через Python)
# - run_queries.sh      (выполнение через Python)
# - create_and_load.sh  (может потребовать изменений)
# - benchmark.sh        (может потребовать изменений)
# - total_size.sh       (размер БД)
# - count.sh            (подсчет записей)
# - physical_query_plans.sh (планы запросов, если поддерживается)
```

### Шаг 3: Реализовать install.sh

```bash
#!/bin/bash
# otterbrix/install.sh

echo "Setting up Otterbrix from local build..."

# Путь к локальной сборке Otterbrix
OTTERBRIX_ROOT="/home/tolisso/otterbrix"
OTTERBRIX_PYTHON_PATH="$OTTERBRIX_ROOT/build/integration/python"

# Проверка Python
if ! command -v python3 &> /dev/null; then
    echo "Error: Python 3 is required"
    exit 1
fi

# Проверка наличия собранного Otterbrix
if [[ ! -d "$OTTERBRIX_PYTHON_PATH" ]]; then
    echo "Error: Otterbrix Python module not found at $OTTERBRIX_PYTHON_PATH"
    echo "Please build Otterbrix first:"
    echo "  cd $OTTERBRIX_ROOT"
    echo "  mkdir -p build && cd build"
    echo "  conan install ../conanfile.py --build missing -s build_type=Release -s compiler.cppstd=gnu17"
    echo "  cmake .. -G Ninja -DCMAKE_TOOLCHAIN_FILE=./build/Release/generators/conan_toolchain.cmake -DCMAKE_BUILD_TYPE=Release -DDEV_MODE=ON"
    echo "  cmake --build . --target all -- -j \$(nproc)"
    exit 1
fi

# Настройка PYTHONPATH
export PYTHONPATH="$OTTERBRIX_PYTHON_PATH:$PYTHONPATH"

# Проверка установки
if python3 -c "import sys; sys.path.insert(0, '$OTTERBRIX_PYTHON_PATH'); import otterbrix" 2>/dev/null; then
    echo "✓ Otterbrix found and can be imported"
    python3 -c "import sys; sys.path.insert(0, '$OTTERBRIX_PYTHON_PATH'); import otterbrix; print(f'Build path: $OTTERBRIX_PYTHON_PATH')"
else
    echo "✗ Failed to import Otterbrix from local build"
    exit 1
fi

# Создать вспомогательные скрипты
cat > otterbrix_helper.py << 'PYTHON_SCRIPT'
#!/usr/bin/env python3
"""
Вспомогательный модуль для работы с Otterbrix через shell-скрипты.
"""
import sys
import os

# Добавить путь к локальной сборке Otterbrix
OTTERBRIX_PYTHON_PATH = "/home/tolisso/otterbrix/build/integration/python"
if os.path.exists(OTTERBRIX_PYTHON_PATH):
    sys.path.insert(0, OTTERBRIX_PYTHON_PATH)

import json
import time
from otterbrix import Client

def create_client(db_name=None):
    """Создать клиента и подключиться к БД"""
    client = Client()
    if db_name:
        client.execute(f"USE {db_name}")
    return client

def execute_query(db_name, query):
    """Выполнить запрос и вернуть результаты"""
    client = create_client(db_name)
    cursor = client.execute(query)
    results = cursor.fetchall()
    cursor.close()
    return results

def get_count(db_name, table_name):
    """Получить количество записей"""
    query = f"SELECT COUNT(*) FROM {table_name}"
    results = execute_query(db_name, query)
    return results[0][0] if results else 0

def get_size(db_name):
    """Получить размер БД (нужно адаптировать под Otterbrix API)"""
    # TODO: Реализовать получение размера
    return 0

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: otterbrix_helper.py <command> [args...]")
        sys.exit(1)
    
    command = sys.argv[1]
    
    if command == "execute":
        db_name = sys.argv[2]
        query = sys.argv[3]
        results = execute_query(db_name, query)
        print(json.dumps(results))
    
    elif command == "count":
        db_name = sys.argv[2]
        table_name = sys.argv[3]
        count = get_count(db_name, table_name)
        print(count)
    
    elif command == "size":
        db_name = sys.argv[2]
        size = get_size(db_name)
        print(size)
    
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
PYTHON_SCRIPT

chmod +x otterbrix_helper.py

echo "Installation completed"
```

### Шаг 4: Реализовать uninstall.sh

```bash
#!/bin/bash
# otterbrix/uninstall.sh

echo "Cleaning up Otterbrix benchmark files..."

# Удалить вспомогательные файлы
rm -f otterbrix_helper.py

# Удалить временные базы данных (опционально)
# rm -rf ~/.otterbrix
# rm -rf ./otterbrix_*

echo "Cleanup completed"
echo "Note: Local Otterbrix build at /home/tolisso/otterbrix/ was not modified"
```

### Шаг 5: Создать ddl.sql

```sql
-- otterbrix/ddl.sql
-- DDL для создания таблицы в Otterbrix

-- Создать базу данных (если нужно)
-- CREATE DATABASE IF NOT EXISTS bluesky_db;

-- Создать таблицу с JSON
CREATE TABLE bluesky (
    data JSON
);

-- Создать индексы (если поддерживается и нужно для производительности)
-- CREATE INDEX idx_kind ON bluesky(data.kind);
-- CREATE INDEX idx_collection ON bluesky(data.commit.collection);
-- CREATE INDEX idx_did ON bluesky(data.did);
```

**Примечание:** Точный синтаксис зависит от диалекта Otterbrix. Возможно потребуется:
- Использовать `CREATE COLLECTION bluesky` вместо `CREATE TABLE`
- Адаптировать типы данных
- Изменить синтаксис индексов

### Шаг 6: Создать queries.sql

```sql
-- otterbrix/queries.sql
-- 5 стандартных запросов JSONBench для Otterbrix

-- Q1: Группировка по типу события
SELECT data.commit.collection AS event,
       COUNT(*) AS count
FROM bluesky
GROUP BY event
ORDER BY count DESC;

-- Q2: Группировка с COUNT DISTINCT
SELECT data.commit.collection AS event,
       COUNT(*) AS count,
       COUNT(DISTINCT data.did) AS users
FROM bluesky
WHERE data.kind = 'commit'
  AND data.commit.operation = 'create'
GROUP BY event
ORDER BY count DESC;

-- Q3: Группировка по времени суток
SELECT data.commit.collection AS event,
       HOUR(TO_TIMESTAMP(data.time_us / 1000000)) AS hour_of_day,
       COUNT(*) AS count
FROM bluesky
WHERE data.kind = 'commit'
  AND data.commit.operation = 'create'
  AND data.commit.collection IN ('app.bsky.feed.post', 'app.bsky.feed.repost', 'app.bsky.feed.like')
GROUP BY event, hour_of_day
ORDER BY hour_of_day, event;

-- Q4: Первые пользователи
SELECT CAST(data.did AS VARCHAR) AS user_id,
       TO_TIMESTAMP(MIN(data.time_us) / 1000000) AS first_post_date
FROM bluesky
WHERE data.kind = 'commit'
  AND data.commit.operation = 'create'
  AND data.commit.collection = 'app.bsky.feed.post'
GROUP BY user_id
ORDER BY first_post_date ASC
LIMIT 3;

-- Q5: Активность пользователей
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

**Важно:** Синтаксис нужно адаптировать под Otterbrix:
- Проверить синтаксис доступа к JSON (может быть `data->>'commit.collection'` вместо `data.commit.collection`)
- Проверить функции datetime (может быть `fromUnixTimestamp64Micro` вместо `TO_TIMESTAMP`)
- Проверить функцию `DATEDIFF` (может быть `date_diff`)
- Проверить `CAST` (может быть `::String`)

### Шаг 7: Создать load_data.sh

```bash
#!/bin/bash
# otterbrix/load_data.sh

if [[ $# -lt 6 ]]; then
    echo "Usage: $0 <directory> <database_name> <table_name> <max_files> <success_log> <error_log>"
    exit 1
fi

DIRECTORY="$1"
DB_NAME="$2"
TABLE_NAME="$3"
MAX_FILES="$4"
SUCCESS_LOG="$5"
ERROR_LOG="$6"

# Validate arguments
if ! [[ "$MAX_FILES" =~ ^[0-9]+$ ]]; then
    echo "Error: <max_files> must be a positive integer."
    exit 1
fi

[[ ! -d "$DIRECTORY" ]] && { echo "Error: Directory '$DIRECTORY' does not exist."; exit 1; }

# Ensure log files exist
touch "$SUCCESS_LOG" "$ERROR_LOG"

echo "Loading data into Otterbrix..."

# Создать Python-скрипт для загрузки
cat > load_otterbrix.py << 'PYTHON_LOADER'
#!/usr/bin/env python3
import sys
import os

# Добавить путь к локальной сборке Otterbrix
OTTERBRIX_PYTHON_PATH = "/home/tolisso/otterbrix/build/integration/python"
if os.path.exists(OTTERBRIX_PYTHON_PATH):
    sys.path.insert(0, OTTERBRIX_PYTHON_PATH)

import gzip
import json
import glob
from otterbrix import Client

def load_data(directory, db_name, table_name, max_files, success_log, error_log):
    """Загрузка данных из NDJSON файлов"""
    
    # Создать клиента
    client = Client()
    
    # Использовать БД
    try:
        client.execute(f"USE {db_name}")
    except:
        # Если БД не существует, создать
        client.execute(f"CREATE DATABASE {db_name}")
        client.execute(f"USE {db_name}")
    
    # Получить список файлов
    files = sorted(glob.glob(f"{directory}/*.json.gz"))[:max_files]
    
    total_records = 0
    
    for file_path in files:
        print(f"Processing {file_path}...")
        
        batch = []
        batch_size = 10000  # Размер batch для вставки
        file_records = 0
        
        try:
            with gzip.open(file_path, 'rt', encoding='utf-8') as f:
                for line_no, line in enumerate(f, 1):
                    try:
                        doc = json.loads(line.strip())
                        batch.append(doc)
                        
                        if len(batch) >= batch_size:
                            # Вставка batch
                            insert_batch(client, table_name, batch)
                            file_records += len(batch)
                            total_records += len(batch)
                            batch = []
                            
                            if file_records % 100000 == 0:
                                print(f"  Loaded {file_records} records from this file...")
                    
                    except json.JSONDecodeError as e:
                        with open(error_log, 'a') as ef:
                            ef.write(f"JSON error in {file_path}:{line_no}: {e}\n")
                        continue
                
                # Вставить остаток
                if batch:
                    insert_batch(client, table_name, batch)
                    file_records += len(batch)
                    total_records += len(batch)
            
            with open(success_log, 'a') as sf:
                sf.write(f"{file_path}: {file_records} records loaded\n")
            
            print(f"  Loaded {file_records} records from {file_path}")
        
        except Exception as e:
            with open(error_log, 'a') as ef:
                ef.write(f"Error processing {file_path}: {e}\n")
            print(f"  Error processing {file_path}: {e}", file=sys.stderr)
            continue
    
    print(f"\nTotal records loaded: {total_records}")

def insert_batch(client, table_name, batch):
    """Вставка batch записей"""
    
    # Способ 1: Построчная вставка через SQL
    for doc in batch:
        json_str = json.dumps(doc).replace("'", "''")  # Escape quotes
        query = f"INSERT INTO {table_name} VALUES ('{json_str}')"
        try:
            client.execute(query)
        except Exception as e:
            # Если ошибка, попробовать другой способ
            pass
    
    # Способ 2: Через API (если поддерживается)
    # db = client[db_name]
    # collection = db[table_name]
    # collection.insert_many(batch)
    
    # Способ 3: Bulk insert через COPY (если поддерживается)
    # ...

if __name__ == "__main__":
    if len(sys.argv) != 7:
        print("Usage: load_otterbrix.py <dir> <db> <table> <max_files> <success_log> <error_log>")
        sys.exit(1)
    
    load_data(
        sys.argv[1],  # directory
        sys.argv[2],  # db_name
        sys.argv[3],  # table_name
        int(sys.argv[4]),  # max_files
        sys.argv[5],  # success_log
        sys.argv[6]   # error_log
    )
PYTHON_LOADER

# Запустить загрузку
python3 load_otterbrix.py "$DIRECTORY" "$DB_NAME" "$TABLE_NAME" "$MAX_FILES" "$SUCCESS_LOG" "$ERROR_LOG"

# Удалить временный скрипт
rm -f load_otterbrix.py

echo "Data loading completed"
```

### Шаг 8: Создать run_queries.sh

```bash
#!/bin/bash
# otterbrix/run_queries.sh

if [[ $# -lt 1 ]]; then
    echo "Usage: $0 <DB_NAME>"
    exit 1
fi

DB_NAME="$1"

echo "Running queries on database: $DB_NAME"

# Создать Python-скрипт для выполнения запросов
cat > run_otterbrix_queries.py << 'PYTHON_RUNNER'
#!/usr/bin/env python3
import sys
import os

# Добавить путь к локальной сборке Otterbrix
OTTERBRIX_PYTHON_PATH = "/home/tolisso/otterbrix/build/integration/python"
if os.path.exists(OTTERBRIX_PYTHON_PATH):
    sys.path.insert(0, OTTERBRIX_PYTHON_PATH)

import time
from otterbrix import Client

def run_queries(db_name, queries_file="queries.sql"):
    """Выполнение запросов с таймингом"""
    
    client = Client()
    client.execute(f"USE {db_name}")
    
    # Прочитать запросы
    with open(queries_file, 'r') as f:
        queries = []
        current_query = []
        for line in f:
            line = line.strip()
            # Пропустить комментарии
            if line.startswith('--') or not line:
                continue
            current_query.append(line)
            # Конец запроса
            if line.endswith(';'):
                queries.append(' '.join(current_query))
                current_query = []
    
    # Выполнить каждый запрос 3 раза
    TRIES = 3
    
    for i, query in enumerate(queries, 1):
        print(f"\nQuery {i}:")
        print(query[:100] + "..." if len(query) > 100 else query)
        
        for try_num in range(TRIES):
            # Очистка кеша (если возможно)
            # client.execute("SYSTEM DROP CACHE")  # Если поддерживается
            
            start = time.time()
            
            try:
                cursor = client.execute(query)
                # Важно: получить все результаты для корректного измерения
                results = cursor.fetchall()
                cursor.close()
                
                end = time.time()
                elapsed = end - start
                
                print(f"Real time: {elapsed:.6f} seconds")
                
                # Показать несколько строк результата (для отладки)
                if try_num == 0 and len(results) > 0:
                    print(f"Result rows: {len(results)}")
                    if len(results) <= 10:
                        for row in results:
                            print(f"  {row}")
            
            except Exception as e:
                print(f"Error: {e}", file=sys.stderr)
                print("Real time: -1 seconds")  # Индикатор ошибки

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: run_otterbrix_queries.py <db_name> [queries_file]")
        sys.exit(1)
    
    db_name = sys.argv[1]
    queries_file = sys.argv[2] if len(sys.argv) > 2 else "queries.sql"
    
    run_queries(db_name, queries_file)
PYTHON_RUNNER

# Запустить
python3 run_otterbrix_queries.py "$DB_NAME" 2>&1 | tee query_log.txt

# Удалить временный скрипт
rm -f run_otterbrix_queries.py
```

### Шаг 9: Создать другие вспомогательные скрипты

#### count.sh

```bash
#!/bin/bash
# otterbrix/count.sh

if [[ $# -lt 2 ]]; then
    echo "Usage: $0 <DB_NAME> <TABLE_NAME>"
    exit 1
fi

DB_NAME="$1"
TABLE_NAME="$2"

OTTERBRIX_PYTHON_PATH="/home/tolisso/otterbrix/build/integration/python"

python3 -c "
import sys
sys.path.insert(0, '$OTTERBRIX_PYTHON_PATH')
from otterbrix import Client
client = Client()
client.execute('USE $DB_NAME')
cursor = client.execute('SELECT COUNT(*) FROM $TABLE_NAME')
count = cursor.fetchone()[0]
cursor.close()
print(f'Total rows: {count}')
"
```

#### total_size.sh

```bash
#!/bin/bash
# otterbrix/total_size.sh

if [[ $# -lt 2 ]]; then
    echo "Usage: $0 <DB_NAME> <TABLE_NAME>"
    exit 1
fi

DB_NAME="$1"
TABLE_NAME="$2"

# TODO: Реализовать получение размера БД
# Нужно выяснить как Otterbrix предоставляет эту информацию

OTTERBRIX_PYTHON_PATH="/home/tolisso/otterbrix/build/integration/python"

python3 -c "
import sys
sys.path.insert(0, '$OTTERBRIX_PYTHON_PATH')
from otterbrix import Client
import os

client = Client()

# Попробовать получить размер через системные таблицы
try:
    cursor = client.execute('''
        SELECT table_size FROM system.tables 
        WHERE database = '$DB_NAME' AND table = '$TABLE_NAME'
    ''')
    size = cursor.fetchone()[0]
    cursor.close()
    print(f'Total size: {size} bytes')
except:
    # Если не поддерживается, можно попробовать получить размер файлов
    print('Size information not available')
"
```

#### drop_table.sh

```bash
#!/bin/bash
# otterbrix/drop_table.sh

if [[ $# -lt 1 ]]; then
    echo "Usage: $0 <DB_NAME>"
    exit 1
fi

DB_NAME="$1"

OTTERBRIX_PYTHON_PATH="/home/tolisso/otterbrix/build/integration/python"

python3 -c "
import sys
sys.path.insert(0, '$OTTERBRIX_PYTHON_PATH')
from otterbrix import Client
client = Client()
client.execute('DROP DATABASE IF EXISTS $DB_NAME')
print('Database dropped')
"
```

### Шаг 10: Создать benchmark.sh

```bash
#!/bin/bash
# otterbrix/benchmark.sh
# Адаптировать из duckdb/benchmark.sh

if [[ $# -lt 1 ]]; then
    echo "Usage: $0 <DATABASE_NAME> [RESULT_FILE]"
    exit 1
fi

DATABASE_NAME="$1"
RESULT_FILE="${2:-}"

echo "Running queries on database: $DATABASE_NAME"

# Запустить запросы
./run_queries.sh "$DATABASE_NAME" 2>&1 | tee query_log.txt

# Обработать результаты
RESULT=$(cat query_log.txt | grep -oP 'Real time: \d+\.\d+ seconds' | sed -r -e 's/Real time: ([0-9]+\.[0-9]+) seconds/\1/' | \
awk '{ if (i % 3 == 0) { printf "[" }; printf $1; if (i % 3 != 2) { printf "," } else { print "]," }; ++i; }')

if [[ -n "$RESULT_FILE" ]]; then
    echo "$RESULT" > "$RESULT_FILE"
    echo "Result written to $RESULT_FILE"
else
    echo "$RESULT"
fi
```

### Шаг 11: Адаптировать main.sh

Основной скрипт обычно можно оставить похожим на duckdb/main.sh, но с адаптацией путей и имен:

```bash
#!/bin/bash
# otterbrix/main.sh

DEFAULT_CHOICE=ask
DEFAULT_DATA_DIRECTORY=~/data/bluesky

CHOICE="${1:-$DEFAULT_CHOICE}"
DATA_DIRECTORY="${2:-$DEFAULT_DATA_DIRECTORY}"
SUCCESS_LOG="${3:-success.log}"
ERROR_LOG="${4:-error.log}"
OUTPUT_PREFIX="${5:-_m6i.8xlarge}"

if [[ ! -d "$DATA_DIRECTORY" ]]; then
    echo "Error: Data directory '$DATA_DIRECTORY' does not exist."
    exit 1
fi

if [ "$CHOICE" = "ask" ]; then
    echo "Select the dataset size to benchmark:"
    echo "1) 1m (default)"
    echo "2) 10m"
    echo "3) 100m"
    echo "4) 1000m"
    echo "5) all"
    read -p "Enter the number corresponding to your choice: " CHOICE
fi

./install.sh

benchmark() {
    local size=$1
    
    file_count=$(find "$DATA_DIRECTORY" -type f | wc -l)
    if (( file_count < size )); then
        echo "Error: Not enough files in '$DATA_DIRECTORY'. Required: $size, Found: $file_count."
        exit 1
    fi
    
    ./create_and_load.sh "otterbrix_${size}" bluesky ddl.sql "$DATA_DIRECTORY" "$size" "$SUCCESS_LOG" "$ERROR_LOG"
    ./total_size.sh "otterbrix_${size}" bluesky | tee "${OUTPUT_PREFIX}_bluesky_${size}m.data_size"
    ./count.sh "otterbrix_${size}" bluesky | tee "${OUTPUT_PREFIX}_bluesky_${size}m.count"
    ./benchmark.sh "otterbrix_${size}" "${OUTPUT_PREFIX}_bluesky_${size}m.results_runtime"
    ./drop_table.sh "otterbrix_${size}"
}

case $CHOICE in
    2) benchmark 10 ;;
    3) benchmark 100 ;;
    4) benchmark 1000 ;;
    5)
        benchmark 1
        benchmark 10
        benchmark 100
        benchmark 1000
        ;;
    *) benchmark 1 ;;
esac

./uninstall.sh
```

### Шаг 12: Тестирование на малых данных

```bash
# Скачать тестовые данные (1M)
cd JSONBench/
./download_data.sh
# Выбрать опцию 1 (1m)

# Запустить тест
cd otterbrix/
./main.sh 1 ~/data/bluesky

# Проверить результаты
cat _m6i.8xlarge_bluesky_1m.results_runtime
```

## Отладка и решение проблем

### Проблема: SQL синтаксис не работает

**Решение**: Создать тестовый скрипт для проверки синтаксиса:

```python
from otterbrix import Client

client = Client()
client.execute("CREATE DATABASE test")
client.execute("USE test")
client.execute("CREATE TABLE test_json (data JSON)")
client.execute("""INSERT INTO test_json VALUES ('{"a": {"b": 1}}')""")

# Тест разных синтаксисов
syntaxes = [
    "data.a.b",
    "data->>'a.b'",
    "data->'a'->'b'",
    "JSON_EXTRACT(data, '$.a.b')"
]

for syntax in syntaxes:
    try:
        query = f"SELECT {syntax} FROM test_json"
        cursor = client.execute(query)
        result = cursor.fetchall()
        print(f"✓ {syntax}: {result}")
    except Exception as e:
        print(f"✗ {syntax}: {e}")
```

### Проблема: Медленная загрузка данных

**Решения**:
1. Увеличить размер batch
2. Использовать bulk insert (если поддерживается)
3. Отключить индексы во время загрузки
4. Использовать несколько потоков

### Проблема: Out of memory

**Решения**:
1. Уменьшить размер batch
2. Настроить параметры памяти Otterbrix
3. Использовать машину с большей памятью

## Следующие шаги

1. ✅ Создать структуру директории
2. ⏭️ Реализовать все скрипты
3. ⏭️ Протестировать на 1K записей
4. ⏭️ Протестировать на 1M записей
5. ⏭️ Масштабировать на 10M, 100M
6. ⏭️ Провести полный бенчмарк на 1B
7. ⏭️ Создать README.md с документацией
8. ⏭️ Создать PR в JSONBench

