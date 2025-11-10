# Getting Started with Otterbrix

Это руководство поможет вам начать работу с Otterbrix - от установки зависимостей до запуска первых запросов.

## Содержание

- [Быстрый старт (пользователи)](#быстрый-старт-пользователи)
- [Разработка (contributors)](#разработка-contributors)
- [Использование API](#использование-api)
- [Примеры работы](#примеры-работы)

---

## Быстрый старт (пользователи)

Если вы хотите просто использовать Otterbrix в своих проектах, следуйте этим шагам.

### Установка через PyPI

```bash
pip install "otterbrix==1.0.1a9"
```

### Первый запрос

```python
from otterbrix import Client

# Создание клиента
client = Client()

# Выполнение SQL запроса
result = client.execute("SELECT * FROM schema.table WHERE count = 1000;")

# Обработка результатов
for row in result:
    print(row)

# Закрытие соединения
client.close()
```

---

## Разработка (contributors)

Если вы хотите внести вклад в Otterbrix или собрать проект из исходников, следуйте инструкциям ниже.

### Системные требования

- **ОС**: Linux (Ubuntu 20.04+), macOS, Windows (WSL2)
- **Docker**: Последняя версия ([установка](https://docs.docker.com/engine/install/))
- **RAM**: Минимум 8GB, рекомендуется 16GB+
- **Disk**: 10GB свободного места

### Требования для локальной сборки (опционально)

Если вы хотите собирать вне Docker:

- **Компилятор**: GCC 9+ или Clang 10+
- **CMake**: 3.22+
- **Python**: 3.8+
- **Conan**: 2.x (опционально, для управления зависимостями)

### Зависимости проекта

Otterbrix использует следующие библиотеки (управляются через Conan):

| Библиотека | Версия | Назначение |
|-----------|--------|-----------|
| Boost | 1.86.0 | Системные утилиты |
| actor-zeta | latest | Actor framework |
| abseil-cpp | 20230802.1 | Google core libraries |
| fmt | 11.1.3 | Форматирование строк |
| spdlog | 1.15.1 | Логирование |
| pybind11 | 2.10.0 | Python bindings |
| msgpack | 4.1.1 | Сериализация |
| Catch2 | 2.13.7 | Тестирование |
| benchmark | 1.6.1 | Бенчмаркинг |
| magic_enum | 0.8.1 | Enum reflection |
| zlib | 1.2.12 | Компрессия |
| bzip2 | 1.0.8 | Компрессия |

### Клонирование репозитория

```bash
# Клонируем репозиторий
git clone https://github.com/agdev/otterbrix.git
cd otterbrix
```

### Сборка в Docker (рекомендуется)

Otterbrix официально поддерживает сборку в Docker для обеспечения воспроизводимости.

```bash
# Сборка Docker образа
docker build -t otterbrix:latest .

# Запуск контейнера
docker run -it otterbrix:latest
```

**Примечание**: Если вам нужна помощь со сборкой, обратитесь к команде: team@otterbrix.com

### Локальная сборка (для разработчиков)

#### 1. Установка зависимостей (Ubuntu)

```bash
# Обновление системы
sudo apt update && sudo apt upgrade -y

# Установка build tools
sudo apt install -y \
    build-essential \
    cmake \
    git \
    ccache \
    ninja-build \
    python3 \
    python3-pip

# Установка Conan (опционально)
pip3 install conan
```

#### 2. Конфигурация CMake

```bash
# Создание build директории
mkdir build && cd build

# Конфигурация CMake (Release)
cmake .. \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_CXX_COMPILER=g++ \
    -GNinja

# Конфигурация CMake (Debug с тестами)
cmake .. \
    -DCMAKE_BUILD_TYPE=Debug \
    -DDEV_MODE=ON \
    -DEXAMPLE=ON \
    -GNinja
```

**Опции сборки**:

| Опция | Значение | Описание |
|-------|---------|----------|
| `CMAKE_BUILD_TYPE` | Release/Debug | Тип сборки |
| `DEV_MODE` | ON/OFF | Режим разработки (включает тесты) |
| `ENABLE_ASAN` | ON/OFF | Address Sanitizer |
| `EXAMPLE` | ON/OFF | Сборка примеров |
| `ALLOW_BENCHMARK` | ON/OFF | Сборка бенчмарков |
| `APPLICATION_SERVER` | ON/OFF | Сборка application server |

#### 3. Компиляция

```bash
# Сборка проекта
ninja

# Или с использованием make
# make -j$(nproc)

# Сборка конкретной цели
ninja otterbrix_core
ninja otterbrix_tests
```

#### 4. Запуск тестов

```bash
# Запуск всех тестов
ctest --output-on-failure

# Запуск конкретного теста
./components/tests/vector_tests
./components/tests/table_tests
```

#### 5. Установка (опционально)

```bash
# Установка в систему
sudo ninja install

# Или установка в custom директорию
cmake .. -DCMAKE_INSTALL_PREFIX=/path/to/install
ninja install
```

### Структура сборки

После успешной сборки структура `build/` выглядит так:

```
build/
├── core/
│   └── libotterbrix_core.a
├── components/
│   ├── libotterbrix_sql.a
│   ├── libotterbrix_table.a
│   ├── libotterbrix_vector.a
│   └── tests/
│       ├── vector_tests
│       ├── table_tests
│       └── ...
├── services/
│   └── libotterbrix_services.a
└── integration/
    └── otterbrix_main
```

---

## Использование API

### Python API

#### Установка и подключение

```python
from otterbrix import Client

# Создание клиента с параметрами по умолчанию
client = Client()

# Создание клиента с кастомными параметрами
client = Client(
    host='localhost',
    port=5432,
    database='mydb'
)
```

#### Выполнение запросов

```python
# SELECT запрос
result = client.execute("SELECT id, name, age FROM users WHERE age > 18")

# Итерация по результатам
for row in result:
    print(f"ID: {row['id']}, Name: {row['name']}, Age: {row['age']}")

# INSERT запрос
client.execute("""
    INSERT INTO users (name, age, email)
    VALUES ('Alice', 25, 'alice@example.com')
""")

# UPDATE запрос
client.execute("""
    UPDATE users
    SET age = 26
    WHERE name = 'Alice'
""")

# DELETE запрос
client.execute("DELETE FROM users WHERE age < 18")
```

#### Параметризованные запросы

```python
# Безопасная подстановка параметров
client.execute(
    "SELECT * FROM users WHERE age > $1 AND city = $2",
    params=[18, "New York"]
)
```

#### Транзакции

```python
# Начало транзакции
client.begin()

try:
    client.execute("INSERT INTO accounts (id, balance) VALUES (1, 1000)")
    client.execute("INSERT INTO accounts (id, balance) VALUES (2, 2000)")

    # Commit транзакции
    client.commit()
except Exception as e:
    # Rollback при ошибке
    client.rollback()
    print(f"Transaction failed: {e}")
```

#### Создание таблиц и схем

```python
# Создание database
client.execute("CREATE DATABASE mydb")

# Создание schema
client.execute("CREATE SCHEMA myschema")

# Создание таблицы
client.execute("""
    CREATE TABLE users (
        id INTEGER PRIMARY KEY,
        name VARCHAR,
        age INTEGER,
        email VARCHAR,
        metadata JSON
    )
""")

# Создание индекса
client.execute("CREATE INDEX idx_users_age ON users(age)")
```

### C++ API

#### Подключение библиотеки

```cpp
#include <otterbrix/client.hpp>
#include <otterbrix/result.hpp>

using namespace components;
```

#### Выполнение запросов

```cpp
// Создание клиента
auto client = std::make_unique<client_t>();

// Выполнение запроса
auto result = client->execute("SELECT * FROM users WHERE age > 18");

// Обработка результатов
while (result->next()) {
    int id = result->get_int("id");
    std::string name = result->get_string("name");
    int age = result->get_int("age");

    std::cout << "ID: " << id
              << ", Name: " << name
              << ", Age: " << age << std::endl;
}
```

#### Prepared Statements

```cpp
// Подготовка statement
auto stmt = client->prepare("SELECT * FROM users WHERE age > $1");

// Выполнение с параметрами
auto result = stmt->execute(18);
```

---

## Примеры работы

### Пример 1: Создание и заполнение таблицы

```python
from otterbrix import Client

client = Client()

# Создание таблицы
client.execute("""
    CREATE TABLE products (
        id INTEGER,
        name VARCHAR,
        price DOUBLE,
        category VARCHAR,
        tags JSON
    )
""")

# Вставка данных
client.execute("""
    INSERT INTO products (id, name, price, category, tags) VALUES
        (1, 'Laptop', 999.99, 'Electronics', '["tech", "computer"]'),
        (2, 'Mouse', 29.99, 'Electronics', '["tech", "accessory"]'),
        (3, 'Desk', 299.99, 'Furniture', '["office", "wood"]'),
        (4, 'Chair', 199.99, 'Furniture', '["office", "ergonomic"]')
""")

# Запрос данных
result = client.execute("""
    SELECT name, price, category
    FROM products
    WHERE price > 100
    ORDER BY price DESC
""")

for row in result:
    print(f"{row['name']}: ${row['price']} ({row['category']})")

client.close()
```

**Вывод**:
```
Laptop: $999.99 (Electronics)
Desk: $299.99 (Furniture)
Chair: $199.99 (Furniture)
```

### Пример 2: Работа с JSON данными

```python
client = Client()

# Создание таблицы с JSON колонкой
client.execute("""
    CREATE TABLE events (
        event_id INTEGER,
        event_type VARCHAR,
        payload JSON
    )
""")

# Вставка JSON данных
client.execute("""
    INSERT INTO events (event_id, event_type, payload) VALUES
        (1, 'user_login', '{"user_id": 123, "ip": "192.168.1.1", "timestamp": "2025-01-10T10:00:00Z"}'),
        (2, 'purchase', '{"user_id": 123, "product_id": 456, "amount": 99.99}'),
        (3, 'user_logout', '{"user_id": 123, "duration_seconds": 3600}')
""")

# Запрос с фильтрацией по JSON полю
result = client.execute("""
    SELECT event_id, event_type, payload
    FROM events
    WHERE payload->>'user_id' = '123'
""")

for row in result:
    print(f"Event {row['event_id']}: {row['event_type']}")
    print(f"  Payload: {row['payload']}")
```

### Пример 3: Агрегация и группировка

```python
client = Client()

# Создание таблицы заказов
client.execute("""
    CREATE TABLE orders (
        order_id INTEGER,
        customer_name VARCHAR,
        product VARCHAR,
        quantity INTEGER,
        price DOUBLE
    )
""")

# Вставка данных
client.execute("""
    INSERT INTO orders (order_id, customer_name, product, quantity, price) VALUES
        (1, 'Alice', 'Laptop', 1, 999.99),
        (2, 'Alice', 'Mouse', 2, 29.99),
        (3, 'Bob', 'Laptop', 1, 999.99),
        (4, 'Bob', 'Keyboard', 1, 79.99),
        (5, 'Alice', 'Monitor', 1, 299.99)
""")

# Агрегация: общая сумма заказов по клиентам
result = client.execute("""
    SELECT
        customer_name,
        COUNT(*) as order_count,
        SUM(quantity * price) as total_amount
    FROM orders
    GROUP BY customer_name
    HAVING SUM(quantity * price) > 1000
    ORDER BY total_amount DESC
""")

for row in result:
    print(f"{row['customer_name']}: {row['order_count']} orders, "
          f"total: ${row['total_amount']:.2f}")
```

**Вывод**:
```
Alice: 3 orders, total: $1359.96
```

### Пример 4: JOIN запросы

```python
client = Client()

# Создание таблиц
client.execute("""
    CREATE TABLE customers (
        customer_id INTEGER,
        name VARCHAR,
        city VARCHAR
    )
""")

client.execute("""
    CREATE TABLE orders (
        order_id INTEGER,
        customer_id INTEGER,
        product VARCHAR,
        amount DOUBLE
    )
""")

# Вставка данных
client.execute("""
    INSERT INTO customers (customer_id, name, city) VALUES
        (1, 'Alice', 'New York'),
        (2, 'Bob', 'Los Angeles'),
        (3, 'Charlie', 'Chicago')
""")

client.execute("""
    INSERT INTO orders (order_id, customer_id, product, amount) VALUES
        (1, 1, 'Laptop', 999.99),
        (2, 1, 'Mouse', 29.99),
        (3, 2, 'Keyboard', 79.99),
        (4, 3, 'Monitor', 299.99)
""")

# JOIN запрос
result = client.execute("""
    SELECT
        c.name,
        c.city,
        COUNT(o.order_id) as order_count,
        SUM(o.amount) as total_spent
    FROM customers c
    LEFT JOIN orders o ON c.customer_id = o.customer_id
    GROUP BY c.name, c.city
    ORDER BY total_spent DESC NULLS LAST
""")

for row in result:
    print(f"{row['name']} ({row['city']}): "
          f"{row['order_count']} orders, ${row['total_spent']:.2f}")
```

### Пример 5: Полуструктурированные данные

```python
import json

client = Client()

# Загрузка JSON данных напрямую
client.execute("""
    CREATE TABLE logs (
        log_id INTEGER,
        timestamp VARCHAR,
        level VARCHAR,
        message VARCHAR,
        metadata JSON
    )
""")

# Импорт JSON файла (концептуально)
logs_data = [
    {
        "log_id": 1,
        "timestamp": "2025-01-10T10:00:00Z",
        "level": "INFO",
        "message": "User logged in",
        "metadata": {"user_id": 123, "ip": "192.168.1.1"}
    },
    {
        "log_id": 2,
        "timestamp": "2025-01-10T10:05:00Z",
        "level": "ERROR",
        "message": "Database connection failed",
        "metadata": {"error_code": 500, "retry_count": 3}
    }
]

for log in logs_data:
    client.execute(
        """
        INSERT INTO logs (log_id, timestamp, level, message, metadata)
        VALUES ($1, $2, $3, $4, $5)
        """,
        params=[
            log["log_id"],
            log["timestamp"],
            log["level"],
            log["message"],
            json.dumps(log["metadata"])
        ]
    )

# Запрос по вложенным JSON полям
result = client.execute("""
    SELECT log_id, level, message, metadata
    FROM logs
    WHERE metadata->>'error_code' IS NOT NULL
""")

for row in result:
    print(f"[{row['level']}] {row['message']}")
    print(f"  Metadata: {row['metadata']}")
```

---

## Отладка и профилирование

### Включение Debug логов

```python
import logging

# Настройка логирования
logging.basicConfig(level=logging.DEBUG)

client = Client(debug=True)
```

### Профилирование запросов

```python
# EXPLAIN для анализа плана запроса
result = client.execute("""
    EXPLAIN SELECT * FROM users WHERE age > 18
""")

for row in result:
    print(row)
```

### Address Sanitizer (для разработчиков)

```bash
# Сборка с ASAN
mkdir build-asan && cd build-asan
cmake .. -DCMAKE_BUILD_TYPE=Debug -DENABLE_ASAN=ON
ninja

# Запуск тестов с ASAN
ctest --output-on-failure
```

---

## Troubleshooting

### Проблема: Ошибка компиляции

**Решение**:
1. Проверьте версию компилятора: `g++ --version` или `clang++ --version`
2. Убедитесь, что все зависимости установлены
3. Очистите build директорию: `rm -rf build && mkdir build`
4. Попробуйте сборку в Docker

### Проблема: Тесты падают

**Решение**:
1. Запустите тесты с verbose: `ctest --output-on-failure --verbose`
2. Запустите конкретный тест напрямую: `./components/tests/vector_tests`
3. Проверьте логи в `/tmp/otterbrix/`

### Проблема: Out of Memory

**Решение**:
1. Увеличьте лимит памяти для Docker
2. Используйте swap файл
3. Соберите в Release режиме (меньше потребление памяти)

### Проблема: Python bindings не работают

**Решение**:
1. Убедитесь, что pybind11 установлен: `pip3 install pybind11`
2. Проверьте Python версию: `python3 --version` (требуется 3.8+)
3. Переустановите пакет: `pip3 install --force-reinstall otterbrix`

---

## Дальнейшие шаги

После успешной установки и первых экспериментов:

1. Изучите [ARCHITECTURE.md](ARCHITECTURE.md) для понимания внутреннего устройства
2. Ознакомьтесь с [COMPONENTS.md](COMPONENTS.md) для детального описания компонентов
3. Посмотрите примеры в `examples/` директории
4. Прочитайте [CONTRIBUTING.md](https://github.com/agdev/otterbrix/blob/main/CONTRIBUTING.md) если хотите внести вклад

---

## Поддержка

Если у вас возникли проблемы:
- Создайте issue на GitHub: https://github.com/agdev/otterbrix/issues
- Напишите команде: team@otterbrix.com

Добро пожаловать в сообщество Otterbrix!
