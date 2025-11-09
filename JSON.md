# Реализация типа JSON для Otterbrix

## Обзор

Этот документ описывает прототип реализации типа данных JSON для колоночной базы данных Otterbrix.

### Основная идея

Вместо того чтобы хранить JSON как строку или бинарный blob, мы храним его в **вспомогательной таблице** с разложенной структурой. Это позволит в будущем эффективно делать запросы по содержимому JSON.

### Ограничения прототипа

- **Только простые JSON объекты** - без вложенных объектов (только `{"key": value}`)
- **Только INTEGER значения** - пока поддерживаем только целочисленные значения
- **Только операция read()** - сборка JSON обратно в строку

---

## Архитектура решения

### Структура вспомогательной таблицы

Для каждой колонки типа JSON создается отдельная вспомогательная таблица:

```
Имя таблицы: __json_<table_name>_<column_name>

Схема:
┌─────────────┬──────────┬──────────────────────────────────────────┐
│ id          │ BIGINT   │ PRIMARY KEY, автоинкремент              │
│ json_id     │ BIGINT   │ NOT NULL, идентификатор JSON объекта    │
│ full_path   │ VARCHAR  │ NOT NULL, путь к полю ("age", "count")  │
│ int_value   │ INTEGER  │ значение поля                           │
└─────────────┴──────────┴──────────────────────────────────────────┘
```

### Пример работы

**Основная таблица:**
```sql
CREATE TABLE users (
    id BIGINT,
    name VARCHAR,
    data JSON  -- <- наша JSON колонка
);
```

**При INSERT:**
```sql
INSERT INTO users VALUES (1, 'Alice', '{"age": 25, "score": 100}');
```

**Автоматически создаются записи в `__json_users_data`:**
```
┌────┬─────────┬───────────┬───────────┐
│ id │ json_id │ full_path │ int_value │
├────┼─────────┼───────────┼───────────┤
│ 1  │ 1       │ "age"     │ 25        │
│ 2  │ 1       │ "score"   │ 100       │
└────┴─────────┴───────────┴───────────┘
```

**В основной таблице `users` хранится только `json_id`:**
```
┌────┬───────┬─────────┐
│ id │ name  │ data    │
├────┼───────┼─────────┤
│ 1  │ Alice │ 1       │  <- это json_id
└────┴───────┴─────────┘
```

**При SELECT:**
```sql
SELECT * FROM users WHERE id = 1;
```

Система автоматически:
1. Читает `json_id = 1` из основной таблицы
2. Ищет все записи в `__json_users_data` где `json_id = 1`
3. Собирает их обратно в JSON строку: `{"age": 25, "score": 100}`

---

## Что было реализовано

### 1. Добавлен новый логический тип JSON

**Файл:** `components/types/types.hpp`

#### Изменение 1: Добавлен enum для JSON типа

```cpp
enum class logical_type : uint8_t
{
    // ... существующие типы ...
    UNION = 107,
    ARRAY = 108,
    JSON = 109,  // <- ДОБАВЛЕНО: новый тип JSON

    UNKNOWN = 127,
    INVALID = 255
};
```

**Объяснение:**
- `enum class` - это перечисление (список возможных значений)
- `logical_type` - это высокоуровневый тип, который видят пользователи
- Мы добавили новое значение `JSON = 109`
- Каждый тип имеет уникальный номер для идентификации

#### Изменение 2: Добавлен extension_type для JSON

```cpp
class logical_type_extension {
public:
    enum class extension_type : uint8_t
    {
        // ... существующие типы ...
        FUNCTION = 8,
        JSON = 9  // <- ДОБАВЛЕНО
    };
    // ...
};
```

**Объяснение:**
- Сложные типы (struct, array, map, json) имеют дополнительную информацию
- `extension_type` указывает, какой именно сложный тип используется
- Это нужно для правильного копирования и сериализации типа

#### Изменение 3: Создан класс json_logical_type_extension

```cpp
class json_logical_type_extension : public logical_type_extension {
public:
    json_logical_type_extension();
    explicit json_logical_type_extension(std::string auxiliary_table_name);

    const std::string& auxiliary_table_name() const noexcept { return auxiliary_table_name_; }
    void set_auxiliary_table_name(const std::string& name) { auxiliary_table_name_ = name; }

private:
    std::string auxiliary_table_name_;  // Имя вспомогательной таблицы
};
```

**Объяснение:**
- `class ... : public logical_type_extension` - наследование от базового класса
- `explicit` - запрещает неявные преобразования типов (безопасность)
- `const noexcept` - метод не изменяет объект и не бросает исключений
- `auxiliary_table_name_` - хранит имя вспомогательной таблицы (например, `__json_users_data`)

**Зачем нужен этот класс:**
- Хранит имя вспомогательной таблицы для данной JSON колонки
- Позволяет системе знать, где искать данные для этого JSON поля

#### Изменение 4: Добавлен фабричный метод create_json

```cpp
class complex_logical_type {
    // ... существующие методы ...

    static complex_logical_type create_json(
        std::string auxiliary_table_name = "",
        std::string alias = ""
    );
};
```

**Объяснение:**
- `static` - метод класса, вызывается без создания объекта
- Фабричный метод - удобный способ создания объектов
- Параметры по умолчанию (`= ""`) делают их опциональными

**Как использовать:**
```cpp
// Создать JSON тип
auto json_type = complex_logical_type::create_json("__json_users_data");
```

### 2. Реализация в types.cpp

**Файл:** `components/types/types.cpp`

#### Изменение 1: Поддержка копирования JSON типов

```cpp
complex_logical_type::complex_logical_type(const complex_logical_type& other)
    : type_(other.type_) {
    if (other.extension_) {
        switch (other.extension_->type()) {
            // ... другие типы ...
            case logical_type_extension::extension_type::JSON:
                extension_ = std::make_unique<json_logical_type_extension>(
                    *static_cast<json_logical_type_extension*>(other.extension_.get())
                );
                break;
            // ...
        }
    }
}
```

**Объяснение:**
- Конструктор копирования вызывается при копировании объекта
- `std::make_unique` - создает умный указатель (автоматическое управление памятью)
- `static_cast` - приведение типа (говорим компилятору, что это именно json_logical_type_extension)
- Аналогичный код добавлен в `operator=` (оператор присваивания)

**Зачем это нужно:**
- В C++ при копировании объектов нужно правильно копировать все поля
- Без этого кода extension не скопируется корректно

#### Изменение 2: Преобразование в физический тип

```cpp
physical_type complex_logical_type::to_physical_type() const {
    switch (type_) {
        // ... другие типы ...
        case logical_type::JSON:
            return physical_type::INT64;  // JSON хранит json_id как BIGINT
        // ...
    }
}
```

**Объяснение:**
- Логический тип - это то, что видит пользователь (JSON)
- Физический тип - это как данные хранятся на диске (INT64)
- JSON колонка физически хранит только `json_id` (64-битное целое число)
- Настоящие данные JSON лежат в отдельной таблице

#### Изменение 3: Реализация фабричного метода

```cpp
complex_logical_type complex_logical_type::create_json(
    std::string auxiliary_table_name,
    std::string alias
) {
    return complex_logical_type(
        logical_type::JSON,
        std::make_unique<json_logical_type_extension>(std::move(auxiliary_table_name)),
        std::move(alias)
    );
}
```

**Объяснение:**
- `std::move` - перемещает данные вместо копирования (эффективность)
- Создаем объект типа `complex_logical_type` с JSON типом и extension

#### Изменение 4: Конструкторы json_logical_type_extension

```cpp
json_logical_type_extension::json_logical_type_extension()
    : logical_type_extension(extension_type::JSON)
    , auxiliary_table_name_() {}

json_logical_type_extension::json_logical_type_extension(std::string auxiliary_table_name)
    : logical_type_extension(extension_type::JSON)
    , auxiliary_table_name_(std::move(auxiliary_table_name)) {}
```

**Объяснение:**
- `: logical_type_extension(extension_type::JSON)` - вызов конструктора базового класса
- `, auxiliary_table_name_()` - инициализация полей класса
- Два конструктора: один без параметров, другой с именем таблицы

---

## Измененные файлы

### 1. components/types/types.hpp

**Что изменилось:**
- Добавлено значение `JSON = 109` в enum `logical_type`
- Добавлено значение `JSON = 9` в enum `extension_type`
- Добавлен класс `json_logical_type_extension`
- Добавлен метод `create_json()` в класс `complex_logical_type`

**Строки:**
- Строка 121: добавлен `JSON = 109`
- Строка 452: добавлен `JSON = 9` в extension_type
- Строки 575-585: добавлен класс `json_logical_type_extension`
- Строка 405: добавлен метод `create_json()`

### 2. components/types/types.cpp

**Что изменилось:**
- Добавлена поддержка копирования JSON типов в конструкторе копирования
- Добавлена поддержка копирования JSON типов в operator=
- Добавлено преобразование JSON → INT64 в `to_physical_type()`
- Реализован метод `create_json()`
- Реализованы конструкторы `json_logical_type_extension`

**Строки:**
- Строки 64-67: поддержка в конструкторе копирования
- Строки 113-116: поддержка в operator=
- Строки 271-272: преобразование в физический тип
- Строки 386-389: реализация create_json()
- Строки 479-485: конструкторы json_logical_type_extension

---

## Что дальше нужно сделать

### 1. Создать JSON Column Data класс

**Файл для создания:** `components/table/json_column_data.hpp` и `.cpp`

**Что нужно реализовать:**
```cpp
class json_column_data_t : public column_data_t {
public:
    json_column_data_t(/* параметры */);

    // Переопределить методы для работы с JSON
    void append(/* данные */) override;  // Вставка JSON
    uint64_t scan(/* параметры */) override;  // Чтение JSON

    // Специальный метод для сборки JSON
    std::string read_json(int64_t json_id);

private:
    std::string auxiliary_table_name_;  // Имя вспомогательной таблицы
    int64_t next_json_id_;  // Счетчик для генерации json_id
};
```

**Что должен делать этот класс:**
- При вставке: разбирать JSON строку на поля и вставлять их в вспомогательную таблицу
- При чтении: собирать JSON обратно из вспомогательной таблицы
- Управлять генерацией уникальных `json_id`

### 2. Интеграция в column_data.cpp

**Файл:** `components/table/column_data.cpp`

**Что добавить в метод `create_column()`:**
```cpp
std::unique_ptr<column_data_t> column_data_t::create_column(...) {
    if (type.to_physical_type() == types::physical_type::STRUCT) {
        return std::make_unique<struct_column_data_t>(...);
    }
    // ... другие типы ...

    // ДОБАВИТЬ:
    else if (type.type() == types::logical_type::JSON) {
        return std::make_unique<json_column_data_t>(...);
    }

    return std::make_unique<standard_column_data_t>(...);
}
```

### 3. Создание вспомогательной таблицы

**Где:** В catalog компоненте или при создании таблицы

**Что нужно:**
- При CREATE TABLE с JSON колонкой автоматически создавать вспомогательную таблицу
- При DROP TABLE удалять вспомогательную таблицу
- Регистрировать связь между основной и вспомогательной таблицей

**Схема вспомогательной таблицы:**
```cpp
auto create_json_auxiliary_table(
    const std::string& main_table_name,
    const std::string& column_name
) {
    std::string aux_table_name = "__json_" + main_table_name + "_" + column_name;

    // Создать таблицу с колонками:
    // - id BIGINT PRIMARY KEY
    // - json_id BIGINT NOT NULL
    // - full_path VARCHAR NOT NULL
    // - int_value INTEGER

    return aux_table_name;
}
```

### 4. Реализация функции read_json()

**Псевдокод:**
```cpp
std::string json_column_data_t::read_json(int64_t json_id) {
    // 1. Выполнить запрос к вспомогательной таблице
    // SELECT full_path, int_value
    // FROM __json_table_name
    // WHERE json_id = ?

    // 2. Собрать результаты в map
    std::map<std::string, int> fields;
    // {"age" -> 25, "score" -> 100}

    // 3. Собрать JSON строку
    std::string result = "{";
    bool first = true;
    for (const auto& [key, value] : fields) {
        if (!first) result += ", ";
        result += "\"" + key + "\": " + std::to_string(value);
        first = false;
    }
    result += "}";

    return result;  // {"age": 25, "score": 100}
}
```

### 5. Парсинг JSON при вставке

**Псевдокод:**
```cpp
void json_column_data_t::append(const std::string& json_string) {
    // 1. Сгенерировать новый json_id
    int64_t json_id = next_json_id_++;

    // 2. Распарсить JSON (простой парсинг для прототипа)
    // Ожидаем формат: {"key1": value1, "key2": value2}
    auto fields = parse_simple_json(json_string);

    // 3. Вставить каждое поле в вспомогательную таблицу
    for (const auto& [key, value] : fields) {
        // INSERT INTO __json_table_name
        // VALUES (NULL, json_id, key, value)
    }

    // 4. Сохранить json_id в основную колонку
    // (это делается через базовый класс column_data_t)
}
```

---

## Диаграмма работы системы

```
┌─────────────────────────────────────────────────────────────────┐
│                     CREATE TABLE users                          │
│                     (id BIGINT, data JSON)                      │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
        ┌────────────────────────────────────────┐
        │ Создается вспомогательная таблица:     │
        │ __json_users_data                      │
        │ (id, json_id, full_path, int_value)    │
        └────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│         INSERT INTO users VALUES (1, '{"age": 25}')            │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
        ┌────────────────────────────────────────┐
        │ 1. Генерируется json_id = 1            │
        │ 2. JSON парсится: {"age": 25}          │
        │ 3. Вставка в __json_users_data:        │
        │    (1, 1, "age", 25)                   │
        │ 4. Вставка в users:                    │
        │    (id=1, data=1)  <- это json_id      │
        └────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│              SELECT data FROM users WHERE id = 1                │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
        ┌────────────────────────────────────────┐
        │ 1. Читаем json_id = 1 из users         │
        │ 2. Запрос к __json_users_data:         │
        │    WHERE json_id = 1                   │
        │ 3. Получаем: [("age", 25)]             │
        │ 4. Собираем JSON: {"age": 25}          │
        │ 5. Возвращаем строку пользователю      │
        └────────────────────────────────────────┘
```

---

## Примеры использования (после полной реализации)

### Создание таблицы с JSON

```sql
CREATE TABLE products (
    id BIGINT,
    name VARCHAR,
    metadata JSON  -- автоматически создаст __json_products_metadata
);
```

### Вставка данных

```sql
INSERT INTO products VALUES
    (1, 'Laptop', '{"price": 1000, "stock": 50}'),
    (2, 'Mouse', '{"price": 25, "stock": 200}');
```

**Что происходит внутри:**

Основная таблица `products`:
```
┌────┬────────┬──────────┐
│ id │ name   │ metadata │
├────┼────────┼──────────┤
│ 1  │ Laptop │ 1        │
│ 2  │ Mouse  │ 2        │
└────┴────────┴──────────┘
```

Вспомогательная таблица `__json_products_metadata`:
```
┌────┬─────────┬───────────┬───────────┐
│ id │ json_id │ full_path │ int_value │
├────┼─────────┼───────────┼───────────┤
│ 1  │ 1       │ price     │ 1000      │
│ 2  │ 1       │ stock     │ 50        │
│ 3  │ 2       │ price     │ 25        │
│ 4  │ 2       │ stock     │ 200       │
└────┴─────────┴───────────┴───────────┘
```

### Чтение данных

```sql
SELECT * FROM products WHERE id = 1;
```

**Результат:**
```
id: 1
name: Laptop
metadata: {"price": 1000, "stock": 50}
```

---

## Преимущества такого подхода

### 1. Эффективные запросы по содержимому JSON (в будущем)

После полной реализации можно будет делать:
```sql
-- Найти все продукты с ценой > 100
SELECT * FROM products
WHERE json_extract(metadata, 'price') > 100;
```

Это превратится в:
```sql
SELECT p.* FROM products p
JOIN __json_products_metadata m ON p.metadata = m.json_id
WHERE m.full_path = 'price' AND m.int_value > 100;
```

### 2. Колоночное хранение для JSON

- Все значения поля `price` лежат в одной колонке - хорошо сжимаются
- Можно строить индексы на JSON поля
- Эффективная фильтрация и агрегация

### 3. Расширяемость

Легко добавить поддержку других типов:
```sql
ALTER TABLE __json_products_metadata
ADD COLUMN string_value VARCHAR,
ADD COLUMN double_value DOUBLE,
ADD COLUMN bool_value BOOLEAN;
```

---

## Недостатки и ограничения

### 1. Накладные расходы

- Каждая JSON колонка требует отдельную таблицу
- JOIN при каждом SELECT для сборки JSON
- Больше места на диске

### 2. Сложность UPDATE

При обновлении JSON нужно:
1. Удалить старые записи из вспомогательной таблицы
2. Вставить новые записи
3. Обновить json_id (или переиспользовать старый)

### 3. Прототип ограничен

- Только простые JSON объекты
- Только INTEGER значения
- Нет вложенности

---

## Следующие шаги для завершения прототипа

1. **Создать json_column_data_t класс** (приоритет: высокий)
   - Наследовать от column_data_t
   - Реализовать append() для вставки
   - Реализовать read_json() для чтения

2. **Добавить простой JSON парсер** (приоритет: высокий)
   - Распознавать формат {"key": value, ...}
   - Извлекать пары ключ-значение
   - Проверять, что значения - целые числа

3. **Интегрировать в column_data.cpp** (приоритет: средний)
   - Добавить в create_column()
   - Обрабатывать JSON тип

4. **Автоматическое создание вспомогательных таблиц** (приоритет: средний)
   - При CREATE TABLE с JSON колонкой
   - Генерировать имя таблицы
   - Создавать с правильной схемой

5. **Тестирование** (приоритет: низкий)
   - Unit-тесты для json_column_data_t
   - Интеграционные тесты с INSERT/SELECT
   - Проверка корректности сборки JSON

---

## Глоссарий терминов

**Логический тип (logical_type)** - тип данных, который видит пользователь (JSON, INTEGER, VARCHAR)

**Физический тип (physical_type)** - как данные хранятся физически на диске (INT64, STRING)

**Extension** - дополнительная информация для сложных типов (например, имя вспомогательной таблицы для JSON)

**Column data** - класс, управляющий хранением данных одной колонки

**json_id** - уникальный идентификатор JSON объекта, по которому все его поля связаны во вспомогательной таблице

**Вспомогательная таблица (auxiliary table)** - отдельная таблица для хранения содержимого JSON в разложенном виде

**Smart pointer (std::unique_ptr)** - умный указатель в C++, автоматически освобождает память

**Factory method (фабричный метод)** - метод для удобного создания объектов (например, create_json())

---

## Заключение

Мы реализовали базовую инфраструктуру типов для поддержки JSON в Otterbrix:

✅ Добавлен логический тип JSON
✅ Добавлен extension класс для хранения метаданных JSON
✅ Реализовано преобразование JSON → INT64 (для хранения json_id)
✅ Добавлены все необходимые методы копирования и создания

Следующий шаг - реализовать json_column_data_t класс, который будет фактически работать с данными.
