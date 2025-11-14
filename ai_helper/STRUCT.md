# STRUCT - Композитные типы данных в Otterbrix

## Оглавление
1. [Что такое STRUCT](#что-такое-struct)
2. [Архитектура STRUCT](#архитектура-struct)
3. [Реализация struct_column_data_t](#реализация-struct_column_data_t)
4. [Создание и использование STRUCT](#создание-и-использование-struct)
5. [Внутреннее устройство](#внутреннее-устройство)
6. [Операции с STRUCT](#операции-с-struct)
7. [Примеры](#примеры)
8. [Сравнение с другими типами](#сравнение-с-другими-типами)

---

## Что такое STRUCT

### Определение

**STRUCT** (структура) - это **композитный тип данных**, который позволяет группировать несколько полей различных типов в одно логическое значение.

### Аналогии

| Концепция | Аналог в других системах |
|-----------|-------------------------|
| SQL | ROW type, RECORD type |
| C/C++ | struct |
| Python | NamedTuple, dataclass |
| JSON | Object `{"name": "Alice", "age": 25}` |
| PostgreSQL | Composite Type |

### Зачем нужен STRUCT?

1. **Логическая группировка данных**
   ```sql
   -- Вместо:
   CREATE TABLE users (
       id BIGINT,
       address_street VARCHAR,
       address_city VARCHAR,
       address_zip VARCHAR
   );

   -- Можно:
   CREATE TABLE users (
       id BIGINT,
       address STRUCT(street VARCHAR, city VARCHAR, zip VARCHAR)
   );
   ```

2. **Вложенные данные**
   ```sql
   person STRUCT(
       name VARCHAR,
       age INTEGER,
       contact STRUCT(
           email VARCHAR,
           phone VARCHAR
       )
   )
   ```

3. **Сериализация сложных объектов**
   - Хранение объектов из приложения без разложения на множество колонок

---

## Архитектура STRUCT

### Иерархия типов

```
complex_logical_type (тип данных)
    ├── логический тип: logical_type::STRUCT
    └── extension: struct_logical_type_extension
                      └── child_types: vector<complex_logical_type>
```

### Иерархия классов хранения

```
column_data_t (базовый класс)
    └── struct_column_data_t
            ├── sub_columns: vector<unique_ptr<column_data_t>>
            └── validity: validity_column_data_t
```

### Рекурсивная природа

**STRUCT может содержать другие STRUCT!**

```
struct_column_data_t
    ├── sub_columns[0]: standard_column_data_t (name: VARCHAR)
    ├── sub_columns[1]: standard_column_data_t (age: INTEGER)
    └── sub_columns[2]: struct_column_data_t (address)
                            ├── sub_columns[0]: standard_column_data_t (street)
                            ├── sub_columns[1]: standard_column_data_t (city)
                            └── sub_columns[2]: standard_column_data_t (zip)
```

---

## Реализация struct_column_data_t

### Основные поля класса

```cpp
class struct_column_data_t : public column_data_t {
public:
    // Дочерние колонки - по одной для каждого поля структуры
    std::vector<std::unique_ptr<column_data_t>> sub_columns;

    // Управление NULL значениями для всей структуры
    validity_column_data_t validity;

    // ... методы ...
};
```

### Ключевые особенности

#### 1. Каждое поле = отдельная column_data_t

**Пример:** STRUCT(name VARCHAR, age INTEGER, score DOUBLE)

Создаются 3 sub_columns:
- `sub_columns[0]` → standard_column_data_t для VARCHAR
- `sub_columns[1]` → standard_column_data_t для INTEGER
- `sub_columns[2]` → standard_column_data_t для DOUBLE

#### 2. Validity для всей структуры

```cpp
validity_column_data_t validity;
```

- Хранит информацию о том, является ли **вся структура** NULL
- Отдельно от validity каждого поля!
- Структура может быть NULL → все поля игнорируются
- Структура NOT NULL → поля могут быть NULL индивидуально

#### 3. Рекурсивное создание

В конструкторе:
```cpp
struct_column_data_t::struct_column_data_t(...) {
    auto& child_types = type_.child_types();

    for (auto& child_type : child_types) {
        // Фабричный метод автоматически создаст нужный тип!
        sub_columns.push_back(
            create_column(resource, block_manager, sub_column_index,
                         start_row, child_type, this)
        );
    }
}
```

**Магия:** `create_column()` сам решает, какой класс создать:
- Если child_type = VARCHAR → создаст standard_column_data_t
- Если child_type = STRUCT → создаст struct_column_data_t (рекурсия!)
- Если child_type = LIST → создаст list_column_data_t
- И т.д.

---

## Создание и использование STRUCT

### 1. Создание типа STRUCT

#### Способ А: Через create_struct

```cpp
#include <components/types/types.hpp>

using namespace components::types;

// Простая структура
std::vector<complex_logical_type> fields;
fields.push_back(complex_logical_type::create_varchar().with_alias("name"));
fields.push_back(complex_logical_type::create_integer().with_alias("age"));

auto person_type = complex_logical_type::create_struct(fields, "person");
```

#### Способ Б: С именованными полями

```cpp
std::vector<complex_logical_type> fields;
fields.push_back(complex_logical_type::create_varchar().with_alias("street"));
fields.push_back(complex_logical_type::create_varchar().with_alias("city"));
fields.push_back(complex_logical_type::create_varchar().with_alias("zip"));

auto address_type = complex_logical_type::create_struct(fields, "address");
```

#### Способ В: Вложенные структуры

```cpp
// 1. Создаем inner struct (contact)
std::vector<complex_logical_type> contact_fields;
contact_fields.push_back(complex_logical_type::create_varchar().with_alias("email"));
contact_fields.push_back(complex_logical_type::create_varchar().with_alias("phone"));
auto contact_type = complex_logical_type::create_struct(contact_fields, "contact");

// 2. Создаем outer struct (person) с contact внутри
std::vector<complex_logical_type> person_fields;
person_fields.push_back(complex_logical_type::create_varchar().with_alias("name"));
person_fields.push_back(complex_logical_type::create_integer().with_alias("age"));
person_fields.push_back(contact_type.with_alias("contact"));

auto person_type = complex_logical_type::create_struct(person_fields, "person");
```

### 2. Создание таблицы со STRUCT

```cpp
std::vector<column_definition_t> columns;
columns.emplace_back("id", complex_logical_type::create_bigint());
columns.emplace_back("data", person_type); // STRUCT колонка

data_table_t table(resource, block_manager, std::move(columns), "users");
```

### 3. Создание значения STRUCT

```cpp
#include <components/types/logical_value.hpp>

// Способ 1: Создание через вектор значений
std::vector<logical_value_t> fields;
fields.push_back(logical_value_t{"Alice"});        // name
fields.push_back(logical_value_t{25});             // age

logical_value_t person_value = logical_value_t::create_struct(fields);

// Способ 2: С указанием типа
logical_value_t person_value = logical_value_t::create_struct(person_type, fields);
```

### 4. Вставка STRUCT в таблицу

```cpp
vector::data_chunk_t chunk;
chunk.initialize(table.columns());

// ID
chunk.set_value(0, 0, logical_value_t{1LL});

// STRUCT
std::vector<logical_value_t> struct_fields;
struct_fields.push_back(logical_value_t{"Alice"});
struct_fields.push_back(logical_value_t{25});
chunk.set_value(1, 0, logical_value_t::create_struct(struct_fields));

// Append
table_append_state state;
table.initialize_append(state);
table.append(chunk, state);
table.commit_append(0, 1);
```

---

## Внутреннее устройство

### Физическое хранение

**STRUCT не имеет собственного физического хранения!**

```cpp
physical_type complex_logical_type::to_physical_type() const {
    switch (type_) {
        case logical_type::STRUCT:
            return physical_type::STRUCT;  // специальный маркер
        // ...
    }
}
```

**Вместо этого:**
- Каждое поле структуры хранится в отдельной `column_data_t`
- `struct_column_data_t` **не хранит данные**, только **координирует** дочерние колонки

### Пример: Таблица с STRUCT

#### Определение таблицы
```sql
CREATE TABLE users (
    id BIGINT,
    person STRUCT(name VARCHAR, age INTEGER)
);
```

#### Физическое представление в row_group_t

```
row_group_t::columns_
    ├── [0] standard_column_data_t (id: BIGINT)
    │         └── data_: segment_tree<column_segment_t>
    │                       └── physical data on disk
    │
    └── [1] struct_column_data_t (person: STRUCT)
              ├── validity: validity_column_data_t
              │              └── data_: битовая маска для NULL
              │
              └── sub_columns:
                    ├── [0] standard_column_data_t (name: VARCHAR)
                    │         └── data_: segment_tree<column_segment_t>
                    │                     └── physical data on disk
                    │
                    └── [1] standard_column_data_t (age: INTEGER)
                              └── data_: segment_tree<column_segment_t>
                                        └── physical data on disk
```

**Итого на диске:**
- 1 колонка для `id`
- 1 битовая маска для validity структуры
- 2 колонки для полей структуры (`name`, `age`)
- **Всего: 4 физические колонки**

### Индексация полей

#### Индексация в child_states

```cpp
state.child_states[0]  →  validity
state.child_states[1]  →  sub_columns[0] (первое поле)
state.child_states[2]  →  sub_columns[1] (второе поле)
state.child_states[3]  →  sub_columns[2] (третье поле)
// и т.д.
```

**Важно:** `child_states[0]` всегда validity!

#### Индексация в column_path (для UPDATE)

```cpp
column_path[depth] == 0  →  validity
column_path[depth] == 1  →  sub_columns[0]
column_path[depth] == 2  →  sub_columns[1]
column_path[depth] == 3  →  sub_columns[2]
// и т.д.
```

**Формула:** `sub_columns[column_path[depth] - 1]`

---

## Операции с STRUCT

### 1. SCAN (чтение данных)

#### Алгоритм scan()

```cpp
uint64_t struct_column_data_t::scan(
    uint64_t vector_index,
    column_scan_state& state,
    vector::vector_t& result,
    uint64_t target_count
) {
    // 1. Сканируем validity (NULL маску для структуры)
    auto scan_count = validity.scan(vector_index, state.child_states[0], result, target_count);

    // 2. Получаем child vectors из result
    auto& child_entries = result.entries();

    // 3. Сканируем каждое поле структуры
    for (uint64_t i = 0; i < sub_columns.size(); i++) {
        auto& target_vector = *child_entries[i];

        // Проверка: нужно ли сканировать это поле?
        if (!state.scan_child_column[i]) {
            // Если не нужно - помечаем как NULL
            target_vector.set_vector_type(vector::vector_type::CONSTANT);
            target_vector.set_null(true);
            continue;
        }

        // Сканируем дочернюю колонку
        sub_columns[i]->scan(vector_index, state.child_states[i + 1], target_vector, target_count);
    }

    return scan_count;
}
```

#### Оптимизация: scan_child_column

```cpp
state.scan_child_column[i]
```

**Назначение:** Пропуск полей, которые не нужны в результате

**Пример:**
```sql
SELECT id, person.name FROM users;
-- Нужно только поле 'name', 'age' можно пропустить
```

```cpp
state.scan_child_column[0] = true;   // name - нужно
state.scan_child_column[1] = false;  // age - не нужно
```

### 2. APPEND (вставка данных)

#### Алгоритм append()

```cpp
void struct_column_data_t::append(column_append_state& state, vector::vector_t& vector, uint64_t count) {
    // 1. Flatten vector если он не FLAT
    if (vector.get_vector_type() != vector::vector_type::FLAT) {
        vector::vector_t append_vector(vector);
        append_vector.flatten(count);
        append(state, append_vector, count);
        return;
    }

    // 2. Добавляем validity
    validity.append(state.child_appends[0], vector, count);

    // 3. Добавляем каждое поле
    auto& child_entries = vector.entries();
    for (uint64_t i = 0; i < child_entries.size(); i++) {
        sub_columns[i]->append(state.child_appends[i + 1], *child_entries[i], count);
    }

    // 4. Увеличиваем счетчик
    count_ += count;
}
```

#### Flatten

**Что такое flatten?**
- Vector может быть в разных форматах (FLAT, CONSTANT, DICTIONARY)
- FLAT формат: данные хранятся последовательно в массиве
- `flatten()` преобразует любой формат в FLAT

**Зачем нужно?**
- Упрощает логику append
- Гарантирует, что `child_entries` доступны напрямую

### 3. UPDATE (обновление данных)

#### Алгоритм update_column()

```cpp
void struct_column_data_t::update_column(
    const std::vector<uint64_t>& column_path,
    vector::vector_t& update_vector,
    int64_t* row_ids,
    uint64_t update_count,
    uint64_t depth
) {
    if (depth >= column_path.size()) {
        throw std::runtime_error("Cannot directly update a struct column");
    }

    auto update_column = column_path[depth];

    if (update_column == 0) {
        // Обновление validity
        validity.update_column(column_path, update_vector, row_ids, update_count, depth + 1);
    } else {
        // Обновление конкретного поля
        if (update_column > sub_columns.size()) {
            throw std::runtime_error("column_path out of range");
        }

        // Рекурсивно обновляем дочернюю колонку
        sub_columns[update_column - 1]->update_column(
            column_path, update_vector, row_ids, update_count, depth + 1
        );
    }
}
```

#### Column Path

**Что такое column_path?**

Путь к полю для обновления в вложенной структуре.

**Пример:**
```sql
UPDATE users SET person.contact.email = 'new@email.com' WHERE id = 1;
```

**Column path:**
```
[person_column_index, 3, 1]
     ^                 ^   ^
     |                 |   └─ contact.email (поле 0 в contact)
     |                 └───── contact (поле 2 в person, +1 для validity)
     └─────────────────────── person column
```

**Depth:**
- `depth = 0` → обрабатывает struct_column_data_t для person
- `depth = 1` → обрабатывает struct_column_data_t для contact
- `depth = 2` → обрабатывает standard_column_data_t для email

### 4. FETCH (получение одной строки)

#### Алгоритм fetch_row()

```cpp
void struct_column_data_t::fetch_row(
    column_fetch_state& state,
    int64_t row_id,
    vector::vector_t& result,
    uint64_t result_idx
) {
    auto& child_entries = result.entries();

    // Создаем child_states если нужно
    for (uint64_t i = state.child_states.size(); i < child_entries.size() + 1; i++) {
        auto child_state = std::make_unique<column_fetch_state>();
        state.child_states.push_back(std::move(child_state));
    }

    // Fetch validity
    validity.fetch_row(*state.child_states[0], row_id, result, result_idx);

    // Fetch каждое поле
    for (uint64_t i = 0; i < child_entries.size(); i++) {
        sub_columns[i]->fetch_row(*state.child_states[i + 1], row_id, *child_entries[i], result_idx);
    }
}
```

---

## Примеры

### Пример 1: Простая структура

```cpp
// Определение типа
std::vector<complex_logical_type> fields;
fields.push_back(complex_logical_type::create_varchar().with_alias("name"));
fields.push_back(complex_logical_type::create_integer().with_alias("age"));
auto person_type = complex_logical_type::create_struct(fields, "person");

// Создание таблицы
std::vector<column_definition_t> columns;
columns.emplace_back("id", complex_logical_type::create_bigint());
columns.emplace_back("person", person_type);
data_table_t table(resource, block_manager, std::move(columns), "users");

// Вставка данных
vector::data_chunk_t chunk;
chunk.initialize(table.columns());

chunk.set_value(0, 0, logical_value_t{1LL});  // id

std::vector<logical_value_t> person_fields;
person_fields.push_back(logical_value_t{"Alice"});
person_fields.push_back(logical_value_t{25});
chunk.set_value(1, 0, logical_value_t::create_struct(person_fields));

table_append_state state;
table.initialize_append(state);
table.append(chunk, state);
table.commit_append(0, 1);

// Чтение данных
table_scan_state scan_state;
std::vector<storage_index_t> column_ids = {0, 1};
table.initialize_scan(scan_state, column_ids);

vector::data_chunk_t result;
table.scan(result, scan_state);

// result[1] содержит STRUCT vector
// result[1].entries()[0] - name (VARCHAR)
// result[1].entries()[1] - age (INTEGER)
```

### Пример 2: Вложенные структуры

```cpp
// Contact struct
std::vector<complex_logical_type> contact_fields;
contact_fields.push_back(complex_logical_type::create_varchar().with_alias("email"));
contact_fields.push_back(complex_logical_type::create_varchar().with_alias("phone"));
auto contact_type = complex_logical_type::create_struct(contact_fields, "contact");

// Person struct с вложенным contact
std::vector<complex_logical_type> person_fields;
person_fields.push_back(complex_logical_type::create_varchar().with_alias("name"));
person_fields.push_back(complex_logical_type::create_integer().with_alias("age"));
person_fields.push_back(contact_type.with_alias("contact"));
auto person_type = complex_logical_type::create_struct(person_fields, "person");

// Создание значения
std::vector<logical_value_t> contact_values;
contact_values.push_back(logical_value_t{"alice@example.com"});
contact_values.push_back(logical_value_t{"+1234567890"});
auto contact_value = logical_value_t::create_struct(contact_values);

std::vector<logical_value_t> person_values;
person_values.push_back(logical_value_t{"Alice"});
person_values.push_back(logical_value_t{25});
person_values.push_back(contact_value);
auto person_value = logical_value_t::create_struct(person_values);
```

### Пример 3: NULL значения

```cpp
// Вся структура NULL
chunk.set_value(1, 0, logical_value_t());  // NULL struct

// Структура NOT NULL, но поле NULL
std::vector<logical_value_t> person_fields;
person_fields.push_back(logical_value_t{"Bob"});
person_fields.push_back(logical_value_t());  // age = NULL
chunk.set_value(1, 1, logical_value_t::create_struct(person_fields));
```

**Физическое хранение:**

| Row | validity (struct) | name    | age  |
|-----|------------------|---------|------|
| 0   | 0 (NULL)         | (игнор) | (игнор) |
| 1   | 1 (NOT NULL)     | "Bob"   | NULL |

---

## Сравнение с другими типами

### STRUCT vs JSON

| Характеристика | STRUCT | JSON |
|----------------|--------|------|
| **Схема** | Строгая, фиксированная | Гибкая, динамическая |
| **Типизация** | Строгая для каждого поля | Только INTEGER (прототип) |
| **Вложенность** | Неограниченная | Ограничена (прототип) |
| **Хранение** | Раздельные колонки для полей | json_id + auxiliary table |
| **Производительность** | Высокая (прямой доступ) | Ниже (требует сборки) |
| **Сжатие** | Отличное (каждое поле сжимается отдельно) | Хорошее |
| **Изменение схемы** | Сложно | Легко |

**Когда использовать STRUCT:**
- Схема известна и стабильна
- Нужна высокая производительность
- Важна типобезопасность

**Когда использовать JSON:**
- Схема динамическая или неизвестна
- Данные приходят извне (API, логи)
- Нужна гибкость

### STRUCT vs LIST

| Характеристика | STRUCT | LIST |
|----------------|--------|------|
| **Количество элементов** | Фиксированное | Переменное |
| **Типы элементов** | Разные типы | Один тип |
| **Доступ по индексу** | По имени поля | По числовому индексу |
| **Семантика** | Композиция разнородных данных | Коллекция однородных данных |

**Пример:**
```cpp
// STRUCT: разные типы, фиксированное количество
person STRUCT(name VARCHAR, age INTEGER, active BOOLEAN)

// LIST: один тип, переменное количество
emails LIST(VARCHAR)
```

### STRUCT vs Множество колонок

**Вместо STRUCT:**
```sql
CREATE TABLE users (
    id BIGINT,
    person_name VARCHAR,
    person_age INTEGER,
    person_contact_email VARCHAR,
    person_contact_phone VARCHAR
);
```

**С STRUCT:**
```sql
CREATE TABLE users (
    id BIGINT,
    person STRUCT(
        name VARCHAR,
        age INTEGER,
        contact STRUCT(
            email VARCHAR,
            phone VARCHAR
        )
    )
);
```

**Преимущества STRUCT:**
- ✅ Логическая группировка
- ✅ Переиспользование типов
- ✅ Проще добавлять/удалять вложенные поля
- ✅ NULL для всей группы сразу

**Преимущества множества колонок:**
- ✅ Проще запросы (не нужно обращаться через точку)
- ✅ Проще индексирование
- ✅ Меньше сложности в коде

---

## Важные детали реализации

### 1. Проверка на unnamed struct

```cpp
if (type_.type() != types::logical_type::UNION && type_.is_unnamed()) {
    throw std::logic_error("A table cannot be created from an unnamed struct");
}
```

**Почему?**
- Все поля STRUCT должны иметь имена (alias)
- Это нужно для доступа к полям: `person.name`, `person.age`
- UNION - исключение (использует числовые индексы)

### 2. Parent указатель

```cpp
create_column(resource, block_manager, sub_column_index, start_row, child_type, this);
                                                                                   ^^^^
                                                                                   parent
```

**Зачем нужен parent?**
- Дочерние колонки знают свою родительскую struct_column_data_t
- Нужно для получения root_type()
- Нужно для навигации вверх по иерархии

### 3. State management

**child_states всегда содержит:**
```cpp
child_states.size() == sub_columns.size() + 1
                                           ^^^
                                           +1 для validity!
```

**child_appends всегда содержит:**
```cpp
child_appends.size() == sub_columns.size() + 1
```

### 4. Индексация: +1 смещение

```cpp
// При scan
validity.scan(..., state.child_states[0], ...);           // 0
sub_columns[i]->scan(..., state.child_states[i + 1], ...); // i+1

// При update_column
if (update_column == 0) {
    validity.update_column(...);
} else {
    sub_columns[update_column - 1]->update_column(...);
}
```

**Важно помнить:** validity всегда под индексом 0!

---

## Потенциальные проблемы и решения

### Проблема 1: Забыли +1 при индексации

❌ **Неправильно:**
```cpp
sub_columns[i]->scan(..., state.child_states[i], ...);
```

✅ **Правильно:**
```cpp
sub_columns[i]->scan(..., state.child_states[i + 1], ...);
```

### Проблема 2: Не проверили scan_child_column

❌ **Неправильно:**
```cpp
for (uint64_t i = 0; i < sub_columns.size(); i++) {
    sub_columns[i]->scan(...);  // Сканирует ВСЕ поля
}
```

✅ **Правильно:**
```cpp
for (uint64_t i = 0; i < sub_columns.size(); i++) {
    if (!state.scan_child_column[i]) {
        target_vector.set_null(true);
        continue;
    }
    sub_columns[i]->scan(...);
}
```

### Проблема 3: Не flatten перед append

❌ **Может сломаться:**
```cpp
void append(column_append_state& state, vector::vector_t& vector, uint64_t count) {
    auto& child_entries = vector.entries();  // Может быть пустым!
}
```

✅ **Правильно:**
```cpp
void append(column_append_state& state, vector::vector_t& vector, uint64_t count) {
    if (vector.get_vector_type() != vector::vector_type::FLAT) {
        vector::vector_t append_vector(vector);
        append_vector.flatten(count);
        append(state, append_vector, count);
        return;
    }
    // Теперь безопасно
    auto& child_entries = vector.entries();
}
```

---

## Производительность

### Чтение (SCAN)

**Сложность:** O(N * F)
- N = количество строк
- F = количество полей в STRUCT

**Оптимизация через scan_child_column:**
- Пропуск ненужных полей
- Реальная сложность: O(N * F_used)

### Запись (APPEND)

**Сложность:** O(N * F)
- Каждое поле пишется отдельно

**Overhead:**
- Flatten (если vector не FLAT): O(N)
- Minimal для FLAT vectors

### Обновление (UPDATE)

**Сложность:** O(log(R) * D + U * F)
- R = количество row_groups
- D = глубина вложенности
- U = количество обновляемых строк
- F = количество полей в пути

### Занимаемое место

**На диске:**
```
Размер STRUCT = Validity (bits) + ∑(размер каждого поля)
```

**Пример:**
```cpp
STRUCT(name VARCHAR, age INTEGER, active BOOLEAN)
```

На 1000 строк:
- Validity: 1000 bits = 125 bytes
- name (VARCHAR): зависит от длины строк
- age (INTEGER): 1000 * 4 = 4000 bytes
- active (BOOLEAN): 1000 * 1 = 1000 bytes

**Vs отдельные колонки:** Такой же размер!

---

## Заключение

### Ключевые моменты

1. **STRUCT = композиция column_data_t**
   - Каждое поле - отдельная колонка
   - Рекурсивная структура для вложенности

2. **Validity на двух уровнях**
   - Validity всей структуры
   - Validity каждого поля отдельно

3. **Индексация с +1 смещением**
   - child_states[0] всегда validity
   - sub_columns[i] → child_states[i+1]

4. **Оптимизация через scan_child_column**
   - Пропуск ненужных полей
   - Важно для производительности

5. **Flatten обязателен**
   - Перед append нужно flatten
   - Гарантирует доступ к child_entries

### Когда использовать STRUCT

✅ **Используй STRUCT если:**
- Логически связанные данные
- Схема известна и стабильна
- Нужна вложенность
- Важна типобезопасность

❌ **Не используй STRUCT если:**
- Схема часто меняется
- Данные динамические (используй JSON)
- Нужна коллекция однородных данных (используй LIST)

### Дальнейшие улучшения

Возможные направления развития:

1. **Projection pushdown**
   - Автоматическое заполнение scan_child_column
   - На основе запроса SELECT

2. **Lazy materialization**
   - Не загружать неиспользуемые поля
   - Пока не обратились к ним

3. **Compression**
   - Специальные алгоритмы для STRUCT
   - Учет корреляции между полями

4. **Индексирование**
   - Составные индексы на поля STRUCT
   - Специальные структуры данных

---

**Файлы для изучения:**
- `components/table/struct_column_data.hpp` - Объявление класса
- `components/table/struct_column_data.cpp` - Реализация
- `components/types/types.hpp` - struct_logical_type_extension
- `components/types/logical_value.hpp` - Создание значений STRUCT
