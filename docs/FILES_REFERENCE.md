# Справочник файлов Otterbrix для работы со строками

## Основные файлы

| Файл | Назначение | Ключевые компоненты |
|------|-----------|-------------------|
| **Типы и логические значения** |
| `/components/types/logical_value.hpp` | Логическое представление значения | `logical_value_t`, конструкторы для std::string |
| `/components/types/logical_value.cpp` | Реализация логических значений | Конструкторы, копирование, операции со строками |
| `/components/types/physical_value.hpp` | Физическое представление значения | `physical_value`, работа с указателями на данные |
| `/components/types/physical_value.cpp` | Реализация физических значений | Конструктор для (const char*, size_t) |
| **Векторы** |
| `/components/vector/vector.hpp` | Заголовок вектора данных | `vector_t`, `unified_vector_format`, `recursive_unified_vector_format` |
| `/components/vector/vector.cpp` | Реализация вектора | `set_value()`, `to_unified_format()`, работа со STRING_LITERAL |
| `/components/vector/vector_buffer.hpp` | Буферы для хранения данных | `string_vector_buffer_t`, `vector_buffer_t` |
| `/components/vector/vector_buffer.cpp` | Реализация буферов | `string_vector_buffer_t::insert()` |
| **Arrow (Append)** |
| `/components/vector/arrow/arrow_appender.hpp` | Заголовок Arrow appender | `arrow_appender_t`, инициализация |
| `/components/vector/arrow/arrow_appender.cpp` | Реализация Arrow appender | `initialize_function_pointers()`, `append()` |
| `/components/vector/arrow/appender/string_data.hpp` | Специальный обработчик строк | `arrow_string_data_t`, `append_templated()` |
| `/components/vector/arrow/arrow_string_view.hpp` | Arrow представление строк | `arrow_string_view_t`, inlined/ref строки |
| `/components/vector/arrow/scaner/arrow_conversion.cpp` | Конвертация Arrow данных | Чтение string_view из unified_vector_format |
| **Чанки данных** |
| `/components/vector/data_chunk.hpp` | Заголовок чанка данных | `data_chunk_t`, методы set_value |
| `/components/vector/data_chunk.cpp` | Реализация чанка данных | `to_unified_format()` |
| **Тесты и примеры** |
| `/components/vector/tests/test_vector.cpp` | Тесты векторов | Примеры со STRING_LITERAL, set_value, получение значений |
| `/components/vector/tests/test_arrow_conversion.cpp` | Тесты Arrow конвертации | Работа со строками в data_chunk |

---

## Структура работы со строками в Otterbrix

```
┌─────────────────────────────────────────────────────────────────┐
│ ВХОД: std::string                                               │
└──────────────────────────┬──────────────────────────────────────┘
                           │
              ┌────────────┴────────────┐
              │                         │
              ▼                         ▼
    ┌──────────────────────┐   ┌──────────────────────┐
    │ logical_value_t      │   │ physical_value       │
    │ STRING_LITERAL type  │   │ (для сравнений)      │
    │ unique_ptr<string>   │   │ (const char*, size)  │
    └──────────┬───────────┘   └──────────────────────┘
               │
               ▼
    ┌──────────────────────────────────────┐
    │ vector_t (FLAT)                      │
    │ type: STRING_LITERAL                 │
    │ data: std::string_view[]             │
    │ auxiliary: string_vector_buffer_t    │
    │          └─ string_heap              │
    │          └─ refs (overflow strings)  │
    │ validity: validity_mask_t            │
    └──────────┬───────────────────────────┘
               │
    ┌──────────┴──────────────────────────┐
    │ Два пути использования:             │
    ├─────────────────────────────────────┤
    │ 1. Прямой доступ через data()       │
    │ 2. unified_vector_format            │
    └──────────┬──────────────┬───────────┘
               │              │
               ▼              ▼
    ┌──────────────────┐  ┌──────────────────────┐
    │ data[i]:         │  │ unified_vector_      │
    │ std::string_view │  │ format               │
    │                  │  │ • data pointer       │
    │                  │  │ • indexing vector    │
    │                  │  │ • validity mask      │
    └──────────────────┘  └──────────┬───────────┘
               │                      │
               │          ┌───────────┴─────────┐
               │          │                     │
               ▼          ▼                     ▼
        ┌────────────┐ ┌─────────────────┐ ┌──────────────┐
        │ Вывод      │ │ Arrow String    │ │ Сканирование │
        │ string_view│ │ Appender        │ │ таблицы      │
        │            │ │ (offset+data)   │ │              │
        └────────────┘ └─────────────────┘ └──────────────┘
```

---

## Ключевые функции для работы со строками

### Создание и вставка

| Функция | Файл | Описание |
|---------|------|---------|
| `logical_value_t::logical_value_t(std::string)` | logical_value.hpp:151 | Создание логического значения из std::string |
| `logical_value_t::logical_value_t(std::string_view)` | logical_value.hpp:156 | Создание логического значения из std::string_view |
| `vector_t::set_value()` | vector.cpp:417 | Вставка значения в вектор |
| `string_vector_buffer_t::insert()` | vector_buffer.hpp:112,114 | Вставка строки в heap буфер |

### Чтение и доступ

| Функция | Файл | Описание |
|---------|------|---------|
| `logical_value_t::value<std::string*>()` | logical_value.hpp:290 | Получить указатель на std::string |
| `logical_value_t::value<const std::string&>()` | logical_value.hpp:294 | Получить const ссылку на std::string |
| `logical_value_t::value<std::string_view>()` | logical_value.hpp:298 | Получить std::string_view |
| `vector_t::value()` | vector.cpp:548+ | Получить логическое значение из вектора |
| `vector_t::to_unified_format()` | vector.cpp:946 | Преобразовать вектор в unified формат |

### Append операции

| Функция | Файл | Описание |
|---------|------|---------|
| `arrow_string_data_t::initialize()` | string_data.hpp:13 | Инициализация буферов для строк |
| `arrow_string_data_t::append_templated()` | string_data.hpp:20 | Добавление строк в Arrow структуру |
| `arrow_string_data_t::finalize()` | string_data.hpp:70 | Финализация Arrow массива строк |

---

## Примеры кода по файлам

### test_vector.cpp - Базовые операции

```cpp
// Строки 48-63: Вставка и чтение строк
vector_t v(std::pmr::get_default_resource(),
          types::logical_type::STRING_LITERAL,
          test_size);

for (size_t i = 0; i < test_size; i++) {
    types::logical_value_t value{
        std::string{"long_string_with_index_" + std::to_string(i)}
    };
    v.set_value(i, value);
}

for (size_t i = 0; i < test_size; i++) {
    types::logical_value_t value = v.value(i);
    std::string result = *(value.value<std::string*>());
}

// Строки 89-114: Массивы строк
vector_t v(std::pmr::get_default_resource(),
          types::complex_logical_type::create_array(
              types::logical_type::STRING_LITERAL, array_size),
          test_size);

for (size_t i = 0; i < test_size; i++) {
    std::vector<types::logical_value_t> arr;
    for (size_t j = 0; j < array_size; j++) {
        arr.emplace_back(
            std::string{"long_string_with_index_" + 
                       std::to_string(i * array_size + j)}
        );
    }
    v.set_value(
        i,
        types::logical_value_t::create_array(
            types::logical_type::STRING_LITERAL, arr)
    );
}

// Строки 142-167: Списки строк
// Аналогично, но с create_list вместо create_array
```

### test_arrow_conversion.cpp - Arrow операции

```cpp
// Строки 45: Вставка строки в data_chunk
chunk.set_value(1, i, 
    logical_value_t{std::string{"long_string_with_index_" + 
                               std::to_string(i)}});

// Строки 59-66: Массив строк в data_chunk
std::vector<logical_value_t> arr;
for (size_t j = 0; j < array_size; j++) {
    arr.emplace_back(
        std::string{"long_string_with_index_" + 
                   std::to_string(i * array_size + j)}
    );
}
chunk.set_value(5, i, 
    logical_value_t::create_array(
        logical_type::STRING_LITERAL, arr));
```

### string_data.hpp - Append процесс

```cpp
// Строки 32: Получение string_view данных
auto data = format.get_data<std::string_view>();

// Строки 38-60: Обход строк с indexing и validity
for (size_t i = from; i < to; i++) {
    auto source_idx = format.referenced_indexing->get_index(i);
    
    if (!format.validity.row_is_valid(source_idx)) {
        // NULL строка
        continue;
    }
    
    auto string_length = data[source_idx].size();
    std::memcpy(aux_buffer.data() + last_offset, 
               data[source_idx].data(), 
               string_length);
}
```

---

## Различия между vector типами для строк

| vector_type | Описание | unified_format данные |
|-------------|---------|---------------------|
| **FLAT** | Обычный вектор строк | data_= основной буфер, indexing= incremental |
| **CONSTANT** | Одна строка повторяется | data_= одна строка, indexing= all zero |
| **DICTIONARY** | Индексированные строки | data_= словарь, indexing= индексы |
| **SEQUENCE** | Генерируемая последовательность | N/A для строк |

---

## Быстрая справка: Как сделать...

### Создать логическое значение строки
```cpp
components::types::logical_value_t val{std::string("hello")};
```

### Вставить строку в вектор
```cpp
v.set_value(index, logical_value_t{std::string("hello")});
```

### Получить строку из вектора
```cpp
std::string str = *(v.value(index).value<std::string*>());
```

### Работать с unified_vector_format
```cpp
unified_vector_format format(resource, count);
v.to_unified_format(count, format);
auto string_data = format.get_data<std::string_view>();
for (size_t i = 0; i < count; i++) {
    size_t idx = format.referenced_indexing->get_index(i);
    if (format.validity.row_is_valid(idx)) {
        const auto& str = string_data[idx];
        // использовать str
    }
}
```

### Добавить строки в Arrow массив
```cpp
arrow_appender_t appender(types, capacity);
data_chunk_t chunk(...);
appender.append(chunk, 0, chunk.size(), chunk.size());
ArrowArray result = appender.finalize();
```

