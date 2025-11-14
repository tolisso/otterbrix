# Работа со строками (VARCHAR/String) в Otterbrix

## Обзор

В Otterbrix строки обрабатываются через несколько ключевых компонентов:
- **logical_value_t** - логическое представление значения (хранит std::string)
- **physical_value** - физическое представление значения (хранит указатель на данные)
- **vector_t** - вектор данных (STRING_LITERAL тип)
- **string_vector_buffer_t** - буфер для хранения строк
- **unified_vector_format** - унифицированный формат для работы с векторами разных типов

---

## 1. Создание и работа со строками на логическом уровне

### logical_value_t - создание строковых значений

**Файл:** `/home/tolisso/diploma/otterbrix/components/types/logical_value.hpp`

```cpp
// Создание строкового значения из std::string
logical_value_t str_value1{std::string("Hello, World!")};

// Создание из std::string_view
logical_value_t str_value2{std::string_view("Hello")};

// Получение строки из logical_value_t
std::string* str_ptr = str_value1.value<std::string*>();
std::string_view str_view = str_value1.value<std::string_view>();
const std::string& str_ref = str_value1.value<const std::string&>();
```

### Конструкторы logical_value_t для строк

```cpp
template<>
inline logical_value_t::logical_value_t(std::string value)
    : type_(logical_type::STRING_LITERAL)
    , value_(std::make_unique<std::string>(std::move(value))) {}

template<>
inline logical_value_t::logical_value_t(std::string_view value)
    : type_(logical_type::STRING_LITERAL)
    , value_(std::make_unique<std::string>(std::move(value))) {}
```

### Способы получения строки из logical_value_t

```cpp
// Получение как указатель на std::string
template<>
inline std::string* logical_value_t::value<std::string*>() const {
    return std::get<std::unique_ptr<std::string>>(value_).get();
}

// Получение как const ссылка на std::string
template<>
inline const std::string& logical_value_t::value<const std::string&>() const {
    return *std::get<std::unique_ptr<std::string>>(value_);
}

// Получение как std::string_view
template<>
inline std::string_view logical_value_t::value<std::string_view>() const {
    return *std::get<std::unique_ptr<std::string>>(value_);
}
```

---

## 2. Работа со строками в vector_t

### Создание вектора строк

**Файл:** `/home/tolisso/diploma/otterbrix/components/vector/vector.cpp`

```cpp
// Создание вектора со строками
auto resource = std::pmr::get_default_resource();
vector_t string_vector(resource, 
                      types::logical_type::STRING_LITERAL,
                      test_size);
```

### Вставка строк в вектор (set_value)

**Файл:** `/home/tolisso/diploma/otterbrix/components/vector/vector.cpp` (строки 417-546)

```cpp
void vector_t::set_value(uint64_t index, const types::logical_value_t& val) {
    // ...
    case types::logical_type::STRING_LITERAL: {
        if (!val.is_null()) {
            assert(type_.type() == types::logical_type::STRING_LITERAL);
            if (!auxiliary_) {
                auxiliary_ = std::make_unique<string_vector_buffer_t>(resource());
            }
            assert(auxiliary_->type() == vector_buffer_type::STRING);
            // Вставляем строку в буфер и получаем string_view
            reinterpret_cast<std::string_view*>(data_)[index] =
                std::string_view((char*) static_cast<string_vector_buffer_t*>(auxiliary_.get())
                                     ->insert(*(val.value<std::string*>())),
                                 val.value<std::string*>()->size());
        }
        break;
    }
    // ...
}
```

### Пример использования set_value для строк

```cpp
// Пример из test_vector.cpp (строки 48-63)
vector_t v(std::pmr::get_default_resource(),
          types::logical_type::STRING_LITERAL,
          test_size);

for (size_t i = 0; i < test_size; i++) {
    // Создаем логическое значение со строкой
    types::logical_value_t value{
        std::string{"long_string_with_index_" + std::to_string(i)}
    };
    // Вставляем в вектор
    v.set_value(i, value);
}

// Получение значений из вектора
for (size_t i = 0; i < test_size; i++) {
    types::logical_value_t value = v.value(i);
    REQUIRE(value.type().type() == types::logical_type::STRING_LITERAL);
    
    // Получаем строку
    std::string result = *(value.value<std::string*>());
    REQUIRE(result == std::string{"long_string_with_index_" + std::to_string(i)});
}
```

---

## 3. Чтение строк из unified_vector_format

### unified_vector_format - что это такое?

**Файл:** `/home/tolisso/diploma/otterbrix/components/vector/vector.hpp`

```cpp
struct unified_vector_format {
    unified_vector_format(std::pmr::memory_resource* resource, uint64_t capacity);
    
    // Получить данные типизированные как T
    template<typename T>
    const T* get_data() const {
        return reinterpret_cast<const T*>(data);
    }
    template<typename T>
    T* get_data() {
        return reinterpret_cast<T*>(data);
    }

    const indexing_vector_t* referenced_indexing = nullptr;  // индексация
    std::byte* data = nullptr;                                // данные
    validity_mask_t validity;                                 // маска валидности
    indexing_vector_t owned_indexing;
};
```

### Преобразование вектора в unified_vector_format

**Файл:** `/home/tolisso/diploma/otterbrix/components/vector/vector.cpp` (строки 946-977)

```cpp
void vector_t::to_unified_format(uint64_t count, unified_vector_format& format) {
    switch (get_vector_type()) {
        case vector_type::DICTIONARY: {
            // Словарь - используем индексацию из словаря
            format.owned_indexing = indexing();
            format.referenced_indexing = &format.owned_indexing;
            if (child().vector_type_ == vector_type::FLAT) {
                format.data = child().data_;
                format.validity = child().validity_;
            } else {
                // Если child не FLAT, его нужно выравнять (flatten)
                // ...
            }
            break;
        }
        case vector_type::CONSTANT:
            // Константный вектор - один элемент повторяется count раз
            format.referenced_indexing = zero_indexing_vector(resource(), count, format.owned_indexing);
            format.data = data_;
            format.validity = validity_;
            break;
        default:
            // FLAT тип
            flatten(count);
            format.referenced_indexing = incremental_indexing_vector(resource());
            format.data = data_;
            format.validity = validity_;
            break;
    }
}
```

### Пример использования unified_vector_format для чтения строк

```cpp
// Пример из arrow_conversion.cpp (строки 98-102)
unified_vector_format run_end_format(result.resource(), compressed_size);
unified_vector_format value_format(result.resource(), compressed_size);

// Преобразуем вектор в unified формат
runs.to_unified_format(compressed_size, run_end_format);
values.to_unified_format(compressed_size, value_format);

// Получаем типизированные данные
auto run_ends_data = run_end_format.get_data<RUN_END_TYPE>();
auto values_data = value_format.get_data<VALUE_TYPE>();

// Обходим данные с использованием индексации
for (size_t i = 0; i < count; i++) {
    auto idx = value_format.referenced_indexing->get_index(i);
    auto value = values_data[idx];
    
    // Проверяем валидность
    if (value_format.validity.row_is_valid(idx)) {
        // Используем значение
    }
}
```

### Для строк (std::string_view)

```cpp
// Пример из string_data.hpp (строки 20-62)
template<bool LARGE_STRING>
static void
append_templated(arrow_append_data_t& append_data, vector_t& input, 
                 size_t from, size_t to, size_t input_size) {
    size_t size = to - from;
    
    // Преобразуем в unified формат
    unified_vector_format format(input.resource(), input_size);
    input.to_unified_format(input_size, format);
    
    // Получаем данные как string_view
    auto data = format.get_data<std::string_view>();  // <-- Типизирование как string_view
    
    // Обходим строки
    for (size_t i = from; i < to; i++) {
        auto source_idx = format.referenced_indexing->get_index(i);
        
        // Проверяем валидность
        if (!format.validity.row_is_valid(source_idx)) {
            // NULL значение
            continue;
        }
        
        // Получаем строку через string_view
        auto string_length = data[source_idx].size();
        const char* string_data = data[source_idx].data();
        
        // Используем строку...
        std::memcpy(aux_buffer.data() + last_offset, string_data, string_length);
    }
}
```

---

## 4. Append операции со строками

### Arrow String Data Appender

**Файл:** `/home/tolisso/diploma/otterbrix/components/vector/arrow/appender/string_data.hpp`

```cpp
template<class SRC = std::string_view, class BUFTYPE = int64_t>
struct arrow_string_data_t {
    static void
    initialize(arrow_append_data_t& result, 
               const types::complex_logical_type& type, 
               uint64_t capacity) {
        // Резервируем буферы
        result.main_buffer().reserve((capacity + 1) * sizeof(BUFTYPE));  // Для offset'ов
        result.auxiliary_buffer().reserve(capacity);                      // Для самих строк
    }

    template<bool LARGE_STRING>
    static void
    append_templated(arrow_append_data_t& append_data, vector_t& input, 
                    size_t from, size_t to, size_t input_size) {
        size_t size = to - from;
        
        // Преобразуем входной вектор в unified формат
        unified_vector_format format(input.resource(), input_size);
        input.to_unified_format(input_size, format);
        
        auto& main_buffer = append_data.main_buffer();
        auto& aux_buffer = append_data.auxiliary_buffer();
        
        // Получаем string_view данные
        auto data = format.get_data<SRC>();
        
        // Получаем offset буфер
        auto offset_data = main_buffer.data<BUFTYPE>();
        
        if (append_data.row_count == 0) {
            offset_data[0] = 0;
        }
        
        auto last_offset = offset_data[append_data.row_count];
        
        // Обходим строки
        for (size_t i = from; i < to; i++) {
            auto source_idx = format.referenced_indexing->get_index(i);
            auto offset_idx = append_data.row_count + i + 1 - from;
            
            // Проверяем NULL
            if (!format.validity.row_is_valid(source_idx)) {
                // Отмечаем как NULL и продолжаем
                append_data.set_null(...);
                offset_data[offset_idx] = last_offset;
                continue;
            }
            
            // Получаем размер строки
            auto string_length = data[source_idx].size();
            
            // Обновляем offset
            auto current_offset = static_cast<size_t>(last_offset) + string_length;
            offset_data[offset_idx] = static_cast<BUFTYPE>(current_offset);
            
            // Копируем данные строки в auxiliary буфер
            aux_buffer.resize(current_offset);
            std::memcpy(aux_buffer.data() + last_offset, 
                       data[source_idx].data(), 
                       data[source_idx].size());
            
            last_offset = static_cast<BUFTYPE>(current_offset);
        }
        
        append_data.row_count += size;
    }

    static void
    append(arrow_append_data_t& append_data, vector_t& input, 
           uint64_t from, uint64_t to, uint64_t input_size) {
        append_templated<false>(append_data, input, from, to, input_size);
    }

    static void
    finalize(arrow_append_data_t& append_data, 
             const types::complex_logical_type& type, 
             ArrowArray* result) {
        result->n_buffers = 3;
        result->buffers[1] = append_data.main_buffer().data();      // Offset буфер
        result->buffers[2] = append_data.auxiliary_buffer().data(); // Строки
    }
};
```

---

## 5. Конвертация между std::string и внутренним представлением

### physical_value - работа со строками

**Файл:** `/home/tolisso/diploma/otterbrix/components/types/physical_value.hpp`

```cpp
class physical_value {
public:
    // Создание physical_value из строки
    explicit physical_value(const char* data, uint32_t size);
    
    // Template конструктор для buffer-like типов (std::string, std::string_view)
    template<typename T>
    physical_value(const T& value, 
                   typename std::enable_if<is_buffer_like<T>>::type* = nullptr)
        : physical_value(value.data(), value.size()) {}
    
    // Получение строки как std::string_view
    std::string_view value_(std::integral_constant<physical_type, physical_type::STRING>) 
        const noexcept;
};
```

### Реализация в physical_value.cpp

```cpp
physical_value::physical_value(const char* data, uint32_t size)
    : type_(physical_type::STRING)
    , size_(size)
    , data_(reinterpret_cast<uint64_t>(data)) {
    assert(size <= uint32_t(-1));
}

std::string_view
physical_value::value_(std::integral_constant<physical_type, physical_type::STRING>) 
    const noexcept {
    assert(type_ == physical_type::STRING);
    return std::string_view(reinterpret_cast<const char*>(data_), size_);
}
```

### string_vector_buffer_t - хранение строк

**Файл:** `/home/tolisso/diploma/otterbrix/components/vector/vector_buffer.hpp`

```cpp
class string_vector_buffer_t : public vector_buffer_t {
public:
    explicit string_vector_buffer_t(std::pmr::memory_resource* resource);
    
    // Вставить строку (различные перегрузки)
    void* insert(void* data, size_t size);
    
    template<typename T>
    void* insert(T&& str_like) {
        return string_heap_.insert(std::forward<T>(str_like));
    }
    
    void* empty_string(size_t size);
    void add_heap_reference(std::unique_ptr<vector_buffer_t> heap);

private:
    core::string_heap_t string_heap_;
    // Используется для переполняющихся строк
    std::pmr::vector<std::shared_ptr<vector_buffer_t>> refs_;
};
```

---

## 6. Практические примеры

### Пример 1: Вставка строк в вектор и чтение

```cpp
#include <components/vector/vector.hpp>
#include <components/types/logical_value.hpp>

// Создание вектора
auto resource = std::pmr::get_default_resource();
components::vector::vector_t v(
    resource,
    components::types::logical_type::STRING_LITERAL,
    1024
);

// Вставка строк
std::vector<std::string> strings = {"hello", "world", "test"};
for (size_t i = 0; i < strings.size(); i++) {
    components::types::logical_value_t value{strings[i]};
    v.set_value(i, value);
}

// Чтение строк
for (size_t i = 0; i < strings.size(); i++) {
    auto value = v.value(i);
    std::string result = *(value.value<std::string*>());
    std::cout << result << std::endl;
}
```

### Пример 2: Работа с unified_vector_format

```cpp
components::vector::unified_vector_format format(
    resource, 
    v.size()
);

// Преобразуем вектор
v.to_unified_format(v.size(), format);

// Получаем string_view данные
auto string_data = format.get_data<std::string_view>();

// Обходим с учетом индексации и валидности
for (size_t i = 0; i < v.size(); i++) {
    size_t idx = format.referenced_indexing->get_index(i);
    
    if (format.validity.row_is_valid(idx)) {
        const auto& str = string_data[idx];
        std::cout << "Size: " << str.size() << ", Data: " << str << std::endl;
    } else {
        std::cout << "NULL" << std::endl;
    }
}
```

### Пример 3: Работа с data_chunk (из test_arrow_conversion.cpp)

```cpp
#include <components/vector/data_chunk.hpp>

data_chunk_t chunk(&resource, types, chunk_size);

// Вставка строк
for (size_t i = 0; i < chunk_size; i++) {
    std::string str = "long_string_with_index_" + std::to_string(i);
    chunk.set_value(
        column_index,
        i,
        components::types::logical_value_t{str}
    );
}

// Получение строк
for (size_t i = 0; i < chunk_size; i++) {
    auto value = chunk.value(column_index, i);
    std::string result = *(value.value<std::string*>());
    // Используем result...
}
```

### Пример 4: Работа со строками в arrays и lists

```cpp
// Array со строками
std::vector<logical_value_t> arr;
for (size_t j = 0; j < array_size; j++) {
    arr.emplace_back(
        std::string{"string_" + std::to_string(j)}
    );
}
logical_value_t array_value = 
    logical_value_t::create_array(logical_type::STRING_LITERAL, arr);
v.set_value(i, array_value);

// Чтение массива строк
auto array_val = v.value(i);
for (size_t j = 0; j < array_val.children().size(); j++) {
    std::string str = *(array_val.children()[j].value<std::string*>());
}

// List со строками (аналогично)
std::vector<logical_value_t> list;
for (size_t j = 0; j < list_size; j++) {
    list.emplace_back(
        std::string{"list_item_" + std::to_string(j)}
    );
}
logical_value_t list_value = 
    logical_value_t::create_list(logical_type::STRING_LITERAL, list);
v.set_value(i, list_value);
```

---

## 7. Ключевые компоненты и их взаимодействие

```
┌─────────────────────────────────────────────────────────────┐
│                    std::string (C++)                          │
│                  "Hello, World!" (256 байт)                   │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼
         ┌───────────────────────────┐
         │   logical_value_t         │
         │  (STRING_LITERAL тип)     │
         │  unique_ptr<std::string>  │
         └────────────┬──────────────┘
                      │
                      ▼
         ┌───────────────────────────────────────┐
         │        vector_t (FLAT)                │
         │  ┌──────────────────────────────────┐ │
         │  │ data: std::string_view[]         │ │
         │  └──────────────────────────────────┘ │
         │  ┌──────────────────────────────────┐ │
         │  │ auxiliary:                       │ │
         │  │  string_vector_buffer_t          │ │
         │  │  ┌──────────────────────────────┐│ │
         │  │  │ string_heap (реальные данные)││ │
         │  │  │ "Hello, World!" (из heap)    ││ │
         │  │  └──────────────────────────────┘│ │
         │  └──────────────────────────────────┘ │
         │  ┌──────────────────────────────────┐ │
         │  │ validity_mask (NULL флаги)       │ │
         │  └──────────────────────────────────┘ │
         └───────────────────────────────────────┘
                      │
                      ▼
         ┌───────────────────────────────────────┐
         │    unified_vector_format              │
         │  ┌──────────────────────────────────┐ │
         │  │ data: std::string_view*          │ │
         │  │ referenced_indexing              │ │
         │  │ validity_mask                    │ │
         │  └──────────────────────────────────┘ │
         └───────────────────────────────────────┘
                      │
                      ▼
         ┌───────────────────────────────────────┐
         │     Arrow C Data Interface            │
         │  ┌──────────────────────────────────┐ │
         │  │ buffers[1]: offset array         │ │
         │  │ buffers[2]: string data          │ │
         │  └──────────────────────────────────┘ │
         └───────────────────────────────────────┘
```

---

## Резюме

1. **Логический уровень**: `logical_value_t` с `std::string` внутри
2. **Вектор уровень**: `vector_t` с типом `STRING_LITERAL`, хранит `std::string_view[]`
3. **Хранение**: `string_vector_buffer_t` с внутренним `string_heap` для реальных данных
4. **Унификация**: `unified_vector_format` дает типизированный доступ к данным
5. **Append**: Специальный `arrow_string_data_t` обработчик для добавления строк
6. **Arrow**: Экспорт в Arrow формат с offset и string buffers

