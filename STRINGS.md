# Хранение строк в Otterbrix

## Как реализовано хранение строк в единственном экземпляре?

### Короткий ответ
**На самом деле в Otterbrix НЕ реализовано глобальное хранение строк в единственном экземпляре** (string interning). Вместо этого используется комбинация локальных механизмов:

1. **Per-vector/per-segment string heaps** - для временных данных
2. **Dictionary compression** - для постоянного хранения в column segments

### Детальное описание

#### 1. Временное хранение строк (in-memory)

**Файл**: `core/string_heap/string_heap.hpp`

Для временных данных (векторы, update segments) используется `string_heap_t`:

```cpp
class string_heap_t {
public:
    explicit string_heap_t(std::pmr::memory_resource* resource);
    void reset();
    void* insert(const void* data, size_t size);

private:
    std::pmr::monotonic_buffer_resource arena_allocator_;
};
```

**Характеристики**:
- Каждый вектор имеет свой собственный string heap
- **Дедупликации нет** - каждая строка хранится отдельно
- Использует `std::pmr::monotonic_buffer_resource` для быстрой последовательной аллокации
- Память освобождается целиком через `reset()` или при уничтожении объекта

**Файл**: `components/vector/vector_buffer.hpp:109-122`

```cpp
class string_vector_buffer_t : public vector_buffer_t {
private:
    core::string_heap_t string_heap_;
    std::pmr::vector<std::shared_ptr<vector_buffer_t>> refs_;
};
```

#### 2. Постоянное хранение строк (column storage)

**Файл**: `components/table/column_segment.cpp:32-73`

Для постоянного хранения в column segments используется **dictionary compression**:

```cpp
struct dictionary_compression_header_t {
    uint32_t dict_size;        // Размер словаря
    uint32_t dict_end;         // Конечное смещение словаря
    uint32_t index_buffer_offset;
    uint32_t index_buffer_count;
    uint32_t bitpacking_width;
};

struct string_dictionary_container_t {
    uint32_t size;
    uint32_t end;
};
```

**Структура хранения**:
```
[Dictionary Header (20 байт)]
[Index Array: int32_t смещения для каждой строки]
[Dictionary: Фактические данные строк растут назад от конца]
```

**Механизм**:
- В индексном массиве хранятся смещения (4 байта на строку)
- Повторяющиеся строки **внутри одного segment** указывают на одну запись в словаре
- Это обеспечивает **локальную дедупликацию** в пределах segment
- **Межсегментной дедупликации нет**

#### 3. Хранение больших строк

**Файл**: `components/table/column_segment.cpp:374-444`

```cpp
static constexpr uint64_t DEFAULT_STRING_BLOCK_LIMIT = 4096;
```

Строки делятся на:
- **Малые строки** (< 4KB): хранятся прямо в dictionary секции, положительное смещение
- **Большие строки** (≥ 4KB): хранятся в отдельных overflow блоках, отрицательное смещение

**Файл**: `components/table/column_state.hpp:204-241`

```cpp
struct string_block_t {
    std::shared_ptr<storage::block_handle_t> block;
    uint64_t offset;
    uint64_t size;
    std::unique_ptr<string_block_t> next;
};

struct uncompressed_string_segment_state {
    std::unique_ptr<string_block_t> head;
    std::unordered_map<uint32_t, string_block_t*> overflow_blocks;
    std::vector<uint32_t> on_disk_blocks;
};
```

### Почему нет глобальной дедупликации?

**Преимущества текущего подхода**:
- Быстрая аллокация (monotonic buffer allocator)
- Хорошая локальность данных
- Масштабируемость (нет глобальной конкуренции за string pool)
- Простота реализации
- Dictionary compression эффективна для повторяющихся значений в колонках

**Недостатки**:
- Одинаковые строки дублируются между векторами/сегментами
- Больший расход памяти для глобально повторяющихся строк

**Вывод**: Дизайн приоритизирует **производительность и простоту** над **максимальной эффективностью памяти**, что соответствует фокусу Otterbrix на быстрых аналитических запросах.

---

## Как можно хранить массивы строк в единственном экземпляре?

Хотя в текущей реализации глобальной дедупликации нет, вот как можно было бы реализовать хранение массивов строк в единственном экземпляре:

### Предлагаемая архитектура

#### 1. Глобальный пул строк (String Pool)

```cpp
class global_string_pool_t {
public:
    // Добавить строку, вернуть ID
    string_id_t intern(std::string_view str);

    // Получить строку по ID
    std::string_view get(string_id_t id) const;

    // Добавить массив строк, вернуть ID массива
    array_id_t intern_array(std::span<std::string_view> strings);

    // Получить массив строк по ID
    std::span<const string_id_t> get_array(array_id_t id) const;

private:
    // Хеш-таблица: hash(string) -> string_id
    std::unordered_map<uint64_t, string_id_t> string_hash_to_id_;

    // Хранилище строк: string_id -> данные строки
    std::vector<std::unique_ptr<char[]>> string_storage_;
    std::vector<string_metadata_t> string_metadata_;

    // Хеш-таблица: hash(array) -> array_id
    std::unordered_map<uint64_t, array_id_t> array_hash_to_id_;

    // Хранилище массивов: array_id -> массив string_id
    std::vector<std::vector<string_id_t>> array_storage_;
};

struct string_metadata_t {
    const char* data;
    uint32_t length;
    uint32_t ref_count;
};

using string_id_t = uint32_t;
using array_id_t = uint32_t;
```

#### 2. Хеширование массивов строк

Для быстрого поиска существующих массивов используем хеш, учитывающий порядок:

```cpp
uint64_t hash_string_array(std::span<const string_id_t> array) {
    uint64_t hash = 0;

    // Используем rolling hash или xxHash
    for (size_t i = 0; i < array.size(); ++i) {
        hash = hash * 31 + array[i];
        // Или: hash = xxhash(hash, array[i], i);
    }

    return hash;
}
```

**Важно**: Хеш учитывает:
- ID строк (уже интернированных)
- Порядок строк в массиве
- Размер массива

#### 3. Интернирование массива строк

```cpp
array_id_t global_string_pool_t::intern_array(
    std::span<std::string_view> strings)
{
    // Шаг 1: Интернировать каждую строку
    std::vector<string_id_t> string_ids;
    string_ids.reserve(strings.size());

    for (const auto& str : strings) {
        string_ids.push_back(intern(str));
    }

    // Шаг 2: Вычислить хеш массива ID
    uint64_t array_hash = hash_string_array(string_ids);

    // Шаг 3: Проверить, существует ли такой массив
    auto it = array_hash_to_id_.find(array_hash);
    if (it != array_hash_to_id_.end()) {
        // Нужна дополнительная проверка на коллизию
        if (arrays_equal(string_ids, array_storage_[it->second])) {
            return it->second;  // Нашли существующий массив
        }
    }

    // Шаг 4: Создать новый массив
    array_id_t new_id = array_storage_.size();
    array_storage_.push_back(std::move(string_ids));
    array_hash_to_id_[array_hash] = new_id;

    return new_id;
}
```

#### 4. Быстрый поиск по массиву строк

```cpp
// O(N) для хеширования + O(1) для lookup в hash table
array_id_t find_array(std::span<std::string_view> strings) {
    // Шаг 1: Найти ID каждой строки
    std::vector<string_id_t> string_ids;
    for (const auto& str : strings) {
        auto id = find_string_id(str);
        if (id == INVALID_ID) {
            return INVALID_ARRAY_ID;  // Строка не существует
        }
        string_ids.push_back(id);
    }

    // Шаг 2: Вычислить хеш и найти массив
    uint64_t array_hash = hash_string_array(string_ids);
    auto it = array_hash_to_id_.find(array_hash);

    if (it != array_hash_to_id_.end()) {
        // Проверка на коллизию
        if (arrays_equal(string_ids, array_storage_[it->second])) {
            return it->second;
        }
    }

    return INVALID_ARRAY_ID;
}
```

#### 5. Оптимизации

##### a) Двухуровневое хеширование

```cpp
struct two_level_hash_t {
    uint64_t quick_hash;   // Быстрый хеш (может иметь коллизии)
    uint64_t strong_hash;  // Сильный хеш (для разрешения коллизий)
};

two_level_hash_t hash_array(std::span<const string_id_t> array) {
    return {
        .quick_hash = xxhash64(array),
        .strong_hash = blake3_hash(array)
    };
}
```

Первый lookup по `quick_hash`, при коллизии проверяем `strong_hash`.

##### b) Кеширование часто используемых массивов

```cpp
// LRU кеш для горячих массивов
lru_cache<array_id_t, std::span<const string_id_t>> hot_arrays_cache_;
```

##### c) Bloom filter для быстрого отсечения

```cpp
// Перед поиском в hash table проверяем bloom filter
bloom_filter_t array_bloom_filter_;

array_id_t find_array(std::span<std::string_view> strings) {
    uint64_t hash = compute_hash(strings);

    if (!array_bloom_filter_.might_contain(hash)) {
        return INVALID_ARRAY_ID;  // Точно нет
    }

    // Продолжаем полный поиск...
}
```

#### 6. Интеграция с column storage

```cpp
// В column segment вместо хранения строк хранятся array_id
struct array_column_segment_t {
    std::vector<array_id_t> array_ids;  // 4 байта на массив
    global_string_pool_t* string_pool;   // Shared pointer
};

// Чтение массива
std::vector<std::string_view> get_array(size_t row) {
    array_id_t array_id = array_ids[row];
    auto string_ids = string_pool->get_array(array_id);

    std::vector<std::string_view> result;
    for (string_id_t sid : string_ids) {
        result.push_back(string_pool->get(sid));
    }
    return result;
}
```

### Сложность операций

| Операция | Сложность | Примечание |
|----------|-----------|------------|
| Интернирование строки | O(len) | Хеширование + lookup |
| Интернирование массива N строк | O(N × avg_len) | N интернирований строк + хеш массива |
| Поиск массива | O(N × avg_len) | N поисков строк + хеш массива |
| Получение массива по ID | O(N) | Копирование N указателей |
| Сравнение массивов | O(N) | Только при коллизии хешей |

### Преимущества подхода

1. **Дедупликация**:
   - Идентичные массивы хранятся один раз
   - Идентичные строки хранятся один раз
   - Экономия памяти при повторяющихся данных

2. **Быстрый поиск**:
   - O(1) lookup по хешу (в среднем)
   - Двухуровневое хеширование минимизирует коллизии

3. **Компактность**:
   - array_id - 4 байта
   - string_id - 4 байта
   - Значительная экономия для больших массивов

4. **Иммутабельность**:
   - Интернированные данные неизменяемы
   - Безопасное share между потоками
   - Copy-on-write семантика

### Недостатки и challenges

1. **Глобальная синхронизация**:
   - Нужны locks для доступа к string pool
   - Может стать bottleneck при высокой конкуренции
   - **Решение**: Sharded string pool (по хешу)

2. **Управление памятью**:
   - Нужен garbage collection или ref counting
   - Фрагментация при удалении строк
   - **Решение**: Generational arena allocator

3. **Сериализация**:
   - ID не валидны между перезапусками
   - Нужна отдельная таблица ID при сохранении
   - **Решение**: Persist hash → ID mapping

### Пример использования

```cpp
// Создание таблицы с колонкой массивов строк
table_builder_t builder;
builder.add_column("tags", type::ARRAY_STRING);

// Вставка данных
std::vector<std::string_view> tags1 = {"cpp", "database", "nosql"};
std::vector<std::string_view> tags2 = {"cpp", "performance"};
std::vector<std::string_view> tags3 = {"cpp", "database", "nosql"};  // дубликат tags1

// Интернирование
auto id1 = string_pool.intern_array(tags1);  // Новый массив
auto id2 = string_pool.intern_array(tags2);  // Новый массив
auto id3 = string_pool.intern_array(tags3);  // Возвращает id1!

assert(id1 == id3);  // Один и тот же массив

// Поиск
auto found_id = string_pool.find_array({"cpp", "database", "nosql"});
assert(found_id == id1);
```

---

## Ключевые файлы для изучения

### Текущая реализация хранения строк:

1. **String heap (временное хранение)**:
   - `core/string_heap/string_heap.hpp` - Заголовочный файл
   - `core/string_heap/string_heap.cpp` - Реализация

2. **Vector string buffer**:
   - `components/vector/vector_buffer.hpp:109-122` - String vector buffer
   - `components/vector/vector_buffer.cpp:29-40` - Реализация

3. **Column segment (dictionary compression)**:
   - `components/table/column_segment.cpp:14-596` - Полная реализация
   - `components/table/column_segment.cpp:32-73` - Структуры словаря
   - `components/table/column_segment.cpp:374-444` - String append логика

4. **Overflow strings**:
   - `components/table/column_state.hpp:204-241` - String blocks и overflow
   - `components/table/update_segment.hpp:279` - Update segment heap

---

## Выводы

1. **Текущее состояние**: Otterbrix не использует глобальное хранение строк в единственном экземпляре. Вместо этого используется:
   - Per-vector string heaps для временных данных (без дедупликации)
   - Dictionary compression для column segments (локальная дедупликация)

2. **Для массивов строк**: Можно реализовать двухуровневое интернирование:
   - Уровень 1: Интернирование индивидуальных строк → string_id
   - Уровень 2: Интернирование массивов string_id → array_id
   - Поиск через хеширование массива ID: O(N) хеширование + O(1) lookup

3. **Trade-offs**: Глобальная дедупликация экономит память, но добавляет:
   - Overhead синхронизации
   - Сложность управления временем жизни
   - Потенциальные bottleneck при высокой нагрузке
