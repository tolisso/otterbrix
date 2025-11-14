# Архитектура компонента Table в Otterbrix

## Обзор

Компонент `components/table` реализует колоночное хранилище данных для базы данных Otterbrix. Это ядро системы хранения, которое управляет таблицами, строками, колонками и физическим размещением данных на диске.

---

## Иерархия классов

```
data_table_t (Таблица - верхний уровень)
    ↓
collection_t (Коллекция row groups)
    ↓
row_group_t (Группа строк - до 2^30 строк)
    ↓
column_data_t (Данные одной колонки)
    ↓
column_segment_t (Сегмент колонки - физическое хранение)
    ↓
block_handle_t / block_manager_t (Блоки на диске)
```

---

## Основные классы

### 1. data_table_t
**Файл:** `data_table.hpp`

**Назначение:** Основной класс таблицы, предоставляет высокоуровневый API для работы с данными.

**Основные поля:**
```cpp
std::pmr::memory_resource* resource_;           // Аллокатор памяти
std::vector<column_definition_t> column_definitions_; // Определения колонок
std::shared_ptr<collection_t> row_groups_;      // Коллекция групп строк
std::string name_;                              // Имя таблицы
```

**Основные методы:**
- `scan()` - Сканирование таблицы (чтение данных)
- `append()` - Добавление строк
- `update()` - Обновление данных
- `delete_rows()` - Удаление строк
- `fetch()` - Получение конкретных строк по ID

**Использование:**
```cpp
// Создание таблицы
data_table_t table(resource, block_manager, column_definitions, "users");

// Вставка данных
table_append_state state;
table.initialize_append(state);
table.append(chunk, state);
table.commit_append(row_start, count);
```

---

### 2. collection_t
**Файл:** `collection.hpp`

**Назначение:** Управляет коллекцией row_group_t. Это контейнер для групп строк.

**Основные поля:**
```cpp
std::pmr::memory_resource* resource_;
storage::block_manager_t& block_manager_;
std::pmr::vector<types::complex_logical_type> types_; // Типы колонок
row_group_segment_tree_t row_groups_;           // Дерево сегментов row groups
uint64_t row_group_size_;                       // Размер одной группы строк
```

**Основные методы:**
- `append()` - Добавление данных в коллекцию
- `scan()` - Сканирование данных
- `merge_storage()` - Слияние с другой коллекцией
- `append_row_group()` - Добавление новой группы строк

**Особенности:**
- Автоматически создает новые row_group при необходимости
- По умолчанию размер row_group = 2048 строк (DEFAULT_VECTOR_CAPACITY)
- Использует segment_tree для эффективного поиска row_group по номеру строки

---

### 3. row_group_t
**Файл:** `row_group.hpp`

**Назначение:** Группа строк - логическая единица хранения. Содержит сегменты всех колонок для определенного диапазона строк.

**Основные поля:**
```cpp
collection_t* collection_;                      // Ссылка на родительскую коллекцию
std::vector<std::shared_ptr<column_data_t>> columns_; // Колонки этой группы
row_version_manager_t* version_info_;           // Информация о версиях для MVCC
uint64_t start;                                 // Начальная строка
uint64_t count;                                 // Количество строк
```

**Основные методы:**
- `initialize_scan()` - Инициализация сканирования
- `scan()` - Чтение данных из группы
- `append()` - Добавление данных в группу
- `update()` - Обновление строк
- `delete_rows()` - Удаление строк
- `commit_append()` - Фиксация добавленных строк

**Особенности:**
- Каждая row_group содержит column_data_t для каждой колонки таблицы
- Максимальный размер: 2^30 строк (MAX_ROW_GROUP_SIZE)
- Поддерживает MVCC через version_info

---

### 4. column_data_t (базовый класс)
**Файл:** `column_data.hpp`

**Назначение:** Базовый класс для хранения данных колонки. Управляет сегментами данных.

**Основные поля:**
```cpp
uint64_t start_;                                // Начальная строка
std::atomic<uint64_t> count_;                   // Количество строк
storage::block_manager_t& block_manager_;       // Менеджер блоков
types::complex_logical_type type_;              // Тип колонки
segment_tree_t<column_segment_t> data_;         // Дерево сегментов данных
std::unique_ptr<update_segment_t> updates_;     // Сегмент обновлений
```

**Основные методы:**
- `scan()` - Чтение данных
- `append()` - Добавление данных
- `update()` - Обновление данных
- `fetch()` - Получение конкретной строки
- `create_column()` - **Фабричный метод** для создания нужного типа column_data_t

**Иерархия наследников:**
```
column_data_t (базовый)
    ├── standard_column_data_t (обычные типы: INT, VARCHAR, DOUBLE)
    ├── struct_column_data_t (STRUCT типы)
    ├── list_column_data_t (LIST типы)
    ├── array_column_data_t (ARRAY типы)
    ├── json_column_data_t (JSON типы - новый!)
    └── validity_column_data_t (специальный - для NULL значений)
```

**Фабричный метод create_column():**
```cpp
std::unique_ptr<column_data_t> column_data_t::create_column(...) {
    if (type.to_physical_type() == physical_type::STRUCT) {
        return std::make_unique<struct_column_data_t>(...);
    } else if (type.to_physical_type() == physical_type::LIST) {
        return std::make_unique<list_column_data_t>(...);
    } else if (type.type() == logical_type::JSON) {
        return std::make_unique<json_column_data_t>(...);
    }
    // ... другие типы
    return std::make_unique<standard_column_data_t>(...);
}
```

---

### 5. Наследники column_data_t

#### 5.1. standard_column_data_t
**Файл:** `standard_column_data.hpp`

**Назначение:** Стандартные типы данных (INT, VARCHAR, DOUBLE, BIGINT и т.д.)

**Дополнительные поля:**
```cpp
validity_column_data_t validity;  // Управление NULL значениями
```

**Особенности:**
- Самый часто используемый тип column_data
- Делегирует большинство операций базовому классу
- Управляет validity отдельно

#### 5.2. struct_column_data_t
**Файл:** `struct_column_data.hpp`

**Назначение:** Структуры (композитные типы с несколькими полями)

**Дополнительные поля:**
```cpp
std::vector<std::shared_ptr<column_data_t>> child_columns_; // Дочерние колонки
validity_column_data_t validity;
```

**Особенности:**
- Каждое поле структуры - отдельная column_data_t
- Рекурсивная структура для вложенных типов

#### 5.3. list_column_data_t
**Файл:** `list_column_data.hpp`

**Назначение:** Списки переменной длины (например, массивы разных размеров)

**Особенности:**
- Хранит смещения для каждого элемента списка
- Дочерние данные хранятся в отдельной column_data_t

#### 5.4. array_column_data_t
**Файл:** `array_column_data.hpp`

**Назначение:** Массивы фиксированной длины

**Особенности:**
- Все массивы одинакового размера
- Более эффективное хранение, чем LIST

#### 5.5. json_column_data_t ✨ (НОВЫЙ)
**Файл:** `json_column_data.hpp`

**Назначение:** JSON данные с разложением в вспомогательную таблицу

**Дополнительные поля:**
```cpp
std::string auxiliary_table_name_;    // Имя вспомогательной таблицы
std::atomic<int64_t> next_json_id_;   // Счетчик json_id
validity_column_data_t validity;
```

**Особенности:**
- Физически хранит json_id (INT64)
- Фактические данные JSON в auxiliary table
- Поддержка только простых объектов и INTEGER значений (прототип)

#### 5.6. validity_column_data_t
**Файл:** `validity_column_data.hpp`

**Назначение:** Специальная колонка для хранения информации о NULL значениях

**Особенности:**
- Использует битовую маску (1 bit на значение)
- Всегда дочерняя колонка других column_data_t
- Физический тип: BIT

---

### 6. column_segment_t
**Файл:** `column_segment.hpp`

**Назначение:** Физический сегмент данных колонки. Хранится в блоке на диске.

**Основные поля:**
```cpp
types::complex_logical_type type;               // Тип данных
uint64_t type_size;                             // Размер типа
std::shared_ptr<storage::block_handle_t> block; // Блок на диске
uint64_t start;                                 // Начальная строка
uint64_t count;                                 // Количество строк в сегменте
uint32_t block_id;                              // ID блока
uint64_t offset;                                // Смещение в блоке
uint64_t segment_size_;                         // Размер сегмента
```

**Основные методы:**
- `scan()` - Чтение данных из сегмента
- `append()` - Добавление данных в сегмент
- `fetch_row()` - Получение одной строки
- `create_segment()` - Создание нового сегмента

**Особенности:**
- Представляет реальные данные на диске
- Один сегмент = один блок (или часть блока)
- Может быть сжатым (compression)

---

### 7. segment_tree_t<T>
**Файл:** `segment_tree.hpp`

**Назначение:** Шаблонный класс для управления деревом сегментов (row_group_t или column_segment_t)

**Основные поля:**
```cpp
std::vector<segment_node_t<T>> nodes_;          // Узлы дерева
std::mutex node_lock_;                          // Блокировка для потокобезопасности
bool finished_loading_;                         // Флаг завершения загрузки
```

**Основные методы:**
- `root_segment()` - Получить корневой сегмент
- `get_segment(row_idx)` - Найти сегмент по индексу строки
- `append_segment()` - Добавить новый сегмент
- `replace_segment()` - Заменить сегмент

**Особенности:**
- Используется для быстрого поиска сегмента по номеру строки
- Поддерживает ленивую загрузку (SUPPORTS_LAZY_LOADING)
- Потокобезопасный

**Использование:**
```cpp
// В column_data_t
segment_tree_t<column_segment_t> data_;

// В collection_t
row_group_segment_tree_t row_groups_; // Наследник segment_tree_t<row_group_t>
```

---

### 8. update_segment_t
**Файл:** `update_segment.hpp`

**Назначение:** Управление обновлениями (UPDATE операции) для поддержки MVCC

**Основные поля:**
```cpp
column_data_t* root;                            // Корневая колонка
std::map<uint64_t, update_node_t*> updates;     // Мапа обновлений по row_id
undo_buffer_t undo_buffer;                      // Буфер для UNDO
```

**Особенности:**
- Хранит обновленные значения отдельно от основных данных
- Поддерживает откат транзакций (UNDO)
- Используется для MVCC (Multi-Version Concurrency Control)

---

### 9. column_definition_t
**Файл:** `column_definition.hpp`

**Назначение:** Определение колонки (метаданные)

**Основные поля:**
```cpp
std::string name_;                              // Имя колонки
types::complex_logical_type type_;              // Тип колонки
uint64_t storage_oid_;                          // OID в хранилище
uint64_t oid_;                                  // Логический OID
std::unique_ptr<logical_value_t> default_value_; // Значение по умолчанию
```

**Использование:**
```cpp
// Создание определения колонки
column_definition_t col("id", complex_logical_type::create_bigint());
column_definition_t col_with_default(
    "age",
    complex_logical_type::create_integer(),
    std::make_unique<logical_value_t>(0)
);
```

---

## Storage Layer (Слой хранения)

### 10. block_manager_t
**Файл:** `storage/block_manager.hpp`

**Назначение:** Управление блоками на диске

**Основные методы:**
- `create_block()` - Создать новый блок
- `read()` - Чтение блока
- `write()` - Запись блока
- `free_block_id()` - Получить свободный ID блока
- `register_block()` - Зарегистрировать блок в памяти

**Наследники:**
- `in_memory_block_manager_t` - Хранение в памяти (для тестов)
- (другие реализации для диска)

**Основные характеристики:**
- Размер блока по умолчанию: 262144 байта (DEFAULT_BLOCK_ALLOC_SIZE)
- Каждый блок имеет уникальный block_id
- Управляет free lists для переиспользования блоков

---

### 11. buffer_manager_t
**Файл:** `storage/buffer_manager.hpp`

**Назначение:** Управление буферным пулом (кэш блоков в памяти)

**Основные методы:**
- `allocate()` - Выделить память
- `pin()` - Закрепить блок в памяти
- `unpin()` - Открепить блок
- `prefetch()` - Предзагрузка блоков

**Наследники:**
- `standard_buffer_manager_t` - Стандартная реализация с LRU eviction

**Особенности:**
- Управляет памятью с помощью buffer_pool_t
- Реализует eviction policy (вытеснение страниц)
- Поддерживает memory limits

---

### 12. block_handle_t
**Файл:** `storage/block_handle.hpp`

**Назначение:** Handle (дескриптор) для блока. RAII-обертка.

**Основные поля:**
```cpp
block_manager_t& block_manager;
uint32_t block_id;
std::atomic<int32_t> readers;                   // Количество читателей
file_buffer_t buffer;                           // Буфер данных
```

**Особенности:**
- Подсчет ссылок (reference counting)
- Автоматическое управление временем жизни
- Thread-safe

---

### 13. buffer_handle_t
**Файл:** `storage/buffer_handle.hpp`

**Назначение:** Handle для буфера памяти

**Особенности:**
- RAII для управления памятью
- Автоматический pin/unpin при создании/уничтожении

---

## State классы (состояния операций)

### 14. table_scan_state
**Файл:** `table_state.hpp`

**Назначение:** Состояние операции сканирования таблицы

**Основные поля:**
```cpp
collection_scan_state collection_state;         // Состояние сканирования коллекции
std::vector<storage_index_t> column_ids;        // ID сканируемых колонок
table_filter_t* filter;                         // Фильтр (WHERE условие)
```

---

### 15. column_scan_state
**Файл:** `column_state.hpp`

**Назначение:** Состояние сканирования колонки

**Основные поля:**
```cpp
column_segment_t* current;                      // Текущий сегмент
uint64_t row_index;                             // Текущая строка
uint64_t internal_index;                        // Внутренний индекс в сегменте
std::vector<column_scan_state> child_states;    // Дочерние состояния (для struct/list)
```

---

### 16. table_append_state
**Назначение:** Состояние операции вставки

**Основные поля:**
```cpp
row_group_append_state row_group_state;         // Состояние для текущей row_group
```

---

### 17. column_append_state
**Назначение:** Состояние вставки в колонку

**Основные поля:**
```cpp
column_segment_t* current_segment;              // Текущий сегмент для вставки
std::vector<column_append_state> child_appends; // Дочерние состояния
```

---

## Диаграмма потока данных

### Операция SELECT (scan)
```
1. data_table_t::scan()
   ↓
2. collection_t::scan()
   ↓
3. row_group_t::scan() (для каждой группы строк)
   ↓
4. column_data_t::scan() (для каждой колонки)
   ↓
5. column_segment_t::scan() (для каждого сегмента)
   ↓
6. block_handle_t::buffer (чтение из памяти/диска)
   ↓
7. Данные копируются в vector::vector_t
   ↓
8. Результат возвращается в vector::data_chunk_t
```

### Операция INSERT (append)
```
1. data_table_t::append()
   ↓
2. collection_t::append()
   ↓
3. row_group_t::append() (создание новой группы при необходимости)
   ↓
4. column_data_t::append() (для каждой колонки)
   ↓
5. column_segment_t::append() (добавление в сегмент)
   ↓
6. block_handle_t::buffer (запись в память)
   ↓
7. data_table_t::commit_append()
   ↓
8. block_manager_t::write() (flush на диск)
```

---

## Ключевые концепции

### 1. Колоночное хранение
- Данные одной колонки хранятся вместе
- Эффективно для аналитических запросов (OLAP)
- Хорошая компрессия (однотипные данные)

### 2. Row Groups
- Таблица разбита на группы строк (по умолчанию 2048 строк)
- Каждая группа = отдельный набор column_data_t
- Позволяет параллелизм при сканировании

### 3. Сегменты
- Данные колонки разбиты на сегменты
- Один сегмент ≈ один блок на диске
- Lazy loading сегментов

### 4. Segment Tree
- Быстрый поиск сегмента по номеру строки: O(log n)
- Используется для row_group_t и column_segment_t

### 5. MVCC (Multi-Version Concurrency Control)
- update_segment_t хранит обновления
- row_version_manager_t управляет версиями
- Позволяет читать старые версии данных

### 6. Buffer Management
- Блоки кэшируются в памяти (buffer_pool)
- LRU eviction при нехватке памяти
- Pin/Unpin для контроля над блоками

---

## Пример создания таблицы и вставки данных

```cpp
// 1. Создание определений колонок
std::vector<column_definition_t> columns;
columns.emplace_back("id", complex_logical_type::create_bigint());
columns.emplace_back("name", complex_logical_type::create_varchar());
columns.emplace_back("age", complex_logical_type::create_integer());
columns.emplace_back("metadata", complex_logical_type::create_json("__json_users_metadata"));

// 2. Создание таблицы
auto resource = std::pmr::get_default_resource();
auto block_manager = /* ... */;
data_table_t table(resource, block_manager, std::move(columns), "users");

// 3. Подготовка данных
vector::data_chunk_t chunk;
chunk.initialize(table.columns());
chunk.set_value(0, 0, logical_value_t{1LL});          // id = 1
chunk.set_value(1, 0, logical_value_t{"Alice"});      // name = "Alice"
chunk.set_value(2, 0, logical_value_t{25});           // age = 25
chunk.set_value(3, 0, logical_value_t{"{\"score\": 100}"}); // metadata

// 4. Вставка данных
table_append_state state;
table.initialize_append(state);
table.append(chunk, state);
table.commit_append(0, 1);

// 5. Чтение данных
table_scan_state scan_state;
std::vector<storage_index_t> column_ids = {0, 1, 2, 3};
table.initialize_scan(scan_state, column_ids);

vector::data_chunk_t result;
table.scan(result, scan_state);

// result теперь содержит прочитанные данные
```

---

## Взаимодействие с другими компонентами

### components/types
- Определяет типы данных (logical_type, physical_type)
- complex_logical_type используется везде

### components/vector
- vector_t - контейнер для данных одной колонки
- data_chunk_t - контейнер для нескольких колонок (строка таблицы)

### components/expressions
- Используется для фильтрации (WHERE)
- table_filter_t применяется при сканировании

---

## Файловая структура

```
components/table/
├── data_table.{hpp,cpp}           # Основной класс таблицы
├── collection.{hpp,cpp}           # Коллекция row groups
├── row_group.{hpp,cpp}            # Группа строк
├── column_data.{hpp,cpp}          # Базовый класс для колонок
├── standard_column_data.{hpp,cpp} # Стандартные типы
├── struct_column_data.{hpp,cpp}   # STRUCT типы
├── list_column_data.{hpp,cpp}     # LIST типы
├── array_column_data.{hpp,cpp}    # ARRAY типы
├── json_column_data.{hpp,cpp}     # JSON типы (новый)
├── validity_column_data.{hpp,cpp} # NULL values
├── column_segment.{hpp,cpp}       # Физические сегменты
├── segment_tree.hpp               # Дерево сегментов (template)
├── update_segment.{hpp,cpp}       # Обновления для MVCC
├── column_definition.{hpp,cpp}    # Определения колонок
├── column_state.hpp               # Состояния операций
├── table_state.hpp                # Состояния таблицы
├── row_version_manager.{hpp,cpp}  # MVCC версии
└── storage/                       # Слой хранения
    ├── block_manager.{hpp,cpp}
    ├── buffer_manager.{hpp,cpp}
    ├── buffer_pool.{hpp,cpp}
    ├── block_handle.{hpp,cpp}
    ├── buffer_handle.{hpp,cpp}
    └── file_buffer.{hpp,cpp}
```

---

## Потокобезопасность

### Блокировки
- `data_table_t::append_lock_` - для вставки данных
- `segment_tree_t::node_lock_` - для модификации дерева
- `update_segment_t` - внутренние блокировки для обновлений

### Атомарные переменные
- `row_group_t::count` - количество строк (std::atomic)
- `column_data_t::count_` - количество строк в колонке
- `json_column_data_t::next_json_id_` - счетчик json_id

---

## Расширение системы (добавление нового типа колонки)

Шаги для добавления нового типа (на примере JSON):

1. **Добавить логический тип** в `components/types/types.hpp`
2. **Создать extension класс** (если нужны метаданные)
3. **Создать класс column_data** наследник от `column_data_t`
4. **Реализовать методы:**
   - Constructor
   - scan() / scan_committed()
   - append() / append_data()
   - update()
   - fetch() / fetch_row()
5. **Добавить в create_column()** в `column_data.cpp`
6. **Добавить в CMakeLists.txt**

---

Эта архитектура обеспечивает:
- ✅ Эффективное колоночное хранение
- ✅ Масштабируемость (row groups)
- ✅ MVCC для конкурентного доступа
- ✅ Расширяемость типов данных
- ✅ Эффективное использование памяти (buffer pool)
