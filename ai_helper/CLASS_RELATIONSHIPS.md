# Взаимосвязи классов в components/table

## Диаграмма владения (Ownership)

```
data_table_t
    ├── owns: shared_ptr<collection_t> row_groups_
    ├── owns: vector<column_definition_t> column_definitions_
    └── owns: mutex append_lock_

collection_t
    ├── owns: row_group_segment_tree_t row_groups_
    ├── owns: pmr::vector<complex_logical_type> types_
    └── ref: block_manager_t& block_manager_

row_group_t
    ├── owns: vector<shared_ptr<column_data_t>> columns_
    ├── owns: shared_ptr<row_version_manager_t> owned_version_info_
    └── ref: collection_t* collection_

column_data_t (базовый)
    ├── owns: segment_tree_t<column_segment_t> data_
    ├── owns: unique_ptr<update_segment_t> updates_
    ├── owns: complex_logical_type type_
    ├── owns: mutex update_lock_
    ├── ref: block_manager_t& block_manager_
    └── ref: column_data_t* parent_

standard_column_data_t : column_data_t
    └── owns: validity_column_data_t validity

json_column_data_t : column_data_t
    ├── owns: validity_column_data_t validity
    ├── owns: string auxiliary_table_name_
    └── owns: atomic<int64_t> next_json_id_

struct_column_data_t : column_data_t
    ├── owns: vector<shared_ptr<column_data_t>> child_columns_
    └── owns: validity_column_data_t validity

column_segment_t
    ├── owns: shared_ptr<block_handle_t> block
    ├── owns: complex_logical_type type
    └── owns: unique_ptr<column_segment_state> segment_state_

segment_tree_t<T>
    ├── owns: vector<segment_node_t<T>> nodes_
    └── owns: mutex node_lock_

update_segment_t
    ├── owns: map<uint64_t, update_node_t*> updates
    ├── owns: undo_buffer_t undo_buffer
    └── ref: column_data_t* root

block_manager_t
    ├── owns: mutex blocks_lock_
    ├── owns: unordered_map<uint32_t, weak_ptr<block_handle_t>> blocks_
    └── ref: buffer_manager_t& buffer_manager

block_handle_t
    ├── owns: file_buffer_t buffer
    ├── owns: atomic<int32_t> readers
    └── ref: block_manager_t& block_manager
```

---

## Диаграмма наследования

```
segment_base_t<T> (template)
    ├── row_group_t : segment_base_t<row_group_t>
    └── column_segment_t : segment_base_t<column_segment_t>

segment_tree_t<T, SUPPORTS_LAZY_LOADING>
    └── row_group_segment_tree_t : segment_tree_t<row_group_t, true>

column_data_t (abstract base)
    ├── standard_column_data_t
    ├── struct_column_data_t
    ├── list_column_data_t
    ├── array_column_data_t
    ├── json_column_data_t
    └── validity_column_data_t

buffer_manager_t (abstract)
    └── standard_buffer_manager_t
```

---

## Ссылки между классами (References)

### 1. data_table_t → collection_t
- **Тип:** `shared_ptr<collection_t>`
- **Владение:** Да
- **Множественность:** 1:1
- **Назначение:** Таблица владеет одной коллекцией row groups

### 2. collection_t → row_group_t
- **Тип:** `segment_tree_t<row_group_t>` (через segment_node_t)
- **Владение:** Да (через unique_ptr в segment_node_t)
- **Множественность:** 1:N
- **Назначение:** Коллекция владеет всеми row groups таблицы

### 3. row_group_t → collection_t
- **Тип:** `collection_t* collection_`
- **Владение:** Нет (обратная ссылка)
- **Множественность:** N:1
- **Назначение:** Row group знает свою коллекцию

### 4. row_group_t → column_data_t
- **Тип:** `vector<shared_ptr<column_data_t>> columns_`
- **Владение:** Да (shared ownership)
- **Множественность:** 1:N (по количеству колонок)
- **Назначение:** Row group владеет данными всех колонок для своего диапазона строк

### 5. column_data_t → column_segment_t
- **Тип:** `segment_tree_t<column_segment_t> data_`
- **Владение:** Да
- **Множественность:** 1:N
- **Назначение:** Колонка владеет всеми своими сегментами

### 6. column_data_t → column_data_t (parent)
- **Тип:** `column_data_t* parent_`
- **Владение:** Нет
- **Множественность:** N:1
- **Назначение:** Для вложенных типов (struct fields, list elements)

### 7. column_segment_t → block_handle_t
- **Тип:** `shared_ptr<block_handle_t> block`
- **Владение:** Shared
- **Множественность:** N:1 (несколько сегментов могут использовать один блок)
- **Назначение:** Сегмент ссылается на блок с данными

### 8. block_handle_t → block_manager_t
- **Тип:** `block_manager_t& block_manager`
- **Владение:** Нет
- **Множественность:** N:1
- **Назначение:** Handle знает своего менеджера

### 9. block_manager_t → block_handle_t
- **Тип:** `unordered_map<uint32_t, weak_ptr<block_handle_t>>`
- **Владение:** Нет (weak_ptr)
- **Множественность:** 1:N
- **Назначение:** Менеджер отслеживает активные handles

### 10. column_data_t → update_segment_t
- **Тип:** `unique_ptr<update_segment_t> updates_`
- **Владение:** Да
- **Множественность:** 1:0..1 (создается при первом UPDATE)
- **Назначение:** Хранение обновлений для MVCC

### 11. update_segment_t → column_data_t
- **Тип:** `column_data_t* root`
- **Владение:** Нет
- **Множественность:** 1:1
- **Назначение:** Update segment знает свою колонку

### 12. standard_column_data_t → validity_column_data_t
- **Тип:** `validity_column_data_t validity`
- **Владение:** Да (composition)
- **Множественность:** 1:1
- **Назначение:** Управление NULL значениями

### 13. struct_column_data_t → column_data_t (children)
- **Тип:** `vector<shared_ptr<column_data_t>> child_columns_`
- **Владение:** Да
- **Множественность:** 1:N (по количеству полей в struct)
- **Назначение:** Struct владеет колонками для каждого поля

---

## Жизненный цикл объектов

### Создание таблицы
```
1. Создается data_table_t
2. Создается collection_t
3. collection_t создает первый row_group_t (если данные есть)
4. row_group_t создает column_data_t для каждой колонки
5. column_data_t создает первый column_segment_t при первой вставке
6. column_segment_t получает block_handle_t от block_manager_t
```

### Вставка данных
```
1. data_table_t::append() вызывается
2. collection_t::append() проверяет, нужна ли новая row_group
3. Если нужна: создается новая row_group_t с новыми column_data_t
4. row_group_t::append() вызывается для текущей группы
5. column_data_t::append() для каждой колонки
6. Если текущий сегмент полон: создается новый column_segment_t
7. column_segment_t::append() записывает данные в блок
```

### Удаление таблицы
```
1. data_table_t уничтожается
2. shared_ptr<collection_t> освобождается → collection_t уничтожается
3. row_group_segment_tree_t уничтожается → все row_group_t уничтожаются
4. vector<shared_ptr<column_data_t>> освобождается → column_data_t уничтожаются
5. segment_tree_t<column_segment_t> уничтожается → column_segment_t уничтожаются
6. shared_ptr<block_handle_t> освобождаются
7. Если это последняя ссылка на block: block_manager_t::unregister_block()
```

---

## Потоки данных (Data Flow)

### SELECT (чтение данных)
```
User → data_table_t::scan()
    ↓
collection_t::scan()
    ↓
row_group_t::scan() [для каждой row_group]
    ↓
column_data_t::scan() [для каждой колонки]
    ↓
column_segment_t::scan() [для каждого сегмента]
    ↓
block_handle_t::buffer [чтение из памяти]
    ↓
buffer_manager_t::pin() [если блок не в памяти]
    ↓
block_manager_t::read() [чтение с диска если нужно]
    ↓
Data copied to vector_t
    ↓
vector_t assembled into data_chunk_t
    ↓
Return to User
```

### INSERT (вставка данных)
```
User → data_table_t::append(data_chunk_t)
    ↓
collection_t::append()
    ↓
[Проверка: нужна ли новая row_group?]
    ↓ Yes
collection_t::append_row_group() [создание новой группы]
    ↓
row_group_t::initialize_empty() [создание column_data_t]
    ↓
row_group_t::append()
    ↓
column_data_t::append() [для каждой колонки]
    ↓
[Проверка: нужен ли новый сегмент?]
    ↓ Yes
column_segment_t::create_segment()
    ↓
block_manager_t::create_block()
    ↓
column_segment_t::append() [запись данных]
    ↓
block_handle_t::buffer [запись в память]
    ↓
data_table_t::commit_append()
    ↓
block_manager_t::write() [flush на диск]
```

### UPDATE (обновление данных)
```
User → data_table_t::update(row_ids, data)
    ↓
collection_t::update()
    ↓
row_group_t::update() [для каждой затронутой row_group]
    ↓
column_data_t::update() [для каждой колонки]
    ↓
[Проверка: есть ли update_segment?]
    ↓ No
column_data_t создает update_segment_t
    ↓
update_segment_t::update() [запись обновления]
    ↓
undo_buffer_t::store() [сохранение старого значения для UNDO]
```

---

## Взаимодействие с state объектами

### Scan state передача
```
table_scan_state
    ├── содержит collection_scan_state
    └── содержит column_ids

collection_scan_state
    ├── содержит row_group_t* current
    └── содержит vector<column_scan_state>

column_scan_state
    ├── содержит column_segment_t* current
    └── содержит vector<column_scan_state> child_states
```

### Append state передача
```
table_append_state
    └── содержит row_group_append_state

row_group_append_state
    └── содержит vector<column_append_state>

column_append_state
    ├── содержит column_segment_t* current_segment
    └── содержит vector<column_append_state> child_appends
```

---

## Dependency Graph (граф зависимостей)

### Прямые зависимости (Direct Dependencies)

```
data_table_t зависит от:
    - collection_t
    - column_definition_t
    - table_state.hpp

collection_t зависит от:
    - row_group_t
    - segment_tree.hpp
    - block_manager_t
    - types::complex_logical_type

row_group_t зависит от:
    - column_data_t
    - row_version_manager_t
    - collection_t (обратная ссылка)

column_data_t зависит от:
    - column_segment_t
    - update_segment_t
    - segment_tree.hpp
    - block_manager_t
    - types::complex_logical_type

column_segment_t зависит от:
    - block_handle_t
    - buffer_manager_t

block_handle_t зависит от:
    - block_manager_t
    - file_buffer_t

block_manager_t зависит от:
    - buffer_manager_t
```

### Циклические зависимости (требуют forward declarations)

1. **row_group_t ↔ collection_t**
   - row_group_t содержит `collection_t*`
   - collection_t владеет `row_group_t` через segment_tree
   - Решение: forward declaration в row_group.hpp

2. **column_data_t ↔ update_segment_t**
   - column_data_t содержит `unique_ptr<update_segment_t>`
   - update_segment_t содержит `column_data_t* root`
   - Решение: forward declaration

3. **column_data_t ↔ column_data_t** (parent-child)
   - Для вложенных типов (struct fields)
   - Решение: это естественная рекурсия, поддерживается напрямую

---

## Паттерны проектирования

### 1. Factory Method
**Где:** `column_data_t::create_column()`
```cpp
static std::unique_ptr<column_data_t> create_column(
    // параметры
    const types::complex_logical_type& type
) {
    if (type.to_physical_type() == physical_type::STRUCT) {
        return std::make_unique<struct_column_data_t>(...);
    }
    // ... другие типы
}
```

### 2. Composite
**Где:** `struct_column_data_t` с `child_columns_`
- Структура может содержать другие column_data_t
- Рекурсивная структура для вложенных типов

### 3. Template Method
**Где:** `column_data_t` базовый класс
- Базовый класс определяет скелет алгоритма (scan, append)
- Наследники переопределяют специфичные части

### 4. RAII (Resource Acquisition Is Initialization)
**Где:**
- `block_handle_t` - автоматическое управление блоками
- `buffer_handle_t` - автоматический pin/unpin
- `std::unique_lock` для мьютексов

### 5. Strategy
**Где:** `buffer_manager_t`
- Абстрактный класс для стратегии управления буферами
- Разные реализации (standard, in-memory)

### 6. Observer (частично)
**Где:** `block_manager_t` с `weak_ptr<block_handle_t>`
- Менеджер отслеживает активные handles
- Не полноценный observer, но похожая идея

---

## Ключевые инварианты

### 1. row_group_t
- `start + count` никогда не превышает MAX_ROW_GROUP_SIZE
- Количество column_data_t = количество колонок в таблице
- Все column_data_t в одной row_group имеют одинаковый диапазон [start, start+count)

### 2. column_data_t
- `count_` = сумма count всех сегментов
- Сегменты не пересекаются по диапазону строк
- Сегменты упорядочены по start

### 3. segment_tree_t
- Сегменты в nodes_ упорядочены по row_start
- Нет пробелов между сегментами (если не lazy loading)

### 4. collection_t
- `total_rows_` = сумма count всех row_group_t
- Все row_group_t имеют одинаковый набор типов колонок (types_)

### 5. block_manager_t
- Каждый block_id уникален
- block_id не переиспользуется до mark_as_free()

---

## Многопоточность и синхронизация

### Блокировки по уровням

1. **data_table_t::append_lock_**
   - Защищает вставку данных
   - Scope: вся операция append

2. **segment_tree_t::node_lock_**
   - Защищает модификацию nodes_
   - Scope: добавление/удаление сегментов

3. **column_data_t::update_lock_**
   - Защищает updates_
   - Scope: операции обновления

4. **block_manager_t::blocks_lock_**
   - Защищает map активных блоков
   - Scope: register/unregister block

### Атомарные операции

- `row_group_t::count` - атомарное увеличение при append
- `column_data_t::count_` - атомарное увеличение
- `json_column_data_t::next_json_id_` - атомарный счетчик

### Потенциальные deadlocks

⚠️ **Избегать:**
- Захват append_lock_, затем node_lock_ в одной операции
- Рекурсивные захваты мьютексов

✅ **Правило:** Всегда захватывать блокировки в одном порядке:
1. data_table_t::append_lock_
2. segment_tree_t::node_lock_
3. column_data_t::update_lock_

---

## Размеры и лимиты

| Сущность | Макс. размер | Константа |
|----------|--------------|-----------|
| row_group_t | 2^30 строк | MAX_ROW_GROUP_SIZE |
| column_segment_t | ~размер блока | Зависит от block_alloc_size |
| block | 262144 байта (по умолчанию) | DEFAULT_BLOCK_ALLOC_SIZE |
| data_chunk_t | 2048 строк (обычно) | DEFAULT_VECTOR_CAPACITY |
| row_id | до 2^55 | MAX_ROW_ID |

---

## Типичные операции и их сложность

| Операция | Временная сложность | Комментарий |
|----------|---------------------|-------------|
| Поиск row_group по row_id | O(log N) | Через segment_tree |
| Поиск column_segment по row_id | O(log M) | Через segment_tree |
| Scan всей таблицы | O(rows) | Последовательное чтение |
| Append одной строки | O(1) амортизированно | Может быть O(N) при создании нового сегмента |
| Fetch по row_id | O(log N + log M) | Поиск row_group + поиск segment |
| Update одной строки | O(log N + log M + 1) | Поиск + запись в update_segment |
| Delete одной строки | O(log N) | Помечается в version_info |

Где:
- N = количество row_group в таблице
- M = количество сегментов в колонке

---

Эта структура классов обеспечивает:
- ✅ Эффективное колоночное хранение
- ✅ Быстрый поиск данных (O(log N))
- ✅ Масштабируемость через row_group
- ✅ Расширяемость через наследование column_data_t
- ✅ MVCC поддержка через update_segment_t
- ✅ Управление памятью через RAII
