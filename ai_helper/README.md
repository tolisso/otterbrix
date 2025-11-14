# AI Helper - Документация для быстрых ответов

Эта папка содержит структурированную документацию о компоненте `components/table` для быстрого ответа на вопросы.

---

## Файлы в этой папке

### 1. TABLE_ARCHITECTURE.md
**Полное описание архитектуры компонента table**

Содержит:
- Иерархию всех классов
- Подробное описание каждого класса
- Назначение и ответственности
- Основные поля и методы
- Примеры использования
- Диаграммы потоков данных
- Ключевые концепции (колоночное хранение, row groups, MVCC)
- Файловую структуру

**Когда читать:**
- Нужно понять общую архитектуру
- Разбираешься, как работает новый класс
- Хочешь понять взаимодействие компонентов

---

### 2. CLASS_RELATIONSHIPS.md
**Взаимосвязи между классами**

Содержит:
- Диаграммы владения (ownership)
- Диаграммы наследования
- Типы ссылок между классами (shared_ptr, weak_ptr, raw pointer)
- Жизненный цикл объектов
- Потоки данных (data flow)
- Граф зависимостей
- Паттерны проектирования
- Инварианты классов
- Многопоточность и синхронизация

**Когда читать:**
- Нужно понять, кто владеет каким объектом
- Ищешь, где может быть утечка памяти
- Разбираешься с deadlock или race condition
- Хочешь понять, как данные передаются между слоями

---

### 3. QUICK_REFERENCE.md
**Быстрая справка и примеры кода**

Содержит:
- Таблицы с кратким описанием классов
- Примеры типичных операций (CREATE, INSERT, SELECT, UPDATE, DELETE)
- State объекты и их использование
- Важные методы классов
- Типичные паттерны кода
- Константы и enums
- Частые ошибки и как их избежать
- Debugging tips
- Cheatsheet

**Когда читать:**
- Нужно быстро вспомнить, как что-то сделать
- Пишешь новый код и хочешь проверить паттерн
- Ищешь пример использования API
- Нужно узнать константу или enum

---

## Как использовать эту документацию

### Сценарий 1: "Как работает INSERT в таблицу?"

1. Открой **QUICK_REFERENCE.md** → раздел "INSERT"
2. Посмотри пример кода
3. Если нужны детали потока данных → **CLASS_RELATIONSHIPS.md** → раздел "INSERT (вставка данных)"
4. Если нужно понять, что делает каждый класс → **TABLE_ARCHITECTURE.md** → соответствующие классы

### Сценарий 2: "Какой класс владеет column_segment_t?"

1. Открой **CLASS_RELATIONSHIPS.md** → раздел "Диаграмма владения"
2. Найди `column_segment_t`
3. Смотри связь: `column_data_t` владеет через `segment_tree_t<column_segment_t> data_`

### Сценарий 3: "Как создать новый тип колонки?"

1. Открой **TABLE_ARCHITECTURE.md** → раздел "Расширение системы"
2. Следуй пошаговой инструкции
3. Посмотри примеры в **QUICK_REFERENCE.md** → паттерны создания child columns

### Сценарий 4: "Почему мой код вызывает deadlock?"

1. Открой **CLASS_RELATIONSHIPS.md** → раздел "Многопоточность и синхронизация"
2. Проверь порядок захвата блокировок
3. Сравни с разделом "Потенциальные deadlocks"

### Сценарий 5: "Что такое row_group_t и зачем он нужен?"

1. Открой **TABLE_ARCHITECTURE.md** → раздел "row_group_t"
2. Прочитай назначение и особенности
3. Если нужны примеры → **QUICK_REFERENCE.md** → "Паттерн 1: Обход всех row groups"

---

## Структура информации

```
TABLE_ARCHITECTURE.md (Детальное описание)
    ├── Что это?
    ├── Зачем нужно?
    ├── Как устроено?
    └── Примеры

CLASS_RELATIONSHIPS.md (Связи и взаимодействие)
    ├── Кто владеет чем?
    ├── Как объекты связаны?
    ├── Как данные текут?
    └── Как избежать проблем?

QUICK_REFERENCE.md (Быстрые ответы)
    ├── Краткие таблицы
    ├── Примеры кода
    ├── Частые ошибки
    └── Tips & tricks
```

---

## Обновление документации

При изменении кода в `components/table`:

1. **Добавлен новый класс?**
   - Добавь описание в **TABLE_ARCHITECTURE.md** → "Основные классы"
   - Добавь связи в **CLASS_RELATIONSHIPS.md** → "Диаграмма владения"
   - Добавь в таблицу в **QUICK_REFERENCE.md**

2. **Изменена иерархия классов?**
   - Обнови **CLASS_RELATIONSHIPS.md** → "Диаграмма наследования"
   - Обнови **TABLE_ARCHITECTURE.md** → соответствующий раздел

3. **Добавлен новый API метод?**
   - Добавь пример в **QUICK_REFERENCE.md**
   - Опиши в **TABLE_ARCHITECTURE.md**, если важный

4. **Найдена частая ошибка?**
   - Добавь в **QUICK_REFERENCE.md** → "Частые ошибки"

---

## Индекс по темам

### Архитектура
- Общая архитектура → **TABLE_ARCHITECTURE.md**
- Иерархия классов → **TABLE_ARCHITECTURE.md** + **CLASS_RELATIONSHIPS.md**
- Диаграммы → **CLASS_RELATIONSHIPS.md**

### Классы
- Описание класса → **TABLE_ARCHITECTURE.md** → найти по имени
- Связи класса → **CLASS_RELATIONSHIPS.md** → "Ссылки между классами"
- Примеры использования → **QUICK_REFERENCE.md** → таблицы и паттерны

### Операции
- CREATE TABLE → **QUICK_REFERENCE.md** → "CREATE TABLE"
- INSERT → **QUICK_REFERENCE.md** → "INSERT" + **CLASS_RELATIONSHIPS.md** → "INSERT (вставка данных)"
- SELECT → **QUICK_REFERENCE.md** → "SELECT" + **CLASS_RELATIONSHIPS.md** → "SELECT (чтение данных)"
- UPDATE → **QUICK_REFERENCE.md** → "UPDATE" + **CLASS_RELATIONSHIPS.md** → "UPDATE (обновление данных)"
- DELETE → **QUICK_REFERENCE.md** → "DELETE"

### Типы данных
- Стандартные типы → **TABLE_ARCHITECTURE.md** → "standard_column_data_t"
- STRUCT → **TABLE_ARCHITECTURE.md** → "struct_column_data_t"
- LIST/ARRAY → **TABLE_ARCHITECTURE.md** → соответствующие разделы
- JSON → **TABLE_ARCHITECTURE.md** → "json_column_data_t"

### Storage
- Block Manager → **TABLE_ARCHITECTURE.md** → "block_manager_t"
- Buffer Manager → **TABLE_ARCHITECTURE.md** → "buffer_manager_t"
- Блоки и буферы → **QUICK_REFERENCE.md** → "Block Manager", "Buffer Manager"

### Многопоточность
- Блокировки → **CLASS_RELATIONSHIPS.md** → "Многопоточность и синхронизация"
- Атомарные операции → **CLASS_RELATIONSHIPS.md** → "Атомарные операции"
- Deadlocks → **CLASS_RELATIONSHIPS.md** → "Потенциальные deadlocks"

### Отладка
- Debugging tips → **QUICK_REFERENCE.md** → "Debugging Tips"
- Частые ошибки → **QUICK_REFERENCE.md** → "Частые ошибки"
- Инварианты → **CLASS_RELATIONSHIPS.md** → "Ключевые инварианты"

### Расширение
- Добавление нового типа → **TABLE_ARCHITECTURE.md** → "Расширение системы"
- Паттерны кода → **QUICK_REFERENCE.md** → "Типичные паттерны кода"

---

## Ключевые концепции (краткий обзор)

### 1. Колоночное хранение
Данные одной колонки хранятся вместе → эффективно для аналитики.
**Детали:** TABLE_ARCHITECTURE.md → "Ключевые концепции"

### 2. Row Groups
Таблица разбита на группы строк (по умолчанию 2048).
**Детали:** TABLE_ARCHITECTURE.md → "row_group_t"

### 3. Сегменты
Данные разбиты на сегменты ≈ размер блока на диске.
**Детали:** TABLE_ARCHITECTURE.md → "column_segment_t"

### 4. MVCC
Multi-Version Concurrency Control - версии для транзакций.
**Детали:** TABLE_ARCHITECTURE.md → "update_segment_t"

### 5. Segment Tree
Дерево для быстрого поиска O(log N).
**Детали:** TABLE_ARCHITECTURE.md → "segment_tree_t"

---

## FAQ - Частые вопросы

### Q: Как найти column_segment_t по номеру строки?
**A:** Используй `column_data_t::data_.get_segment(row_idx)` → O(log N)
**Детали:** QUICK_REFERENCE.md → "Segment Tree"

### Q: Где хранятся NULL значения?
**A:** В `validity_column_data_t` (битовая маска)
**Детали:** TABLE_ARCHITECTURE.md → "validity_column_data_t"

### Q: Почему JSON физически INT64?
**A:** JSON хранит только `json_id`, данные в auxiliary table
**Детали:** TABLE_ARCHITECTURE.md → "json_column_data_t"

### Q: Как избежать deadlock?
**A:** Всегда захватывай блокировки в одном порядке
**Детали:** CLASS_RELATIONSHIPS.md → "Потенциальные deadlocks"

### Q: Какой максимальный размер row_group?
**A:** 2^30 строк (MAX_ROW_GROUP_SIZE)
**Детали:** QUICK_REFERENCE.md → "Константы"

### Q: Как создать новый тип колонки?
**A:** Наследуй от `column_data_t` и добавь в `create_column()`
**Детали:** TABLE_ARCHITECTURE.md → "Расширение системы"

---

## Глоссарий

| Термин | Значение | Где подробнее |
|--------|----------|---------------|
| Row Group | Группа строк (~2048) | TABLE_ARCHITECTURE.md |
| Column Segment | Физический сегмент данных колонки | TABLE_ARCHITECTURE.md |
| Segment Tree | Дерево для O(log N) поиска | TABLE_ARCHITECTURE.md |
| MVCC | Multi-Version Concurrency Control | TABLE_ARCHITECTURE.md |
| Validity | Битовая маска для NULL значений | TABLE_ARCHITECTURE.md |
| Block | Блок данных на диске (~256 KB) | TABLE_ARCHITECTURE.md → Storage Layer |
| Buffer Pool | Кэш блоков в памяти | TABLE_ARCHITECTURE.md → buffer_manager_t |

---

## Версия документации

**Последнее обновление:** 2025-11-14
**Версия Otterbrix:** Текущая версия в разработке
**Покрытие:** Все основные классы components/table

---

## Примеры типичных вопросов и где найти ответы

| Вопрос | Файл → Раздел |
|--------|---------------|
| Как работает INSERT? | QUICK_REFERENCE.md → INSERT |
| Кто владеет column_data_t? | CLASS_RELATIONSHIPS.md → Диаграмма владения |
| Что делает row_group_t? | TABLE_ARCHITECTURE.md → row_group_t |
| Как избежать утечки памяти? | CLASS_RELATIONSHIPS.md → Жизненный цикл |
| Примеры scan кода? | QUICK_REFERENCE.md → SELECT |
| Как работает MVCC? | TABLE_ARCHITECTURE.md → update_segment_t |
| Какие есть паттерны? | QUICK_REFERENCE.md → Типичные паттерны |
| Как дебажить? | QUICK_REFERENCE.md → Debugging Tips |

---

Эта документация создана для быстрых и точных ответов на вопросы о components/table. Используй навигацию выше для поиска нужной информации!
