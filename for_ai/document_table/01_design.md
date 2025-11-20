# Document Table Storage - Дизайн

## Концепция

**document_table_storage** - гибридное хранилище, которое:
- Хранит JSON документы в колоночном формате (data_table)
- Автоматически извлекает JSON paths и создает колонки
- Поддерживает динамическое добавление новых колонок при появлении новых полей
- Сочетает производительность колоночного хранения с гибкостью schema-less документов

## Ключевые преимущества

1. **Колоночная производительность** - быстрые аналитические запросы как у table_storage
2. **Гибкость схемы** - автоматическая адаптация к изменениям структуры документов
3. **Эффективное хранение** - лучшее сжатие за счет колоночного формата
4. **Поддержка JSON paths** - прямой доступ к вложенным полям

## Архитектура

### Три типа хранения в context_collection_t

```cpp
class context_collection_t {
    document_storage_t document_storage_;              // B-tree (существующее)
    table_storage_t table_storage_;                    // data_table (существующее)
    document_table_storage_t document_table_storage_;  // NEW: гибридное хранилище
    storage_type_t storage_type_;                      // флаг типа хранения
};

enum class storage_type_t {
    DOCUMENT_BTREE,      // document_storage_ - B-tree для документов
    TABLE_COLUMNS,       // table_storage_ - фиксированная схема
    DOCUMENT_TABLE       // document_table_storage_ - NEW: документы в колонках
};
```

### Структура document_table_storage_t

```cpp
class document_table_storage_t {
public:
    // Конструктор с начальной схемой или без
    explicit document_table_storage_t(std::pmr::memory_resource* resource);

    // Основные операции
    void insert(document_id_t id, const document_ptr& doc);
    document_ptr get(document_id_t id);
    void remove(document_id_t id);
    void update(document_id_t id, const document_ptr& doc);

    // Сканирование
    void scan(data_chunk_t& output, const table_scan_state& state);

    // Схема
    const schema_t& current_schema() const;
    void evolve_schema(const document_ptr& doc);  // добавление новых колонок

private:
    // Хранилище
    data_table_t table_;

    // Маппинг JSON path -> column index
    std::pmr::unordered_map<std::string, size_t> path_to_column_;

    // Маппинг document_id -> row_id в таблице
    std::pmr::unordered_map<document_id_t, size_t> id_to_row_;

    // Схема (динамически расширяемая)
    dynamic_schema_t schema_;
};
```

## Схема данных

### JSON Path Extraction

Пример документа:
```json
{
  "_id": "doc1",
  "name": "Alice",
  "age": 30,
  "address": {
    "city": "Moscow",
    "zip": "123456"
  },
  "tags": ["developer", "golang"]
}
```

Извлеченные колонки:
```
| _id  | name  | age | address.city | address.zip | tags[0]    | tags[1] |
|------|-------|-----|--------------|-------------|------------|---------|
| doc1 | Alice | 30  | Moscow       | 123456      | developer  | golang  |
```

### Динамическое расширение схемы

При вставке нового документа с дополнительными полями:
```json
{
  "_id": "doc2",
  "name": "Bob",
  "age": 25,
  "phone": "+7123456789",  // новое поле
  "address": {
    "city": "SPb",
    "country": "Russia"    // новое поле
  }
}
```

Схема автоматически расширяется:
```
Новые колонки: phone, address.country
Для старых строк: NULL значения
```

## Обработка типов

### Автоматическое определение типов

1. **Скалярные типы**: int, string, bool, float
2. **Вложенные объекты**: разворачиваются в dot-notation (address.city)
3. **Массивы**:
   - Простые массивы: tags[0], tags[1]
   - Массивы объектов: items[0].price, items[1].price
4. **NULL значения**: для отсутствующих полей

### Стратегии обработки массивов

#### Вариант 1: Fixed size arrays
- Ограничение на количество элементов (например, максимум 10)
- Колонки: array[0], array[1], ... array[9]

#### Вариант 2: Separate array table (рекомендуется)
- Массивы хранятся в отдельной таблице
- Связь через document_id + path + index
- Избегаем разрастания схемы

#### Вариант 3: JSON column type
- Массивы хранятся как JSON строка в одной колонке
- Менее эффективно для фильтрации, но проще

## Интеграция с существующей системой

### Catalog

Добавляем новый формат:
```cpp
enum class used_format_t {
    documents = 0,      // B-tree
    columns = 1,        // data_table
    document_table = 2  // NEW: гибридное
};
```

### Executor

```cpp
if (data_format == used_format_t::document_table) {
    plan = document_table::planner::create_plan(...);
}
```

### Создание коллекции

```sql
-- Явное указание типа хранения
CREATE COLLECTION products STORAGE=DOCUMENT_TABLE;

-- Или через специальную команду
CREATE DOCUMENT_TABLE products;
```

## Операторы

Создаем новый набор операторов: `components/physical_plan/document_table/operators/`

### Основные операторы

1. **full_scan** - сканирование таблицы с предикатами
2. **insert** - вставка документа с эволюцией схемы
3. **update** - обновление с возможным расширением схемы
4. **delete** - удаление строк
5. **index_scan** - использование индексов

### Особенности операторов

```cpp
// document_table/operators/operator_insert.cpp
void operator_insert::on_execute_impl(pipeline::context_t* ctx) {
    for (const auto& doc : input_documents) {
        // 1. Проверяем, нужно ли расширить схему
        if (storage_->needs_schema_evolution(doc)) {
            storage_->evolve_schema(doc);
        }

        // 2. Конвертируем документ в row
        auto row = document_to_row(doc, storage_->current_schema());

        // 3. Вставляем в таблицу
        storage_->insert(doc->id(), row);
    }
}
```

## Производительность

### Преимущества vs document_storage (B-tree)
- ✅ Быстрее аналитические запросы (колоночный формат)
- ✅ Лучшее сжатие данных
- ✅ Эффективная фильтрация по колонкам
- ✅ Векторизация операций

### Преимущества vs table_storage
- ✅ Не требует заранее определенной схемы
- ✅ Автоматическая адаптация к изменениям
- ✅ Поддержка вложенных структур
- ✅ Гибкость как у документов

### Компромиссы
- ⚠️ Накладные расходы на эволюцию схемы
- ⚠️ Больше колонок = больше метаданных
- ⚠️ Сложнее обработка массивов
- ⚠️ NULL значения для разреженных данных

## Примеры использования

### Вставка документов

```sql
CREATE DOCUMENT_TABLE users;

INSERT INTO users VALUES ('{
  "id": 1,
  "name": "Alice",
  "email": "alice@example.com",
  "profile": {
    "age": 30,
    "city": "Moscow"
  }
}');

INSERT INTO users VALUES ('{
  "id": 2,
  "name": "Bob",
  "email": "bob@example.com",
  "profile": {
    "age": 25,
    "city": "SPb",
    "country": "Russia"  // новое поле - схема автоматически расширится
  }
}');
```

### Запросы

```sql
-- Эффективная фильтрация по колонкам
SELECT * FROM users WHERE profile.age > 25;

-- Доступ к вложенным полям
SELECT name, profile.city FROM users WHERE profile.country = 'Russia';

-- Аналитика
SELECT profile.city, COUNT(*), AVG(profile.age)
FROM users
GROUP BY profile.city;
```

## Этапы реализации

1. ✅ Дизайн архитектуры
2. Реализация document_table_storage_t
3. JSON path extraction и schema evolution
4. Операторы document_table/operators
5. Интеграция с catalog и executor
6. Тесты и оптимизация
