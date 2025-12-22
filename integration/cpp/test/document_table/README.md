# Document Table Integration Tests

Интеграционные тесты для document_table storage (колоночный формат на Apache Arrow).

## Файлы

### test_document_table_sql.cpp
Основные SQL тесты для document_table:
- CREATE DATABASE/COLLECTION
- INSERT документов
- SELECT, WHERE, GROUP BY, ORDER BY
- Агрегации (COUNT, SUM, MIN, MAX)
- Проекции (выбор отдельных полей)
- Работа с dynamic schema (автоматическое добавление новых полей)

### test_document_table_primary_key.cpp
Тесты primary_key_scan оператора:
- Базовый O(1) lookup по _id
- Performance тест (10,000 документов)
- Multiple lookups
- Сравнение с full_scan (33.6x ускорение)

### test_jsonbench.cpp
Первая версия JSONBench тестов:
- Загрузка Bluesky JSON данных
- Сравнение document vs document_table
- Базовые запросы

### test_jsonbench_separate.cpp
Расширенная версия JSONBench:
- 6 отдельных тестов (INSERT + 5 queries)
- Детальная статистика по каждому запросу
- Тесты на 100, 1000, 10000 записей
- Сравнение производительности document vs document_table

## Запуск тестов

```bash
# Все тесты document_table
./integration/cpp/test/test_otterbrix "[document_table]"

# Primary key тесты
./integration/cpp/test/test_otterbrix "[primary_key]"

# JSONBench тесты
./integration/cpp/test/test_otterbrix "[jsonbench]"

# Конкретный тест
./integration/cpp/test/test_otterbrix "document_table: primary key scan - basic findOne"
```

## Тестовые данные

Тесты используют JSON файлы из родительской директории:
- `../test_sample_100.json` - 100 Bluesky records
- `../test_sample_1000.json` - 1000 Bluesky records
- `../test_sample_10000.json` - 10000 Bluesky records

## Ключевые результаты

### Primary Key Scan
- **33.6x ускорение** по сравнению с full_scan на 10K документах
- O(1) lookup через id_to_row_ map
- Автоматическая оптимизация через planner

### JSONBench (1000 records)
| Тип запроса | document | document_table | Разница |
|-------------|----------|----------------|---------|
| INSERT | 99ms | 4,683ms | 47x медленнее |
| Projection | 382ms | 440ms | **1.2x** (почти паритет!) |
| Aggregation | 311ms | 706ms | **2.3x** медленнее |

**Вывод:** document_table перспективен для analytical workloads (проекции, агрегации).

## Известные проблемы

- ⚠️ Crash при фильтрации по вложенным полям (commit.operation)
- ⚠️ document storage требует явную генерацию _id

## См. также

- `/for_ai/document_table/FINAL_SUMMARY.md` - полный отчет о реализации
- `/for_ai/document_table/04_operators.md` - описание операторов
- `/components/physical_plan/document_table/` - реализация операторов
