# Индекс документации: Работа со строками в Otterbrix

Создано: 14 ноября 2025 года

## Быстрый доступ

1. **STRING_EXAMPLES.md** - Подробные примеры и объяснения
   - Обзор системы хранения строк
   - Создание и работа с logical_value_t
   - Работа с vector_t и set_value()
   - Чтение из unified_vector_format
   - Append операции
   - Практические примеры с кодом
   - Диаграмма взаимодействия компонентов

2. **FILES_REFERENCE.md** - Справочник файлов и функций
   - Таблица основных файлов
   - Структура потока данных
   - Таблица ключевых функций
   - Примеры по файлам
   - Различия между типами векторов
   - Быстрая справка "Как сделать..."

3. **STRINGS_SUMMARY.txt** - Краткий обзор
   - Расположение основных файлов
   - Описание ключевых компонентов
   - Диаграмма потока данных
   - Описание операций
   - Важные нюансы
   - Рекомендации по производительности

## Структурированный путь обучения

### Для быстрого старта (10 минут)
1. Прочитайте раздел "Обзор" в STRING_EXAMPLES.md
2. Посмотрите диаграмму в конце STRING_EXAMPLES.md
3. Используйте "Быструю справку" в FILES_REFERENCE.md

### Для среднего уровня (30 минут)
1. Изучите разделы 1-3 в STRING_EXAMPLES.md
2. Посмотрите примеры кода в FILES_REFERENCE.md
3. Прочитайте STRINGS_SUMMARY.txt целиком

### Для глубокого понимания (1-2 часа)
1. Прочитайте все содержимое STRING_EXAMPLES.md
2. Изучите примеры в FILES_REFERENCE.md
3. Найдите исходные файлы на компьютере:
   - /home/tolisso/diploma/otterbrix/components/vector/vector.cpp
   - /home/tolisso/diploma/otterbrix/components/vector/vector.hpp
   - /home/tolisso/diploma/otterbrix/components/vector/tests/test_vector.cpp

## Перечень компонентов

### Основные классы

| Класс | Файл | Описание |
|-------|------|---------|
| logical_value_t | logical_value.hpp | Логическое значение строки |
| vector_t | vector.hpp | Вектор данных с поддержкой строк |
| unified_vector_format | vector.hpp | Унифицированный формат доступа |
| string_vector_buffer_t | vector_buffer.hpp | Буфер для хранения строк |
| arrow_string_data_t | appender/string_data.hpp | Append обработчик для строк |

### Ключевые методы

| Метод | Класс | Файл | Назначение |
|-------|-------|------|-----------|
| set_value() | vector_t | vector.cpp | Вставка строки в вектор |
| to_unified_format() | vector_t | vector.cpp | Преобразование формата |
| value<string*>() | logical_value_t | logical_value.hpp | Получение строки |
| insert() | string_vector_buffer_t | vector_buffer.hpp | Вставка в heap |
| append_templated() | arrow_string_data_t | string_data.hpp | Append операция |

## Визуальная архитектура

```
std::string (C++ стандартная библиотека)
    ↓
logical_value_t (компонента types)
    ↓
vector_t (компонента vector)
    ├─ data: std::string_view[]
    ├─ auxiliary: string_vector_buffer_t
    │   └─ string_heap
    └─ validity: validity_mask_t
    ↓
unified_vector_format (унификация)
    ├─ data: std::byte*
    ├─ referenced_indexing
    └─ validity
    ↓
ArrowArray (стандарт Arrow)
    ├─ buffers[1]: offsets
    └─ buffers[2]: data
```

## Часто задаваемые вопросы

### Q: Где хранятся реальные данные строк?
**A:** В string_vector_buffer_t::string_heap. vector_t::data содержит только std::string_view[].
Подробнее: STRING_EXAMPLES.md, раздел 2, строки 476-488

### Q: Как получить строку из вектора?
**A:** Через логическое значение:
```cpp
std::string str = *(v.value(i).value<std::string*>());
```
Подробнее: STRING_EXAMPLES.md, раздел 2

### Q: Как работает unified_vector_format?
**A:** Это абстракция, позволяющая одинаково работать с разными типами векторов.
Подробнее: STRING_EXAMPLES.md, раздел 3

### Q: Как добавить строку в Arrow?
**A:** Через arrow_string_data_t::append_templated().
Подробнее: STRING_EXAMPLES.md, раздел 4

### Q: Как обрабатываются NULL значения?
**A:** Через validity_mask_t в unified_vector_format.
Подробнее: STRINGS_SUMMARY.txt, раздел "Важные нюансы"

## Ссылки на исходные файлы

### Реальные файлы проекта
- /home/tolisso/diploma/otterbrix/components/types/logical_value.hpp
- /home/tolisso/diploma/otterbrix/components/types/logical_value.cpp
- /home/tolisso/diploma/otterbrix/components/vector/vector.hpp
- /home/tolisso/diploma/otterbrix/components/vector/vector.cpp
- /home/tolisso/diploma/otterbrix/components/vector/vector_buffer.hpp
- /home/tolisso/diploma/otterbrix/components/vector/arrow/appender/string_data.hpp
- /home/tolisso/diploma/otterbrix/components/vector/tests/test_vector.cpp
- /home/tolisso/diploma/otterbrix/components/vector/tests/test_arrow_conversion.cpp

### Тесты
- test_vector.cpp, строки 48-167: Базовые операции
- test_vector.cpp, строки 89-114: Массивы строк
- test_vector.cpp, строки 142-167: Списки строк
- test_arrow_conversion.cpp, строки 12-118: Arrow конвертация

## Советы для изучения

1. **Начните с примеров в тестах** - они наиболее простые и наглядные
2. **Используйте диаграммы** - они помогают понять архитектуру
3. **Читайте код параллельно с документацией** - так быстрее понимается
4. **Экспериментируйте** - попробуйте написать простой пример

## Обновления документации

Документация была создана на основе анализа исходного кода Otterbrix:
- Дата анализа: 14 ноября 2025
- Версия ветки: tolisso/json-1
- Компоненты: types, vector, vector/arrow

Если код изменился, документация может потребовать обновления.
