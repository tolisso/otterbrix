# JSONBench - Сравнение document_table vs documents

## Результаты теста (100 записей test_sample.json)

### document_table ✅ РАБОТАЕТ ПРАВИЛЬНО
```
Вставка: 160ms (625 rec/s)
SELECT *: 100 записей за 4ms ✅
SELECT WHERE kind='commit': 99 записей ✅
```

### documents (B-tree) ❌ НЕКОРРЕКТНЫЕ РЕЗУЛЬТАТЫ
```
Вставка: 4ms (25,000 rec/s) - в 40 раз быстрее
SELECT *: ТОЛЬКО 1 запись за 0ms ❌❌❌
SELECT WHERE kind='commit': 1 запись ❌
```

## Проблема

При использовании `insert_many()` для `documents` storage:
- ✅ Вставка выполняется очень быстро
- ❌ **SELECT возвращает только 1 запись вместо 100**

## Возможные причины

1. **Перезапись данных** - Все документы вставляются с одним и тем же ключом (все `id=000000000000000000000000`)
2. **Проблема с B-tree индексом** - документы вставляются, но не индексируются
3. **Проблема с курсором** - данные есть, но SELECT их не видит

## Логи

### document_table (корректно)
```
[DEBUG operator_insert] Inserting document with id=000000000000000000000000
[DEBUG operator_insert] Document inserted, row_id=0
...
[DEBUG operator_insert] Document inserted, row_id=99
[DEBUG operator_insert] Inserted 100 documents
```

### documents (проблема)
Нужно добавить логи для отладки.

## Рекомендации

### Для document_table:
✅ Готов к использованию - правильно сохраняет и извлекает данные

### Для documents:
❌ **Не использовать** - теряет данные при `insert_many()`
⚠️ Требует исправления

## Тест

**Файл**: `/home/tolisso/otterbrix/integration/cpp/test/test_jsonbench.cpp`

**Команда**:
```bash
cd /home/tolisso/otterbrix/build/integration/cpp/test
./test_otterbrix "JSONBench - Compare document_table vs document results"
```

## Примечание

Все 100 документов JSON были успешно:
- ✅ Распарсены
- ✅ Переданы в insert_many()
- ❌ Но при `documents` storage только 1 виден после вставки

Возможно, проблема в том, что документы НЕ имеют `_id` поля, и они все получают одинаковый автогенерированный ID, что приводит к перезаписи.

