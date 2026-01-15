# Code Cleanup Plan - document_table

План исправлений для улучшения качества кода document_table (ветка json-3).

---

## 1. Debug Output (Высокий приоритет)

Удалить все `std::cout`/`std::cerr` из production кода.

### Файлы:

| Файл | Строки | Описание |
|------|--------|----------|
| `components/physical_plan/document_table/operators/scan/full_scan.cpp` | 172-180 | `[TIMING full_scan]` вывод |
| `components/physical_plan/document_table/operators/columnar_group.cpp` | 182+ | `[TIMING columnar_group]` вывод |
| `components/physical_plan/document_table/operators/aggregation.cpp` | 8,12,22,26,36 | `[DEBUG]` выводы |
| `components/physical_plan_generator/impl/document_table/create_plan_aggregate.cpp` | 91-95,169,175,181 | `[DEBUG PLANNER]` выводы |

### Рекомендация:
Заменить на логирование через `components/log/log.hpp` или полностью удалить.

---

## 2. Hardcoded Paths (Высокий приоритет)

Абсолютные пути в тестах.

### Файлы:

| Файл | Строка | Проблема |
|------|--------|----------|
| `integration/cpp/test/test_jsonbench.cpp` | 16 | `/home/tolisso/otterbrix/...` |
| `integration/cpp/test/test_jsonbench_old.cpp` | 23 | `/home/tolisso/otterbrix/...` |

### Рекомендация:
Использовать относительные пути или переменные окружения:
```cpp
// Было:
std::string data_path = "/home/tolisso/otterbrix/integration/cpp/test/test_sample_10000.json";

// Должно быть:
std::string data_path = std::string(TEST_DATA_DIR) + "/test_sample_10000.json";
// Где TEST_DATA_DIR определяется в CMakeLists.txt
```

---

## 3. TODO Comments (Средний приоритет)

Незавершённая функциональность.

| Файл | Строка | TODO |
|------|--------|------|
| `components/document_table/json_path_extractor.cpp` | 102 | `// TODO: добавить в array_storage` |
| `components/physical_plan/document_table/operators/operator_delete.cpp` | 17 | separate update_join operator |
| `components/physical_plan/document_table/operators/operator_insert.cpp` | 207 | Index insertion needs proper chunk data |
| `components/physical_plan/document_table/operators/operator_insert.cpp` | 237 | row_id-based filtering |
| `components/physical_plan/document_table/operators/operator_update.cpp` | 21 | separate update_join operator |
| `components/physical_plan_generator/impl/document_table/create_plan_match.cpp` | 14,42 | index support |
| `components/physical_plan_generator/impl/document_table/create_plan_update.cpp` | 93 | правильный node |
| `components/physical_plan_generator/impl/document_table/create_plan_delete.cpp` | 89 | правильный node |
| `components/physical_plan_generator/impl/document_table/create_plan_aggregate.cpp` | 133 | DISTINCT support |

### Рекомендация:
Создать GitHub issues для каждого TODO или удалить если неактуально.

---

## 4. Устаревшая документация (Средний приоритет)

Документы в `for_ai/` не соответствуют текущему коду.

### Устаревшие файлы:

| Файл | Проблема |
|------|----------|
| `for_ai/document_table/03_dynamic_schema.md` | Класс dynamic_schema удалён |
| `for_ai/document_table/README_UNION_TYPES.md` | Union types убраны |
| `for_ai/document_table/FINAL_SUMMARY.md` | Устаревшие бенчмарки |
| `for_ai/document_table/00_summary.md` | Упоминает удалённые компоненты |
| `for_ai/document_table/01_design.md` | Старая архитектура |
| `for_ai/document_table/04_operators.md` | Не отражает текущие операторы |

### Рекомендация:
Удалить или пометить как `[DEPRECATED]`. Актуальная информация в `CURRENT_STATE.md`.

---

## 5. Deprecated Code (Низкий приоритет)

Неиспользуемый или deprecated код.

| Файл | Проблема |
|------|----------|
| `components/document_table/json_path_extractor.hpp` | `extract_paths()` помечен как deprecated |
| `components/document_table/json_path_extractor.hpp` | `type` в `extracted_path_t` помечен как deprecated |
| `components/document_table/json_path_extractor.hpp` | `is_nullable` помечен как deprecated |

### Рекомендация:
Удалить deprecated API после проверки что он не используется.

---

## 6. Смешанные языки (Низкий приоритет)

Комментарии на разных языках.

### Примеры:
- `components/document_table/json_path_extractor.hpp`: Русские комментарии
- `components/physical_plan_generator/impl/document_table/`: Смесь русского и английского

### Рекомендация:
Унифицировать - использовать только английский для комментариев в коде.

---

## 7. Неиспользуемые файлы (Низкий приоритет)

| Файл | Описание |
|------|----------|
| `integration/cpp/test/test_jsonbench_old.cpp` | Старая версия бенчмарка |
| `components/physical_plan/document_table/operators/scan/TODO_index_scans.md` | Markdown в директории с кодом |

---

## Порядок выполнения

1. **Критические (блокируют merge):**
   - [ ] Удалить hardcoded paths
   - [ ] Удалить debug output (std::cout)

2. **Перед release:**
   - [ ] Обработать TODO комментарии
   - [ ] Очистить deprecated код

3. **Housekeeping:**
   - [ ] Обновить/удалить устаревшую документацию
   - [ ] Унифицировать язык комментариев
   - [ ] Удалить неиспользуемые файлы

---

## Оценка трудозатрат

| Категория | Файлов | Оценка |
|-----------|--------|--------|
| Debug output | 4 | 30 мин |
| Hardcoded paths | 2 | 15 мин |
| TODO comments | 9 | 1-2 часа (анализ) |
| Документация | 6 | 30 мин |
| Deprecated code | 1 | 15 мин |
| **Всего** | | **3-4 часа** |
