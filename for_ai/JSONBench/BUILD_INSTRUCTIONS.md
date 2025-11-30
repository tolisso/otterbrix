# Инструкции по сборке локального Otterbrix для JSONBench

## Зачем нужна локальная сборка?

Для бенчмарка JSONBench мы используем локальную сборку Otterbrix из репозитория `/home/tolisso/otterbrix/` вместо установки через `pip install otterbrix`. Это позволяет:

1. Тестировать последние доработки и изменения
2. Оптимизировать код специально под бенчмарк
3. Отлаживать проблемы на месте
4. Использовать Release-сборку с максимальными оптимизациями

## Процесс сборки

### Шаг 1: Установка зависимостей

```bash
# Системные зависимости (Ubuntu/Debian)
sudo apt update
sudo apt install -y \
    build-essential \
    ninja-build \
    python3-pip \
    python3-dev \
    curl \
    gnupg \
    apt-transport-https \
    zlib1g

# Python зависимости для сборки
pip3 install conan==2.20.0 pytest==6.2.5 cmake
```

### Шаг 2: Настройка Conan

```bash
# Создать профиль Conan
conan profile detect --force

# Добавить удаленный репозиторий Otterbrix
conan remote add otterbrix http://conan.otterbrix.com
```

### Шаг 3: Сборка Otterbrix

```bash
# Перейти в корень репозитория
cd /home/tolisso/otterbrix

# Создать директорию сборки
mkdir -p build
cd build

# Установить зависимости через Conan
conan install ../conanfile.py \
    --build missing \
    -s build_type=Release \
    -s compiler.cppstd=gnu17

# Сконфигурировать CMake
cmake .. \
    -G Ninja \
    -DCMAKE_TOOLCHAIN_FILE=./build/Release/generators/conan_toolchain.cmake \
    -DCMAKE_BUILD_TYPE=Release \
    -DDEV_MODE=ON

# Собрать (используя все ядра процессора)
cmake --build . --target all -- -j $(nproc)
```

### Шаг 4: Проверка сборки

```bash
# Проверить что Python модуль создан
ls -la /home/tolisso/otterbrix/build/integration/python/

# Должны увидеть файлы:
# otterbrix.py
# _otterbrix.so (или .pyd на Windows)
# и другие

# Протестировать импорт
export PYTHONPATH=/home/tolisso/otterbrix/build/integration/python:$PYTHONPATH
python3 -c "import otterbrix; print('✓ Otterbrix импортирован успешно')"
```

### Шаг 5: Запуск тестов (опционально)

```bash
# C++ тесты
cd /home/tolisso/otterbrix/build
ctest -C Release -V --output-on-failure --timeout 60

# Python тесты
cd integration/python/
pytest -v -s
```

## Использование локальной сборки

### В Python скриптах

```python
#!/usr/bin/env python3
import sys
import os

# Добавить путь к локальной сборке
OTTERBRIX_PYTHON_PATH = "/home/tolisso/otterbrix/build/integration/python"
if os.path.exists(OTTERBRIX_PYTHON_PATH):
    sys.path.insert(0, OTTERBRIX_PYTHON_PATH)
else:
    print(f"Error: Otterbrix not found at {OTTERBRIX_PYTHON_PATH}")
    sys.exit(1)

from otterbrix import Client

# Теперь можно использовать Otterbrix
client = Client()
```

### В bash скриптах

```bash
#!/bin/bash
# Настроить PYTHONPATH
export PYTHONPATH="/home/tolisso/otterbrix/build/integration/python:$PYTHONPATH"

# Запустить Python с Otterbrix
python3 your_script.py
```

### Для интерактивного использования

```bash
# Добавить в ~/.bashrc или ~/.zshrc
export PYTHONPATH="/home/tolisso/otterbrix/build/integration/python:$PYTHONPATH"

# Или для текущей сессии
export PYTHONPATH="/home/tolisso/otterbrix/build/integration/python:$PYTHONPATH"

# Теперь можно использовать
python3
>>> import otterbrix
>>> client = otterbrix.Client()
```

## Пересборка после изменений

Если вы внесли изменения в код Otterbrix:

```bash
cd /home/tolisso/otterbrix/build

# Пересобрать (инкрементальная сборка)
cmake --build . --target all -- -j $(nproc)

# Если изменили CMakeLists.txt или зависимости:
cmake .. \
    -G Ninja \
    -DCMAKE_TOOLCHAIN_FILE=./build/Release/generators/conan_toolchain.cmake \
    -DCMAKE_BUILD_TYPE=Release \
    -DDEV_MODE=ON
cmake --build . --target all -- -j $(nproc)

# Полная пересборка (если что-то пошло не так)
cd /home/tolisso/otterbrix
rm -rf build
mkdir build && cd build
# Повторить процесс сборки с шага 3
```

## Оптимизации для бенчмарка

### Release сборка с максимальными оптимизациями

```bash
cd /home/tolisso/otterbrix/build

cmake .. \
    -G Ninja \
    -DCMAKE_TOOLCHAIN_FILE=./build/Release/generators/conan_toolchain.cmake \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_CXX_FLAGS="-O3 -march=native -DNDEBUG" \
    -DDEV_MODE=OFF

cmake --build . --target all -- -j $(nproc)
```

### Debug сборка для отладки

```bash
cd /home/tolisso/otterbrix
mkdir -p build_debug && cd build_debug

conan install ../conanfile.py \
    --build missing \
    -s build_type=Debug \
    -s compiler.cppstd=gnu17

cmake .. \
    -G Ninja \
    -DCMAKE_TOOLCHAIN_FILE=./build/Debug/generators/conan_toolchain.cmake \
    -DCMAKE_BUILD_TYPE=Debug \
    -DDEV_MODE=ON

cmake --build . --target all -- -j $(nproc)
```

## Решение проблем

### Проблема: "import otterbrix" не работает

**Решение:**
```bash
# Проверьте PYTHONPATH
echo $PYTHONPATH

# Убедитесь что модуль существует
ls -la /home/tolisso/otterbrix/build/integration/python/otterbrix.py
ls -la /home/tolisso/otterbrix/build/integration/python/_otterbrix.so

# Проверьте Python путь
python3 -c "import sys; print('\n'.join(sys.path))"
```

### Проблема: Ошибки компиляции

**Решение:**
```bash
# Проверьте версию компилятора
gcc --version  # Должен быть >= 9.0
g++ --version

# Обновите Conan
pip3 install --upgrade conan==2.20.0

# Очистите кеш Conan
conan remove "*" -c

# Попробуйте пересобрать с нуля
```

### Проблема: Не находятся зависимости

**Решение:**
```bash
# Убедитесь что добавлен remote репозиторий
conan remote list
# Должен быть: otterbrix: http://conan.otterbrix.com

# Если нет, добавьте
conan remote add otterbrix http://conan.otterbrix.com

# Обновите профиль
conan profile detect --force
```

### Проблема: Медленная сборка

**Решение:**
```bash
# Используйте ccache для кеширования
sudo apt install ccache

# Используйте параллельную сборку
cmake --build . -- -j $(nproc)

# Или указать конкретное число потоков
cmake --build . -- -j 8
```

## Проверка готовности для JSONBench

Перед началом работы с JSONBench убедитесь:

```bash
# 1. Модуль импортируется
export PYTHONPATH=/home/tolisso/otterbrix/build/integration/python:$PYTHONPATH
python3 -c "import otterbrix; print('OK')"

# 2. Можно создать клиента
python3 -c "
import sys
sys.path.insert(0, '/home/tolisso/otterbrix/build/integration/python')
from otterbrix import Client
client = Client()
print('✓ Client created')
"

# 3. Базовые операции работают
python3 << 'EOF'
import sys
sys.path.insert(0, '/home/tolisso/otterbrix/build/integration/python')
from otterbrix import Client

client = Client()
# Добавьте тесты базовых операций
print('✓ All basic operations work')
EOF
```

## Дополнительная информация

### Структура сборки

```
/home/tolisso/otterbrix/
├── build/                          # Директория сборки
│   ├── integration/
│   │   └── python/                 # ← Python модуль здесь
│   │       ├── otterbrix.py
│   │       ├── _otterbrix.so       # C++ биндинги
│   │       └── ...
│   ├── components/                 # Скомпилированные компоненты
│   ├── core/                       # Ядро
│   └── ...
├── integration/                    # Исходники интеграций
│   └── python/                     # Исходники Python биндингов
├── components/                     # Исходники компонентов
├── core/                           # Исходники ядра
└── CMakeLists.txt                  # Конфигурация CMake
```

### Полезные команды

```bash
# Размер собранных файлов
du -sh /home/tolisso/otterbrix/build

# Список скомпилированных целей
cmake --build build --target help

# Информация о сборке
cmake -LA build

# Очистка сборки (не удаляя конфигурацию)
cmake --build build --target clean

# Пересборка конкретной цели
cmake --build build --target otterbrix_python
```

## Следующие шаги

После успешной сборки:

1. Перейдите к [06_NEXT_STEPS.md](./06_NEXT_STEPS.md) для начала работы с JSONBench
2. Создайте тестовый скрипт из [02_OTTERBRIX_API_REFERENCE.md](./02_OTTERBRIX_API_REFERENCE.md)
3. Следуйте [03_IMPLEMENTATION_GUIDE.md](./03_IMPLEMENTATION_GUIDE.md) для создания скриптов бенчмарка

---

**Важно**: Все скрипты JSONBench для Otterbrix используют эту локальную сборку автоматически через правильную настройку PYTHONPATH.

