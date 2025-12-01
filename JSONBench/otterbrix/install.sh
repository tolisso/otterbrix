#!/bin/bash

echo "Setting up Otterbrix from local build..."

# Путь к локальной сборке Otterbrix
OTTERBRIX_ROOT="/home/tolisso/otterbrix"
OTTERBRIX_PYTHON_PATH="$OTTERBRIX_ROOT/build/integration/python"

# Проверка Python
if ! command -v python3 &> /dev/null; then
    echo "Error: Python 3 is required"
    exit 1
fi

echo "Python version:"
python3 --version

# Проверка наличия собранного Otterbrix
if [[ ! -d "$OTTERBRIX_PYTHON_PATH" ]]; then
    echo "Error: Otterbrix Python module not found at $OTTERBRIX_PYTHON_PATH"
    echo "Please build Otterbrix first:"
    echo "  cd $OTTERBRIX_ROOT"
    echo "  mkdir -p build && cd build"
    echo "  conan install ../conanfile.py --build missing -s build_type=Release -s compiler.cppstd=gnu17"
    echo "  cmake .. -G Ninja -DCMAKE_TOOLCHAIN_FILE=./build/Release/generators/conan_toolchain.cmake -DCMAKE_BUILD_TYPE=Release -DDEV_MODE=ON"
    echo "  cmake --build . --target all -- -j \$(nproc)"
    exit 1
fi

# Проверка установки
if python3 -c "import sys; sys.path.insert(0, '$OTTERBRIX_PYTHON_PATH'); import otterbrix" 2>/dev/null; then
    echo "✓ Otterbrix found and can be imported"
    echo "Build path: $OTTERBRIX_PYTHON_PATH"
else
    echo "✗ Failed to import Otterbrix from local build"
    exit 1
fi

echo ""
echo "✓ Otterbrix setup completed successfully!"
echo ""
echo "Note: Make sure to start Otterbrix server before running benchmarks:"
echo "  cd $OTTERBRIX_ROOT/build"
echo "  ./otterbrix_server"


