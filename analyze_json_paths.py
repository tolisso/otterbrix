#!/usr/bin/env python3
"""
Анализирует NDJSON файл и выводит все JSON paths к значениям с их частотой.
"""

import json
import sys
from collections import defaultdict
from typing import Any, Dict, Set


def extract_leaf_paths(obj: Any, prefix: str = "") -> Set[str]:
    """
    Рекурсивно извлекает пути только к leaf-значениям (примитивам).
    
    Args:
        obj: JSON объект
        prefix: Текущий префикс пути
    
    Returns:
        Множество путей к примитивным значениям
    """
    paths = set()
    
    if isinstance(obj, dict):
        for key, value in obj.items():
            current_path = f"{prefix}.{key}" if prefix else key
            
            if isinstance(value, dict):
                # Рекурсивно обрабатываем вложенный объект
                paths.update(extract_leaf_paths(value, current_path))
            elif isinstance(value, list):
                # Обрабатываем элементы массива
                for idx, item in enumerate(value):
                    if isinstance(item, (dict, list)):
                        paths.update(extract_leaf_paths(item, f"{current_path}[{idx}]"))
                    else:
                        # Примитивное значение в массиве
                        paths.add(f"{current_path}[{idx}]")
            else:
                # Примитивное значение (string, number, bool, null)
                paths.add(current_path)
                
    elif isinstance(obj, list):
        for idx, item in enumerate(obj):
            if isinstance(item, (dict, list)):
                paths.update(extract_leaf_paths(item, f"{prefix}[{idx}]"))
            else:
                paths.add(f"{prefix}[{idx}]")
    
    return paths


def analyze_ndjson(filepath: str) -> Dict[str, int]:
    """
    Анализирует NDJSON файл и подсчитывает частоту каждого пути к значению.
    """
    path_counts = defaultdict(int)
    
    with open(filepath, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            
            doc = json.loads(line)
            paths = extract_leaf_paths(doc)
            
            for path in paths:
                path_counts[path] += 1
    
    return dict(path_counts)


def main():
    if len(sys.argv) != 2:
        print("Usage: python3 analyze_json_paths.py <ndjson_file>")
        sys.exit(1)
    
    filepath = sys.argv[1]
    path_counts = analyze_ndjson(filepath)
    
    # Сортируем по алфавиту
    for path in sorted(path_counts.keys()):
        print(f"{path}: {path_counts[path]}")


if __name__ == "__main__":
    main()
