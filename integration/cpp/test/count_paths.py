import json
import sys
from typing import Set, Any

def extract_paths(obj: Any, current_path: str, paths: Set[str]) -> None:
    """
    Рекурсивно извлекает все JSON пути из объекта.
    Пути в формате JSONPath: root.key[0].subkey
    """
    if isinstance(obj, dict):
        for key, value in obj.items():
            new_path = f"{current_path}.{key}" if current_path else key
            paths.add(new_path)
            extract_paths(value, new_path, paths)
    elif isinstance(obj, list):
        for i, value in enumerate(obj):
            new_path = f"{current_path}[{i}]" if current_path else f"[{i}]"
            paths.add(new_path)
            extract_paths(value, new_path, paths)

def count_unique_paths_from_lines(file_path: str) -> int:
    """
    Подсчитывает уникальные пути из JSON объектов, каждый на новой строке (JSONL).
    """
    all_paths: Set[str] = set()
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                
                try:
                    json_obj = json.loads(line)
                    extract_paths(json_obj, "", all_paths)
                except json.JSONDecodeError as e:
                    print(f"Ошибка в строке {line_num}: {e}", file=sys.stderr)
                    continue
        
        return len(all_paths)
    
    except FileNotFoundError:
        print(f"Файл {file_path} не найден", file=sys.stderr)
        sys.exit(1)

def main():
    if len(sys.argv) != 2:
        print("Использование: python json_paths.py input.jsonl")
        print("input.jsonl - файл с JSON объектами, каждый на новой строке")
        sys.exit(1)
    
    input_file = sys.argv[1]
    unique_count = count_unique_paths_from_lines(input_file)
    print(f"Количество уникальных путей: {unique_count}")

if __name__ == "__main__":
    main()

