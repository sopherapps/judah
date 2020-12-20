"""Utility Functions centered around data"""
from typing import Any, Dict


def remove_empty_string_values(value: Any, replacement: Any = None) -> Any:
    """Converts a given empty string to None
    """
    if value == '':
        return replacement

    return value


def replace_keys_with_aliases(alias_map: Dict[str, str], data: Dict[Any, Any]):
    """Returns the data with the right aliases set"""
    return {alias_map.get(key, key): value for key, value in data.items()}


def replace_substrings(substring_replacement_map: Dict[str, str], string: str) -> str:
    """Replaces characters in a given string"""
    new_string = string

    for substring, replacement in substring_replacement_map.items():
        new_string = new_string.replace(substring, replacement)

    return new_string
