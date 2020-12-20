"""Module for transforming the data field names by removing unwanted characters"""
from typing import Dict, Any

from .base import BaseTransformer
from ..utils.data import replace_substrings


class FieldNameCleanerTransformer(BaseTransformer):
    """Removes unwanted characters, replacing them with other characters in the field names"""
    substring_replacement_map: Dict[str, str] = {}

    @classmethod
    def run(cls, data: Dict[Any, Any]) -> Dict[Any, Any]:
        """Replaces unwanted characters with wanted characters"""
        return {
            replace_substrings(substring_replacement_map=cls.substring_replacement_map, string=str(key)): value
            for key, value in data.items()
        }
