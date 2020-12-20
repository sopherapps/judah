"""Module containing a data transformer that exchanges field names for their aliases"""
from typing import Dict, Any

from .base import BaseTransformer
from ..utils.data import replace_keys_with_aliases


class FieldNameAliasTransformer(BaseTransformer):
    """Changes the field names of the data passed to it to their aliases"""
    _alias_map: Dict[str, str] = {}

    @classmethod
    def run(cls, data: Dict[Any, Any]) -> Dict[Any, Any]:
        """Replaces the keys of the data to new ones"""
        return replace_keys_with_aliases(alias_map=cls._alias_map, data=data)
