"""Module containing the base transformer for data"""
from typing import Dict, Any, Union, List


class BaseTransformer:
    """
    The base class for all data transformers that receive a dictionary
    and return a transformed dictionary
    """

    @classmethod
    def run(cls, data: Dict[Any, Any]) -> Union[Dict[Any, Any], List[Any]]:
        """This transforms the data"""
        return data
