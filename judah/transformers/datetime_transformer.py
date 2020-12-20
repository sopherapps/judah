"""Module contains the class that transforms data by formatting the date"""
from datetime import datetime
from typing import Dict, Any, List, Union

from .base import BaseTransformer
from ..utils.dates import change_datetime_format


class DatetimeTransformer(BaseTransformer):
    """Transforms the format of the date field in a given data dictionary"""
    datetime_field: str = 'Date'
    source_datetime_format: str = '%m/%d/%Y'
    destination_datetime_format: str = '%Y-%m-%d'

    @classmethod
    def run(cls, data: Dict[Any, Any]):
        """Transforms the dates to the right format"""
        data_copy = data.copy()
        date_value = data_copy.get(cls.datetime_field, None)

        if date_value is not None:
            data_copy[cls.datetime_field] = change_datetime_format(
                datetime_value=date_value, old=cls.source_datetime_format,
                new=cls.destination_datetime_format)

        return data_copy


class MultipleFormatDatetimeTransformer(BaseTransformer):
    """
    Transforms the format of the datetime field in a given data dictionary
    from a number of possible source formats
    """
    datetime_field: str = 'Date'
    source_datetime_formats: List[str] = ['%m/%d/%Y']
    destination_datetime_format: str = '%Y-%m-%d'

    @classmethod
    def __format_datetime(cls, datetime_value: str) -> Union[str, datetime]:
        """Attempts to format the value but using the many possible source datetime formats"""
        new_value = datetime_value

        for old_datetime_format in cls.source_datetime_formats:
            try:
                new_value = change_datetime_format(
                    datetime_value=datetime_value, old=old_datetime_format,
                    new=cls.destination_datetime_format)
                break
            except ValueError:
                pass

        return new_value

    @classmethod
    def run(cls, data: Dict[Any, Any]):
        """Transforms the dates to the right format"""
        data_copy = data.copy()
        datetime_value = data_copy.get(cls.datetime_field, None)

        if datetime_value is not None:
            data_copy[cls.datetime_field] = cls.__format_datetime(datetime_value=datetime_value)

        return data_copy
