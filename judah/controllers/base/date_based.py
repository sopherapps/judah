from datetime import date
from typing import Optional, Type, Iterator, Dict, Any

from ..base import BaseController
from ...sources.base.date_based import DateBasedBaseSource


class DateBasedBaseController(BaseController):
    """Base controller class for date based data sources"""
    _source: DateBasedBaseSource
    _last_timestamp: Optional[date] = None

    start_date: Optional[date] = None
    end_date: Optional[date] = None
    source_class: Type[DateBasedBaseSource]

    @classmethod
    def _update_last_timestamp(cls):
        """Updates the last timestamp property of this class"""
        super()._update_last_timestamp()

        if isinstance(cls._last_timestamp, date):
            cls.start_date = cls._last_timestamp

    @classmethod
    def _query_source(cls, ) -> Iterator[Dict[Any, Any]]:
        """Queries the data source"""
        cls._update_source(start_date=cls.start_date, end_date=cls.end_date)
        return cls._source.get()
