from datetime import date
from typing import Optional, Tuple, Type, Iterator, Dict, Any

from ..base import BaseController
from ...sources.base.quarter_based import QuarterBasedBaseSource
from ...utils.dates import convert_date_to_quarter_year_tuple


class QuarterBasedBaseController(BaseController):
    """Base controller class for quarter based data sources"""
    _source: QuarterBasedBaseSource
    _last_timestamp: Optional[date] = None

    start_quarter_and_year: Optional[Tuple[int, int]] = None
    end_quarter_and_year: Optional[Tuple[int, int]] = None
    source_class: Type[QuarterBasedBaseSource]

    @classmethod
    def _update_last_timestamp(cls):
        """Updates the last timestamp property of this class"""
        super()._update_last_timestamp()

        if isinstance(cls._last_timestamp, date):
            cls.start_quarter_and_year = convert_date_to_quarter_year_tuple(cls._last_timestamp)

    @classmethod
    def _query_source(cls, ) -> Iterator[Dict[Any, Any]]:
        """Queries the data source"""
        cls._update_source(start_quarter_and_year=cls.start_quarter_and_year,
                           end_quarter_and_year=cls.end_quarter_and_year)
        return cls._source.get()

