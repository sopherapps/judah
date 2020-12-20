from typing import Optional, Tuple, Iterator, Dict, Any

from ..base import BaseSource
from ...utils.dates import get_current_quarter, get_current_year, update_quarter_year_tuple, \
    get_default_start_quarter, get_default_start_year


class QuarterBasedBaseSource(BaseSource):
    start_quarter_and_year: Optional[Tuple[int, int]] = None
    end_quarter_and_year: Optional[Tuple[int, int]] = None
    default_batch_size_in_quarters: int = 1

    def get(self) -> Iterator[Dict[str, Any]]:
        """Returns data from a given date to a given date as an iterator"""
        start_quarter, start_year = self._get_next_start_quarter_and_year()
        end_quarter, end_year = self._get_next_end_quarter_and_year()

        if end_year < start_year:
            yield from []
        elif end_quarter < start_quarter and end_year <= start_year:
            yield from []
        else:
            yield from self._query_data_source(
                start_quarter_and_year=(start_quarter, start_year,),
                end_quarter_and_year=(end_quarter, end_year,))

            self.start_quarter_and_year = self._get_next_start_quarter_and_year(
                quarters_to_increment_by=self.default_batch_size_in_quarters)

    def _query_data_source(
            self, start_quarter_and_year: Tuple[int, int],
            end_quarter_and_year: Tuple[int, int]) -> Iterator[Dict[str, Any]]:
        """Queries a source given start and end quarter_year tuples and returns an iterator with data records"""
        raise NotImplementedError('Implement the _query_data_source method to return an iterator of dictionaries')

    def _get_next_end_quarter_and_year(self,
                                       quarters_to_increment_by: Optional[int] = 0,
                                       quarters_to_decrement_by: Optional[int] = 0) -> Tuple[int, int]:
        """Gets the next (end quarter, end year) tuple in the given iteration"""
        if isinstance(self.end_quarter_and_year, tuple):
            end_quarter, end_year = self.end_quarter_and_year
        elif isinstance(self.start_quarter_and_year, tuple):
            end_quarter, end_year = update_quarter_year_tuple(
                self.start_quarter_and_year,
                quarters_to_decrement_by=0,
                quarters_to_increment_by=self.default_batch_size_in_quarters)
        else:
            end_quarter, end_year = (get_current_quarter(), get_current_year(),)

        return update_quarter_year_tuple(quarter_year_tuple=(end_quarter, end_year,),
                                         quarters_to_increment_by=quarters_to_increment_by,
                                         quarters_to_decrement_by=quarters_to_decrement_by)

    def _get_next_start_quarter_and_year(self,
                                         quarters_to_increment_by: Optional[int] = 0,
                                         quarters_to_decrement_by: Optional[int] = 0) -> Tuple[int, int]:
        """Gets the next (start quarter, start year) tuple in the given iteration"""
        if isinstance(self.start_quarter_and_year, tuple):
            start_quarter, start_year = self.start_quarter_and_year
        elif isinstance(self.end_quarter_and_year, tuple):
            start_quarter, start_year = update_quarter_year_tuple(
                self.end_quarter_and_year,
                quarters_to_decrement_by=self.default_batch_size_in_quarters,
                quarters_to_increment_by=0)
        else:
            # default the end_quarter_and_year tuple to current quarter, year
            end_quarter_and_year_tuple = (get_current_quarter(), get_current_year())
            start_quarter, start_year = update_quarter_year_tuple(
                end_quarter_and_year_tuple,
                quarters_to_decrement_by=self.default_batch_size_in_quarters,
                quarters_to_increment_by=0)

        return update_quarter_year_tuple(quarter_year_tuple=(start_quarter, start_year,),
                                         quarters_to_increment_by=quarters_to_increment_by,
                                         quarters_to_decrement_by=quarters_to_decrement_by)
