"""Module containing a base source class"""

from datetime import date
from typing import Optional, List, Dict, Any, Iterator

from pydantic import BaseModel


class BaseSource(BaseModel):
    base_uri: Optional[str] = None
    name: Optional[str] = None
    attributes: List[str] = None

    def get(self) -> Iterator[Dict[str, Any]]:
        """Returns data from a given date to a given date as an iterator"""
        yield from self._query_data_source()

    def _query_data_source(self, *args, **kwargs) -> Iterator[Dict[str, Any]]:
        """Queries a given start and end date and returns an iterator with data records"""
        raise NotImplementedError(
            'Implement the _query_data_source method to return an iterator of dictionaries')
