"""Module containing configurations for the date-based REST API data source"""

from datetime import date
from typing import Iterator, Dict, Any, Optional

import requests

from ..base.date_based import DateBasedBaseSource
from ...utils.dates import convert_date_to_string


class RequestError(BaseException):
    """Exception that occurs when a request fails"""

    def __init__(self, status_code: int, message: Optional[str] = ''):
        self.status_code = status_code
        self.message = message
        super().__init__(f'{status_code}: {self.message}')


class DateBasedRestAPISource(DateBasedBaseSource):
    """Base class for the REST API source that depends on dates"""
    response_data_key: Optional[str] = None
    default_headers: Optional[Dict[Any, Any]] = {}
    start_date_param_name: Optional[str] = None
    end_date_param_name: Optional[str] = None
    date_format: str = '%Y-%m-%d'
    timezone: Optional[str] = None

    def _authenticate(self):
        """A method to generate headers for authorization and update the default_headers"""
        pass

    def _get_headers(self) -> Dict[Any, Any]:
        """An overridable method to return headers use in the request"""
        return self.default_headers.copy()

    def _get_params(self, **kwargs) -> Dict[Any, Any]:
        """An overridable method to return query params dict to use in the request"""
        params = {**kwargs}

        if isinstance(self.start_date_param_name, str):
            start_date: Optional[date] = params.get(self.start_date_param_name, None)

            if start_date is not None:
                params[self.start_date_param_name] = convert_date_to_string(
                    date_value=start_date, date_format=self.date_format, timezone_name=self.timezone)

        if isinstance(self.end_date_param_name, str):
            end_date: Optional[date] = params.get(self.end_date_param_name, None)

            if end_date is not None:
                params[self.end_date_param_name] = convert_date_to_string(
                    date_value=end_date, date_format=self.date_format, timezone_name=self.timezone)

        return params

    def _get_data_url(self):
        """Gets the url for given data from the REST API"""
        if self.base_uri is None or self.name is None:
            raise Exception("base_uri and name attributes should not be None")

        return f"{self.base_uri}/{self.name}"

    def _query_url(self, url: str, start_date: date, end_date: date) -> requests.Response:
        """Make a requests GET request for a given set of dates"""
        headers = self._get_headers()
        params = self._get_params(**{self.start_date_param_name: start_date, self.end_date_param_name: end_date})
        return requests.get(url=url, headers=headers, params=params)

    def _extract_list_from_response(self, response: requests.Response):
        """Extracts the list data from the requests.Response object"""
        json_response = response.json()

        if self.response_data_key is not None:
            json_response = json_response.get(self.response_data_key)

        if isinstance(json_response, dict):
            yield json_response
        else:
            yield from json_response

    def _query_data_source(self, start_date: date, end_date: date) -> Iterator[Dict[str, Any]]:
        """Queries a given start and end date and returns an iterator with data records"""
        url = self._get_data_url()
        response = self._query_url(url=url, start_date=start_date, end_date=end_date)

        if response.status_code in (401, 403, 400,):
            self._authenticate()
            response = self._query_url(url=url, start_date=start_date, end_date=end_date)

        if response.ok:
            yield from self._extract_list_from_response(response=response)
        else:
            raise RequestError(message=response.text, status_code=response.status_code)
