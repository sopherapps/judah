"""Module with tests for the date based REST API source"""
from datetime import date, timedelta
from typing import Optional, Dict, Any
from unittest import TestCase, main
from unittest.mock import patch, call

import requests
from pydantic import BaseModel

from judah.sources.rest_api.date_based import DateBasedRestAPISource


class MockResponse(BaseModel):
    ok: bool = True
    status_code: int = 200
    mock_data: Optional[Dict[Any, Any]] = None

    def json(self, *args, **kwargs):
        """Dummy json method"""
        return self.mock_data


class TestDateBasedRestApiSource(TestCase):
    """Tests for the DateBasedRestApiSource"""

    def setUp(self) -> None:
        """Initialize some variables"""
        self.options = dict(response_data_key='results',
                            default_headers={
                                "Host": "judah.github.com"
                            },
                            start_date_param_name="start_date",
                            end_date_param_name="end_date",
                            date_format='%Y-%m-%d',
                            end_date=date.today(),
                            start_date=date.today() - timedelta(days=1))

    def test_get_with_params(self):
        """Makes a GET request with the params from the _get_params method"""
        mock_data = {'results': [{'foo': 'bar'}, {'foo2': 'bar2'}]}
        dummy_params = {'foo': 'bar', 'sample': 6}

        class DateBasedRestApiSourceWithParams(DateBasedRestAPISource):
            base_uri: Optional[str] = 'http://example.com'
            name: str = 'sample'

            def _get_params(self, **kwargs) -> Dict[Any, Any]:
                return dummy_params

        with patch.object(requests, 'get',
                          return_value=MockResponse(mock_data=mock_data)) as mock_get:
            source = DateBasedRestApiSourceWithParams(**self.options)
            data = list(source.get())

            url = f'{source.base_uri}/{source.name}'
            mock_get.assert_called_with(url=url, headers=self.options['default_headers'], params=dummy_params)

            self.assertListEqual(data, mock_data['results'])

    def test_get_with_default_params(self):
        """Makes a GET request with the default params of start_date and end_date"""
        mock_data = {'results': [{'foo': 'bar'}, {'foo2': 'bar2'}]}
        default_params = {
            self.options['start_date_param_name']: self.options['start_date'].strftime(self.options['date_format']),
            self.options['end_date_param_name']: self.options['end_date'].strftime(self.options['date_format'])}

        class DateBasedRestApiSourceWithDefaultParams(DateBasedRestAPISource):
            base_uri: Optional[str] = 'http://example.com'
            name: str = 'sample'

        with patch.object(requests, 'get',
                          return_value=MockResponse(mock_data=mock_data)) as mock_get:
            source = DateBasedRestApiSourceWithDefaultParams(**self.options)
            data = list(source.get())

            url = f'{source.base_uri}/{source.name}'
            mock_get.assert_called_with(url=url, headers=self.options['default_headers'], params=default_params)

            self.assertListEqual(data, mock_data['results'])

    def test_get_with_headers(self):
        """Makes a GET request with the headers from the _get_headers method"""
        mock_data = {'results': [{'foo': 'bar'}, {'foo2': 'bar2'}]}
        dummy_headers = {'foo': 'bar', 'sample': 6}

        class DateBasedRestApiSourceWithHeaders(DateBasedRestAPISource):
            base_uri: Optional[str] = 'http://example.com'
            name: str = 'sample'

            def _get_headers(self) -> Dict[Any, Any]:
                return dummy_headers

            def _get_params(self, **kwargs) -> Dict[Any, Any]:
                return {}

        with patch.object(requests, 'get',
                          return_value=MockResponse(mock_data=mock_data)) as mock_get:
            source = DateBasedRestApiSourceWithHeaders(**self.options)
            data = list(source.get())

            url = f'{source.base_uri}/{source.name}'
            mock_get.assert_called_with(url=url, headers=dummy_headers, params={})

            self.assertListEqual(data, mock_data['results'])

    def test_get_with_default_headers(self):
        """Makes a GET request with the default headers passed during initialization"""
        mock_data = {'results': [{'foo': 'bar'}, {'foo2': 'bar2'}]}

        class DateBasedRestApiSourceWithDefaultHeaders(DateBasedRestAPISource):
            base_uri: Optional[str] = 'http://example.com'
            name: str = 'sample'

            def _get_params(self, **kwargs) -> Dict[Any, Any]:
                return {}

        with patch.object(requests, 'get',
                          return_value=MockResponse(mock_data=mock_data)) as mock_get:
            source = DateBasedRestApiSourceWithDefaultHeaders(**self.options)
            data = list(source.get())

            url = f'{source.base_uri}/{source.name}'
            mock_get.assert_called_with(url=url, headers=self.options['default_headers'], params={})

            self.assertListEqual(data, mock_data['results'])

    def test_get_with_authentication(self):
        """
        Makes a GET request with the appropriate authorization header,
        and attempts again if the authorization is expired
        """
        mock_data = {'results': [{'foo': 'bar'}, {'foo2': 'bar2'}]}

        def mocked_get_method(url, params, headers, **kwargs):
            if 'Authorization' in headers:
                return MockResponse(mock_data=mock_data)
            return MockResponse(status_code=401, ok=False)

        class DateBasedRestApiSourceWithAuthentication(DateBasedRestAPISource):
            base_uri: Optional[str] = 'http://example.com'
            name: str = 'sample'

            def _authenticate(self):
                self.default_headers['Authorization'] = 'Bearer some token'

            def _get_params(self, **kwargs) -> Dict[Any, Any]:
                return {}

        with patch.object(requests, 'get', side_effect=mocked_get_method) as mock_get:
            source = DateBasedRestApiSourceWithAuthentication(**self.options)
            data = list(source.get())

            uri = f'{source.base_uri}/{source.name}'
            calls = [call(url=uri, headers=self.options['default_headers'], params={}),
                     call(url=uri, headers={'Authorization': 'Bearer some token',
                                            **self.options['default_headers']}, params={})]
            mock_get.assert_has_calls(calls, any_order=False)

            self.assertListEqual(data, mock_data['results'])


if __name__ == '__main__':
    main()
