"""Module containing tests for the dates utility functions"""
from datetime import datetime, date
from typing import Any
from unittest import TestCase, main
from unittest.mock import patch

import pytz

from judah.utils.dates import (
    convert_date_to_string,
    get_default_start_year,
    get_default_start_month,
    get_quarter_from_month,
    get_default_start_quarter,
    get_current_year,
    get_current_quarter,
    get_default_historical_start_date,
    get_default_historical_start_quarter_and_year,
    update_date,
    update_quarter_year_tuple,
    convert_date_to_quarter_year_tuple,
    change_datetime_format
)


class TestDatesUtilities(TestCase):
    """Tests for the dates utility functions"""

    def setUp(self) -> None:
        """Initialize some variables"""
        self.month = 11
        self.day = 11
        self.year = 2020
        self.hour = 22
        self.minute = 10
        self.second = 33
        self.timezone_name = "Africa/Kampala"
        self.utc_offset_hours = 3
        self.timezone = pytz.timezone(self.timezone_name)
        self.datetime_format = "%Y/%m/%dT%H:%M:%S%z"
        self.month_quarter_map = {1: 1, 2: 1, 3: 1, 4: 2, 5: 2, 6: 2, 7: 3, 8: 3, 9: 3, 10: 4, 11: 4, 12: 4}

    def test_convert_date_to_string(self):
        """Converts a date to the expected string format defaulting to UTC timezone and no timezone separator"""
        test_datetime = datetime(
            year=self.year, month=self.month,
            day=self.day,
            tzinfo=pytz.utc)
        date_string = f'{self.year}/{self.month}/{self.day}'
        expected_datetime_string = f'{date_string}T00:00:00+0000'

        self.assertEqual(convert_date_to_string(test_datetime, self.datetime_format), expected_datetime_string)

    def test_convert_date_to_string_with_timezone(self):
        """Converts a date to the expected string format with given timezone and no timezone separator"""
        test_datetime = datetime(
            year=self.year, month=self.month,
            day=self.day,
            tzinfo=pytz.utc).astimezone(self.timezone)
        date_string = f'{self.year}/{self.month}/{self.day}'
        utc_offset_hours_string = f'{self.utc_offset_hours}'.zfill(2)
        expected_datetime_string = f'{date_string}T00:00:00+{utc_offset_hours_string}00'

        returned_datetime_string = convert_date_to_string(
            test_datetime, self.datetime_format, timezone_name=self.timezone_name)

        self.assertEqual(returned_datetime_string, expected_datetime_string)

    def test_convert_date_to_string_with_timezone_separator(self):
        """Converts a date to the expected string format defaulting to UTC timezone and given timezone separator"""
        test_datetime = datetime(
            year=self.year, month=self.month,
            day=self.day,
            tzinfo=pytz.utc)
        date_string = f'{self.year}/{self.month}/{self.day}'
        timezone_separator = ':'
        expected_datetime_string = f'{date_string}T00:00:00+00{timezone_separator}00'

        returned_datetime_string = convert_date_to_string(
            test_datetime, self.datetime_format, timezone_separator=timezone_separator)

        self.assertEqual(returned_datetime_string, expected_datetime_string)

    @patch('os.getenv')
    def test_get_default_start_year(self, mock_os_getenv):
        """
        Should return the HISTORICAL_STARTING_YEAR from the environment
        """
        test_start_year = 1999
        mock_os_getenv.return_value = f'{test_start_year}'

        self.assertEqual(get_default_start_year(), test_start_year)
        mock_os_getenv.assert_called_once_with('HISTORICAL_STARTING_YEAR', None)

    @patch('os.getenv')
    def test_get_default_start_month(self, mock_os_getenv):
        """
        Should return the HISTORICAL_STARTING_MONTH from the environment
        """
        test_start_month = 5
        mock_os_getenv.return_value = f'{test_start_month}'

        self.assertEqual(get_default_start_month(), test_start_month)
        mock_os_getenv.assert_called_once_with('HISTORICAL_STARTING_MONTH', None)

    @patch('judah.utils.dates.get_default_start_month')
    def test_get_default_start_quarter(self, mock_get_default_start_month):
        """
        Should return the quarter in which the given default start month belongs to
        """
        for month, quarter in self.month_quarter_map.items():
            mock_get_default_start_month.return_value = month
            self.assertEqual(get_default_start_quarter(), quarter)

    def test_get_quarter_from_month(self):
        """
        Should return the quarter in which the given start month belongs to
        """
        for month, quarter in self.month_quarter_map.items():
            self.assertEqual(get_quarter_from_month(month), quarter)

    def test_get_current_year(self):
        """
        Should return the current year
        """
        current_year = datetime.now().year
        self.assertEqual(get_current_year(), current_year)

    def test_get_current_quarter(self):
        """
        Should return the quarter in which the current month is in
        """
        current_month = datetime.now().month
        current_quarter = self.month_quarter_map[current_month]
        self.assertEqual(get_current_quarter(), current_quarter)

    @patch('os.getenv')
    def test_get_default_historical_start_date(self, mock_os_getenv):
        """
        Should return the date object got from
        HISTORICAL_STARTING_YEAR, HISTORICAL_STARTING_MONTH and HISTORICAL_STARTING_DAY
        """
        test_year = 1998
        test_month = 4
        test_day = 7

        def handle_os_getenv(string: str, default: Any):
            return_map = {
                'HISTORICAL_STARTING_YEAR': f'{test_year}',
                'HISTORICAL_STARTING_MONTH': f'{test_month}',
                'HISTORICAL_STARTING_DAY': f'{test_day}'
            }
            return return_map[string]

        mock_os_getenv.side_effect = handle_os_getenv
        expected_date = date(year=test_year, month=test_month, day=test_day)

        self.assertEqual(get_default_historical_start_date(), expected_date)

    @patch('os.getenv')
    def test_get_default_historical_start_quarter_and_year(self, mock_os_getenv):
        """
        Should return the quarter, year tuple got from
        HISTORICAL_STARTING_YEAR, HISTORICAL_STARTING_MONTH
        """
        test_year = 1997
        test_month = 7

        def handle_os_getenv(string: str, default: Any):
            return_map = {
                'HISTORICAL_STARTING_YEAR': f'{test_year}',
                'HISTORICAL_STARTING_MONTH': f'{test_month}',
            }
            return return_map[string]

        mock_os_getenv.side_effect = handle_os_getenv
        expected_quarter_and_month = (self.month_quarter_map[test_month], test_year,)

        self.assertEqual(get_default_historical_start_quarter_and_year(), expected_quarter_and_month)

    def test_update_date(self):
        """
        Should increment or decrement date accordingly
        """
        test_date = date(year=2020, month=3, day=8)
        days_to_increment_by = 31
        days_to_decrement_by = 8

        last_day_of_february = date(year=2020, month=2, day=29)
        test_date_after_31_days = date(year=2020, month=4, day=8)
        test_date_after_23_days = date(year=2020, month=3, day=31)

        incremented_date = update_date(
            test_date, days_to_decrement_by=0, days_to_increment_by=days_to_increment_by)
        decremented_date = update_date(
            test_date, days_to_decrement_by=days_to_decrement_by, days_to_increment_by=0)
        incremented_and_decremented_date = update_date(
            test_date, days_to_decrement_by=days_to_decrement_by, days_to_increment_by=days_to_increment_by)

        self.assertEqual(decremented_date, last_day_of_february)
        self.assertEqual(incremented_date, test_date_after_31_days)
        self.assertEqual(incremented_and_decremented_date, test_date_after_23_days)

    def test_update_quarter_year_tuple(self):
        """
        Should increment or decrement a (quarter, year,) tuple accordingly
        """
        test_quarter_year_tuple = (3, 2020,)
        quarters_to_increment_by = 4
        quarters_to_decrement_by = 3

        last_quarter_year_tuple_of_2019 = (4, 2019,)
        test_quarter_year_tuple_after_4_quarters = (3, 2021,)
        test_quarter_year_tuple_after_1_quarter = (4, 2020,)

        incremented_quarter_year_tuple = update_quarter_year_tuple(
            test_quarter_year_tuple, quarters_to_increment_by=quarters_to_increment_by, quarters_to_decrement_by=0)
        decremented_quarter_year_tuple = update_quarter_year_tuple(
            test_quarter_year_tuple, quarters_to_increment_by=0, quarters_to_decrement_by=quarters_to_decrement_by)
        incremented_and_decremented_quarter_year_tuple = update_quarter_year_tuple(
            test_quarter_year_tuple, quarters_to_increment_by=quarters_to_increment_by,
            quarters_to_decrement_by=quarters_to_decrement_by)

        self.assertEqual(incremented_quarter_year_tuple, test_quarter_year_tuple_after_4_quarters)
        self.assertEqual(decremented_quarter_year_tuple, last_quarter_year_tuple_of_2019)
        self.assertEqual(incremented_and_decremented_quarter_year_tuple, test_quarter_year_tuple_after_1_quarter)

    def test_convert_date_to_quarter_year_tuple(self):
        """
        Should convert a given date to a quarter_year tuple
        """
        date_quarter_map = {
            date(year=2020, month=3, day=20): 1,
            date(year=2010, month=5, day=2): 2,
            date(year=1990, month=7, day=30): 3,
            date(year=2020, month=10, day=10): 4,
        }

        for test_date, quarter in date_quarter_map.items():
            self.assertEqual(convert_date_to_quarter_year_tuple(test_date), (quarter, test_date.year))

    def test_change_datetime_format(self):
        """
        Should convert a date string's format to another format
        """
        original_format = '%Y-%m-%d %H:%M:%S%z'
        new_format = '%Y/%d/%m/%dT%H-%M-%S%Z'
        original_date_string = '2002-04-03 12:38:42+0000'
        expected_date_string = '2002/03/04/03T12-38-42UTC'

        returned_date_string = change_datetime_format(
            datetime_value=original_date_string, old=original_format, new=new_format)

        self.assertEqual(returned_date_string, expected_date_string)


if __name__ == '__main__':
    main()
