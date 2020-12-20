"""Tests for the datetime transformer"""
from unittest import TestCase, main

from judah.transformers.datetime_transformer import DatetimeTransformer


class TestDatetimeTransformer(TestCase):
    """Tests for the DatetimeTransformer base class"""

    def test_run(self):
        """
        should transform the format of the date field in a given data dictionary
        """
        input_data = {
            'start_date': '09/24/2020 11:45:32',
            'amount': 5789
        }

        expected_output_data = {
            'start_date': '2020-09-24 11-45-32',
            'amount': 5789
        }

        class ChildDatetimeTransformer(DatetimeTransformer):
            datetime_field: str = 'start_date'
            source_datetime_format: str = '%m/%d/%Y %H:%M:%S'
            destination_datetime_format: str = '%Y-%m-%d %H-%M-%S'

        self.assertDictEqual(ChildDatetimeTransformer.run(input_data), expected_output_data)


if __name__ == '__main__':
    main()
