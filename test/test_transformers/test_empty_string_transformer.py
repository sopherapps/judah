"""Tests for the empty string transformer"""
from typing import Any
from unittest import TestCase, main

from judah.transformers.empty_string_transformer import EmptyStringTransformer


class TestEmptyStringTransformer(TestCase):
    """Tests for the EmptyStringTransformer base class"""

    def test_run(self):
        """
        should transform empty strings to some thing else
        """
        test_replacement = 'hello world'
        input_data = {
            'name': '',
            'address': 'Katakwi',
            'email': ''
        }

        expected_output_data = {
            'name': test_replacement,
            'address': 'Katakwi',
            'email': test_replacement
        }

        class ChildEmptyStringTransformer(EmptyStringTransformer):
            replacement: Any = test_replacement

        self.assertDictEqual(ChildEmptyStringTransformer.run(input_data), expected_output_data)


if __name__ == '__main__':
    main()
