"""Tests for the field name cleaner transformer"""
from typing import Dict
from unittest import TestCase, main

from judah.transformers.field_name_cleaner_transformer import FieldNameCleanerTransformer


class TestFieldNameCleanerTransformer(TestCase):
    """Tests for the FieldNameCleanerTransformer base class"""

    def test_run(self):
        """
        should remove unwanted characters, replacing them with other characters in the field names
        """
        input_data = {
            '(nom)hey': 'John Doe',
            'address-district': 'Katakwi',
            'email': 'johndoe@example.com'
        }

        expected_output_data = {
            '_nom_hey': 'John Doe',
            'address district': 'Katakwi',
            'email': 'johndoe@example.com'
        }

        class ChildFieldNameCleanerTransformer(FieldNameCleanerTransformer):
            substring_replacement_map: Dict[str, str] = {
                '(': '_',
                ')': '_',
                '-': ' '
            }

        self.assertDictEqual(ChildFieldNameCleanerTransformer.run(input_data), expected_output_data)


if __name__ == '__main__':
    main()
