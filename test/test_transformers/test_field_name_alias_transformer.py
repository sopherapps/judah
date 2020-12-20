"""Tests for the field name alias transformer"""
from typing import Dict
from unittest import TestCase, main

from judah.transformers.field_name_alias_transformer import FieldNameAliasTransformer


class TestFieldNameAliasTransformer(TestCase):
    """Tests for the FieldNameAliasTransformer base class"""

    def test_run(self):
        """
        should change the field names of the data passed to it to their aliases
        """
        input_data = {
            'nom': 'John Doe',
            'adresse': 'Katakwi',
            'email': 'johndoe@example.com'
        }

        expected_output_data = {
            'name': 'John Doe',
            'address': 'Katakwi',
            'email': 'johndoe@example.com'
        }

        class ChildFieldNameAliasTransformer(FieldNameAliasTransformer):
            _alias_map: Dict[str, str] = {
                'nom': 'name',
                'adresse': 'address'
            }

        self.assertDictEqual(ChildFieldNameAliasTransformer.run(input_data), expected_output_data)


if __name__ == '__main__':
    main()
