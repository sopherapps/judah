"""Tests for the field value alias transformer"""
from typing import Dict
from unittest import TestCase, main

from judah.transformers.field_value_alias_transformer import FieldValueAliasTransformer


class TestFieldValueAliasTransformer(TestCase):
    """Tests for the FieldValueAliasTransformer base class"""

    def test_run(self):
        """
        should change the field values of the data passed to it to their aliases
        """
        first_input_data = {
            'name': 'John Doe',
            'address': 'Katakwi',
            'email': 'johndoe@example.com'
        }

        first_expected_output_data = {
            'name': 'Anonymous',
            'address': 'Katakwi',
            'email': 'johndoe@example.com'
        }

        second_input_data = {
            'name': 'Peter Doe',
            'address': 'Hoima',
            'email': 'peterdoe@example.com'
        }

        second_expected_output_data = {
            'name': 'Peter Doe',
            'address': 'Hoima',
            'email': 'peterdoe@example.com'
        }

        class ChildFieldValueAliasTransformer(FieldValueAliasTransformer):
            _alias_map: Dict[str, str] = {
                'John Doe': 'Anonymous',
            }
            _field_name: str = 'name'

        self.assertDictEqual(ChildFieldValueAliasTransformer.run(first_input_data), first_expected_output_data)
        self.assertDictEqual(ChildFieldValueAliasTransformer.run(second_input_data), second_expected_output_data)


if __name__ == '__main__':
    main()
