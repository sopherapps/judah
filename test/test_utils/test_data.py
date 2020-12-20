"""Module containing tests for the data utility functions"""

from unittest import TestCase, main
from judah.utils.data import remove_empty_string_values, replace_keys_with_aliases, replace_substrings


class TestDataUtilities(TestCase):
    """Tests for the data utility functions"""

    def test_remove_empty_string_values(self):
        """Removes empty string value, replacing it with a given replacement"""
        replacement = 'hello'
        self.assertEqual(remove_empty_string_values('', replacement=replacement), replacement)
        self.assertEqual(remove_empty_string_values(''), None)
        self.assertEqual(remove_empty_string_values('None empty', replacement=replacement), 'None empty')

    def test_replace_keys_with_aliases(self):
        """Replaces keys in a dict with aliases from a key-alias map"""
        alias_map = {
            'nom': 'name',
            'adresse': 'address'
        }
        data = [
            {'nom': 'John Doe', 'adresse': 'Hoima'},
            {'nom': 'Jean Doe', 'adresse': 'Paris'},
            {'nom': 'Nazir Doe', 'adresse': 'Cairo'},
        ]

        expect_output = [
            {'name': 'John Doe', 'address': 'Hoima'},
            {'name': 'Jean Doe', 'address': 'Paris'},
            {'name': 'Nazir Doe', 'address': 'Cairo'},
        ]

        for datum, expected_datum in zip(data, expect_output):
            processed_datum = replace_keys_with_aliases(alias_map=alias_map, data=datum)
            self.assertDictEqual(processed_datum, expected_datum)

    def test_replace_substrings(self):
        """Replaces characters in a given string with other characters"""
        substring_replacement_map = {
            'foo': 'bar',
            'tea': 'java'
        }
        phrase = '"foo" is just some word people use. It is like "tea"'
        expected_phrase = '"bar" is just some word people use. It is like "java"'

        processed_phrase = replace_substrings(substring_replacement_map=substring_replacement_map, string=phrase)

        self.assertEqual(processed_phrase, expected_phrase)


if __name__ == '__main__':
    main()
