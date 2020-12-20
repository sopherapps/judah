"""Module containing tests for the quarter based Exports Site data source"""
import os
from collections import Iterator
from datetime import date
from typing import Optional
from unittest import TestCase, main
from unittest.mock import patch, Mock, call

from selenium import webdriver

from judah.sources.export_site.quarter_based import QuarterBasedExportSiteSource
from judah.utils.dates import convert_date_to_quarter_year_tuple, update_quarter_year_tuple

_PARENT_FOLDER = os.path.dirname(__file__)
_MOCK_ASSET_FOLDER_PATH = os.path.join(os.path.dirname(os.path.dirname(_PARENT_FOLDER)), 'assets')


class ChildQuarterBasedExportSiteSource(QuarterBasedExportSiteSource):
    """Child export site source that just picks a file from the file system"""
    base_uri: str = 'http://example.com'
    name: str = 'test_export_site_source'

    def _download_file(self, start_date: date, end_date: date) -> Optional[str]:
        """Downloads the CSV from the export site and returns the path to it"""
        return None


class TestQuarterBasedExportSiteSource(TestCase):
    """Tests for the QuarterBasedExportSiteSource"""

    def setUp(self) -> None:
        """Initialize some variables"""
        self.mock_csv_file_path = os.path.join(_MOCK_ASSET_FOLDER_PATH, 'mock.csv')
        self.expected_data = [
            {"Date": "09/03/2020", "number": "1", "period_from": "00:00", "period_until": "00:15", "Capacity": "16616"},
            {"Date": "09/03/2020", "number": "2", "period_from": "00:15", "period_until": "00:30", "Capacity": "16616"},
            {"Date": "09/03/2020", "number": "3", "period_from": "00:30", "period_until": "00:45", "Capacity": "16616"},
            {"Date": "09/03/2020", "number": "4", "period_from": "00:45", "period_until": "01:00", "Capacity": "16620"},
        ]

    @patch('judah.sources.export_site.quarter_based.visit_website')
    def test_initialize_chrome(self, mock_visit_website):
        """
        Should initialize Chrome in case it is not yet initialized and visits the base url
        """
        quarter_based_export_site_source = ChildQuarterBasedExportSiteSource()
        self.assertIsNone(quarter_based_export_site_source.chrome)

        quarter_based_export_site_source._initialize_chrome()

        self.assertIsInstance(quarter_based_export_site_source.chrome, webdriver.Chrome)
        mock_visit_website.assert_called_once_with(
            driver=quarter_based_export_site_source.chrome, website_url=quarter_based_export_site_source.base_uri)

        quarter_based_export_site_source.chrome.close()
        quarter_based_export_site_source.chrome.quit()

    @patch.object(ChildQuarterBasedExportSiteSource, '_initialize_chrome')
    def test_del(self, mock_initialize_chrome):
        """
        Should quit chrome on deletion
        """
        quarter_based_export_site_source = ChildQuarterBasedExportSiteSource()
        quarter_based_export_site_source.chrome = Mock(spec=webdriver.Chrome)
        quarter_based_export_site_source.__del__()
        quarter_based_export_site_source.chrome.quit.assert_called_once()

    @patch('judah.sources.export_site.quarter_based.delete_parent_folder')
    @patch.object(ChildQuarterBasedExportSiteSource, '_initialize_chrome')
    @patch.object(ChildQuarterBasedExportSiteSource, '_download_file')
    def test_query_data_source(self, mock_download_file, mock_initialize_chrome, mock_delete_parent_folder):
        """
        Should query a given start_quarter_and_year and end_quarter_and_year
        and return an iterator with data records and then deletes folder
        """
        # initializations
        mock_download_file.return_value = self.mock_csv_file_path
        quarter_based_export_site_source = ChildQuarterBasedExportSiteSource()
        start_quarter_and_year = (1, 2020,)
        end_quarter_and_year = (3, 2020,)

        # method call
        response = quarter_based_export_site_source._query_data_source(
            start_quarter_and_year=start_quarter_and_year, end_quarter_and_year=end_quarter_and_year)
        self.assertIsInstance(response, Iterator)

        data = [record for record in response]

        # assertions
        mock_initialize_chrome.assert_called_once()
        mock_download_file.assert_called_once_with(
            start_quarter_and_year=start_quarter_and_year, end_quarter_and_year=end_quarter_and_year)
        mock_delete_parent_folder.assert_called_once_with(self.mock_csv_file_path)
        self.assertListEqual(data, self.expected_data)

    @patch('judah.sources.export_site.quarter_based.delete_parent_folder')
    @patch.object(ChildQuarterBasedExportSiteSource, '_initialize_chrome')
    @patch.object(ChildQuarterBasedExportSiteSource, '_download_file')
    def test_query_data_source_no_file_downloaded(self, mock_download_file, mock_initialize_chrome,
                                                  mock_delete_parent_folder):
        """
        Should query a given start_quarter_and_year and end_quarter_and_year
         and return an empty iterator if there is no file downloaded
        """
        # initializations
        mock_download_file.return_value = None
        quarter_based_export_site_source = ChildQuarterBasedExportSiteSource()
        start_quarter_and_year = (1, 2020,)
        end_quarter_and_year = (3, 2020,)

        # method call
        response = quarter_based_export_site_source._query_data_source(
            start_quarter_and_year=start_quarter_and_year, end_quarter_and_year=end_quarter_and_year)
        self.assertIsInstance(response, Iterator)

        data = [record for record in response]

        # assertions
        mock_initialize_chrome.assert_called_once()
        mock_download_file.assert_called_once_with(
            start_quarter_and_year=start_quarter_and_year, end_quarter_and_year=end_quarter_and_year)
        mock_delete_parent_folder.assert_not_called()
        self.assertListEqual(data, [])

    @patch.object(ChildQuarterBasedExportSiteSource, '_query_data_source')
    @patch.object(ChildQuarterBasedExportSiteSource, '_get_next_end_quarter_and_year')
    @patch.object(ChildQuarterBasedExportSiteSource, '_get_next_start_quarter_and_year')
    def test_get(self, mock_get_next_start_quarter_and_year,
                 mock_get_next_end_quarter_and_year, mock_query_data_source):
        """
        Should return data from a given date to a given date as an iterator
        """
        start_quarter_and_year = (1, 2020,)
        end_quarter_and_year = (3, 2020,)
        mock_get_next_start_quarter_and_year.return_value = start_quarter_and_year
        mock_get_next_end_quarter_and_year.return_value = end_quarter_and_year
        mock_query_data_source.return_value = (record for record in self.expected_data)

        quarter_based_export_site_source = ChildQuarterBasedExportSiteSource()
        response = quarter_based_export_site_source.get()

        self.assertIsInstance(response, Iterator)

        data = [record for record in response]

        self.assertListEqual(data, self.expected_data)
        mock_get_next_start_quarter_and_year.assert_has_calls(
            [call(), call(quarters_to_increment_by=quarter_based_export_site_source.default_batch_size_in_quarters)])
        mock_get_next_end_quarter_and_year.assert_called_once()

    @patch.object(ChildQuarterBasedExportSiteSource, '_query_data_source')
    @patch.object(ChildQuarterBasedExportSiteSource, '_get_next_end_quarter_and_year')
    @patch.object(ChildQuarterBasedExportSiteSource, '_get_next_start_quarter_and_year')
    def test_get_end_quarter_earlier_than_start_quarter(self, mock_get_next_start_quarter_and_year,
                                                        mock_get_next_end_quarter_and_year, mock_query_data_source):
        """
        Should return an iterator of an empty list
        if the end_quarter_and_year is earlier than start_quarter_and_year
        """
        start_quarter_and_year = (1, 2020,)
        end_quarter_and_year = (3, 2019,)
        mock_get_next_start_quarter_and_year.return_value = start_quarter_and_year
        mock_get_next_end_quarter_and_year.return_value = end_quarter_and_year

        quarter_based_export_site_source = ChildQuarterBasedExportSiteSource()
        response = quarter_based_export_site_source.get()

        self.assertIsInstance(response, Iterator)

        data = [record for record in response]

        self.assertListEqual(data, [])
        mock_get_next_start_quarter_and_year.assert_called_once()
        mock_get_next_end_quarter_and_year.assert_called_once()
        mock_query_data_source.assert_not_called()

    def test_get_next_start_quarter_and_year_with_no_initial_quarter_year_tuples(self):
        """
        Should get the next start_quarter_and_year tuple as current quarter, year
         minus the default_batch_size_in_quarters plus net quarter increment
        when only default_batch_size_in_quarters is given
        """
        today = date.today()
        current_quarter_and_year_tuple = convert_date_to_quarter_year_tuple(today)
        quarters_to_increment_by = 4
        quarters_to_decrement_by = 8

        quarter_based_export_site_source = ChildQuarterBasedExportSiteSource()
        net_quarters_to_increment_by = (
                    quarters_to_increment_by
                    - quarters_to_decrement_by
                    - quarter_based_export_site_source.default_batch_size_in_quarters)

        next_start_quarter_and_year = quarter_based_export_site_source._get_next_start_quarter_and_year(
            quarters_to_increment_by=quarters_to_increment_by, quarters_to_decrement_by=quarters_to_decrement_by)

        expected_next_start_quarter_and_year = update_quarter_year_tuple(
            current_quarter_and_year_tuple, quarters_to_increment_by=net_quarters_to_increment_by,
            quarters_to_decrement_by=0)
        self.assertTupleEqual(next_start_quarter_and_year, expected_next_start_quarter_and_year)

    def test_get_next_start_quarter_and_year_given_end_quarter_and_year(self):
        """
        Should get the next start_quarter_and_year tuple as end_quarter_and_year tuple
        minus the default_batch_size_in_quarters
        plus net quarter increment  when default_batch_size_in_quarters and end_quarter_and_year_tuple are given
        """
        end_quarter_and_year = (2, 2019,)
        quarters_to_increment_by = 4
        quarters_to_decrement_by = 8

        quarter_based_export_site_source = ChildQuarterBasedExportSiteSource()
        quarter_based_export_site_source.end_quarter_and_year = end_quarter_and_year
        net_quarters_to_increment_by = (
                    quarters_to_increment_by
                    - quarters_to_decrement_by
                    - quarter_based_export_site_source.default_batch_size_in_quarters)

        next_start_quarter_and_year = quarter_based_export_site_source._get_next_start_quarter_and_year(
            quarters_to_increment_by=quarters_to_increment_by, quarters_to_decrement_by=quarters_to_decrement_by)

        expected_next_start_quarter_and_year = update_quarter_year_tuple(
            end_quarter_and_year, quarters_to_increment_by=net_quarters_to_increment_by,
            quarters_to_decrement_by=0)
        self.assertTupleEqual(next_start_quarter_and_year, expected_next_start_quarter_and_year)

    def test_get_next_start_quarter_and_year_with_initial_start_quarter_and_year(self):
        """
        Should get the next start_quarter_and_year tuple as start_quarter_and_year
         plus net quarter increment when start_quarter_and_year is given,
        regardless of end_quarter_and_year and default_batch_size_in_quarters
        """
        end_quarter_and_year = (1, 2019)
        start_quarter_and_year = (3, 2020,)
        quarters_to_increment_by = 4
        quarters_to_decrement_by = 15
        net_quarters_to_increment_by = quarters_to_increment_by - quarters_to_decrement_by

        quarter_based_export_site_source = ChildQuarterBasedExportSiteSource()
        quarter_based_export_site_source.end_quarter_and_year = end_quarter_and_year
        quarter_based_export_site_source.start_quarter_and_year = start_quarter_and_year

        next_start_quarter_and_year = quarter_based_export_site_source._get_next_start_quarter_and_year(
            quarters_to_increment_by=quarters_to_increment_by, quarters_to_decrement_by=quarters_to_decrement_by)

        expected_next_start_quarter_and_year = update_quarter_year_tuple(
            start_quarter_and_year, quarters_to_increment_by=net_quarters_to_increment_by,
            quarters_to_decrement_by=0)
        self.assertTupleEqual(next_start_quarter_and_year, expected_next_start_quarter_and_year)

    def test_get_next_end_quarter_and_year_with_no_initial_quarter_year_tuples(self):
        """
        Should get the next end_quarter_and_year as current quarter_and_year tuple plus net quarter increment
        when only default_batch_size_in_quarters is given
        Assumption: current quarter_and_year tuple is end_quarter_and_year if no increment or decrement
        """
        today = date.today()
        current_quarter_and_year_tuple = convert_date_to_quarter_year_tuple(today)
        quarters_to_increment_by = 4
        quarters_to_decrement_by = 9
        net_quarters_to_increment_by = quarters_to_increment_by - quarters_to_decrement_by

        quarter_based_export_site_source = ChildQuarterBasedExportSiteSource()
        next_end_quarter_and_year = quarter_based_export_site_source._get_next_end_quarter_and_year(
            quarters_to_increment_by=quarters_to_increment_by, quarters_to_decrement_by=quarters_to_decrement_by)

        expected_next_end_quarter_and_year = update_quarter_year_tuple(
            current_quarter_and_year_tuple, quarters_to_increment_by=net_quarters_to_increment_by,
            quarters_to_decrement_by=0)
        self.assertTupleEqual(next_end_quarter_and_year, expected_next_end_quarter_and_year)

    def test_get_next_end_quarter_and_year_given_start_quarter_and_year(self):
        """
        Should get the next end quarter_and_year as start_quarter_and_year plus the net quarter increment
        plus default_batch_size_in_quarters
        when default_batch_size_in_quarters and start_quarter_and_year are given and no initial end_quarter_and_year is given
        """
        start_quarter_and_year = (2, 2020)
        quarters_to_increment_by = 4
        quarters_to_decrement_by = 9

        quarter_based_export_site_source = ChildQuarterBasedExportSiteSource()
        quarter_based_export_site_source.start_quarter_and_year = start_quarter_and_year
        net_quarters_to_increment_by = (
                quarters_to_increment_by
                - quarters_to_decrement_by
                + quarter_based_export_site_source.default_batch_size_in_quarters
        )

        next_end_quarter_and_year = quarter_based_export_site_source._get_next_end_quarter_and_year(
            quarters_to_increment_by=quarters_to_increment_by, quarters_to_decrement_by=quarters_to_decrement_by)

        expected_next_end_quarter_and_year = update_quarter_year_tuple(
            start_quarter_and_year, quarters_to_increment_by=net_quarters_to_increment_by,
            quarters_to_decrement_by=0)
        self.assertTupleEqual(next_end_quarter_and_year, expected_next_end_quarter_and_year)

    def test_get_next_end_quarter_and_year_with_initial_end_quarter_and_year(self):
        """
        Should get the next end_quarter_and_year as end_quarter_and_year plus
        net quarter increment when end_quarter_and_year is given,
        regardless of start_quarter_and_year and default_batch_size_in_quarters
        """
        end_quarter_and_year = (1, 2020)
        start_quarter_and_year = (3, 2020,)
        quarters_to_increment_by = 4
        quarters_to_decrement_by = 18
        net_quarters_to_increment_by = quarters_to_increment_by - quarters_to_decrement_by

        quarter_based_export_site_source = ChildQuarterBasedExportSiteSource()
        quarter_based_export_site_source.end_quarter_and_year = end_quarter_and_year
        quarter_based_export_site_source.start_quarter_and_year = start_quarter_and_year

        next_end_quarter_and_year = quarter_based_export_site_source._get_next_end_quarter_and_year(
            quarters_to_increment_by=quarters_to_increment_by, quarters_to_decrement_by=quarters_to_decrement_by)

        expected_next_end_quarter_and_year = update_quarter_year_tuple(
            end_quarter_and_year, quarters_to_increment_by=net_quarters_to_increment_by,
            quarters_to_decrement_by=0)
        self.assertTupleEqual(next_end_quarter_and_year, expected_next_end_quarter_and_year)


if __name__ == '__main__':
    main()
