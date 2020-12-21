"""Module containing tests for the index based Exports Site data source"""
import os
from collections import Iterator
from datetime import date, timedelta
from typing import Optional
from unittest import TestCase, main
from unittest.mock import patch, Mock, call

from selenium import webdriver

from judah.sources.export_site.index_based import IndexBasedExportSiteSource

_PARENT_FOLDER = os.path.dirname(__file__)
_MOCK_ASSET_FOLDER_PATH = os.path.join(os.path.dirname(os.path.dirname(_PARENT_FOLDER)), 'assets')


class ChildIndexBasedExportSiteSource(IndexBasedExportSiteSource):
    """Child export site source that just picks a file from the file system"""
    base_uri: str = 'http://example.com'
    name: str = 'test_export_site_source'

    def _download_file(self, current_option_index: int) -> Optional[str]:
        """Downloads the CSV from the export site and returns the path to it"""
        return None


class TestIndexBasedExportSiteSource(TestCase):
    """Tests for the IndexBasedExportSiteSource"""

    def setUp(self) -> None:
        """Initialize some variables"""
        self.mock_csv_file_path = os.path.join(_MOCK_ASSET_FOLDER_PATH, 'mock.csv')
        self.expected_data = [
            {"Date": "09/03/2020", "number": "1", "period_from": "00:00", "period_until": "00:15", "Capacity": "16616"},
            {"Date": "09/03/2020", "number": "2", "period_from": "00:15", "period_until": "00:30", "Capacity": "16616"},
            {"Date": "09/03/2020", "number": "3", "period_from": "00:30", "period_until": "00:45", "Capacity": "16616"},
            {"Date": "09/03/2020", "number": "4", "period_from": "00:45", "period_until": "01:00", "Capacity": "16620"},
        ]

    @patch('judah.sources.export_site.index_based.visit_website')
    def test_initialize_chrome(self, mock_visit_website):
        """
        Should initialize Chrome in case it is not yet initialized and visits the base url
        """
        index_based_export_site_source = ChildIndexBasedExportSiteSource()
        self.assertIsNone(index_based_export_site_source.chrome)

        index_based_export_site_source._initialize_chrome()

        self.assertIsInstance(index_based_export_site_source.chrome, webdriver.Chrome)
        mock_visit_website.assert_called_once_with(
            driver=index_based_export_site_source.chrome, website_url=index_based_export_site_source.base_uri)

        index_based_export_site_source.chrome.close()
        index_based_export_site_source.chrome.quit()

    @patch('judah.sources.export_site.index_based.delete_parent_folder')
    @patch.object(ChildIndexBasedExportSiteSource, '_initialize_chrome')
    @patch.object(ChildIndexBasedExportSiteSource, '_download_file')
    def test_query_data_source(self, mock_download_file, mock_initialize_chrome, mock_delete_parent_folder):
        """
        Should download all the files in the file list on the export site, one after the other,
        and return an iterator with data records and then deletes folder
        """
        # initializations
        mock_download_file.return_value = self.mock_csv_file_path
        number_of_files = 5
        index_based_export_site_source = ChildIndexBasedExportSiteSource(number_of_indices=number_of_files)

        # method call
        response = index_based_export_site_source._query_data_source()
        self.assertIsInstance(response, Iterator)

        data = [record for record in response]

        # assertions
        mock_initialize_chrome.assert_called_once()
        mock_download_file_calls = [call(current_option_index=index) for index in range(number_of_files)]
        mock_download_file.assert_has_calls(mock_download_file_calls)
        mock_delete_parent_folder_calls = [call(self.mock_csv_file_path) for index in range(number_of_files)]
        mock_delete_parent_folder.assert_has_calls(mock_delete_parent_folder_calls)
        records_from_all_files = []
        for index in range(number_of_files):
            records_from_all_files += self.expected_data.copy()
        self.assertListEqual(data, records_from_all_files)

    @patch('judah.sources.export_site.index_based.delete_parent_folder')
    @patch.object(ChildIndexBasedExportSiteSource, '_initialize_chrome')
    @patch.object(ChildIndexBasedExportSiteSource, '_download_file')
    def test_query_data_source_no_file_downloaded(self, mock_download_file, mock_initialize_chrome,
                                                  mock_delete_parent_folder):
        """
        Should return an empty iterator if there is no file downloaded
        """
        # initializations
        mock_download_file.return_value = None
        number_of_files = 5
        index_based_export_site_source = ChildIndexBasedExportSiteSource(number_of_indices=number_of_files)

        # method call
        response = index_based_export_site_source._query_data_source()
        self.assertIsInstance(response, Iterator)

        data = [record for record in response]

        # assertions
        mock_initialize_chrome.assert_called_once()
        mock_download_file_calls = [call(current_option_index=index) for index in range(number_of_files)]
        mock_download_file.assert_has_calls(mock_download_file_calls)
        mock_delete_parent_folder.assert_not_called()
        self.assertListEqual(data, [])

    @patch.object(ChildIndexBasedExportSiteSource, '_initialize_chrome')
    def test_del(self, mock_initialize_chrome):
        """
        Should quit chrome on deletion
        """
        index_based_export_site_source = ChildIndexBasedExportSiteSource()
        index_based_export_site_source.chrome = Mock(spec=webdriver.Chrome)
        index_based_export_site_source.__del__()
        index_based_export_site_source.chrome.quit.assert_called_once()

    @patch.object(ChildIndexBasedExportSiteSource, '_query_data_source')
    def test_get(self, mock_query_data_source):
        """
        Should return data from the list of files as an iterator
        """
        mock_query_data_source.return_value = (record for record in self.expected_data)

        number_of_files = 5
        index_based_export_site_source = ChildIndexBasedExportSiteSource(number_of_indices=number_of_files)
        response = index_based_export_site_source.get()

        self.assertIsInstance(response, Iterator)

        data = [record for record in response]

        self.assertListEqual(data, self.expected_data)


if __name__ == '__main__':
    main()
