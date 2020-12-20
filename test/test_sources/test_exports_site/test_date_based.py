"""Module containing tests for the date based Exports Site data source"""
import os
from collections import Iterator
from datetime import date, timedelta
from typing import Optional
from unittest import TestCase, main
from unittest.mock import patch, Mock, call

from selenium import webdriver

from judah.sources.export_site.date_based import DateBasedExportSiteSource

_PARENT_FOLDER = os.path.dirname(__file__)
_MOCK_ASSET_FOLDER_PATH = os.path.join(os.path.dirname(os.path.dirname(_PARENT_FOLDER)), 'assets')


class ChildDateBasedExportSiteSource(DateBasedExportSiteSource):
    """Child export site source that just picks a file from the file system"""
    base_uri: str = 'http://example.com'
    name: str = 'test_export_site_source'

    def _download_file(self, start_date: date, end_date: date) -> Optional[str]:
        """Downloads the CSV from the export site and returns the path to it"""
        return None


class TestDateBasedExportSiteSource(TestCase):
    """Tests for the DateBasedExportSiteSource"""

    def setUp(self) -> None:
        """Initialize some variables"""
        self.mock_csv_file_path = os.path.join(_MOCK_ASSET_FOLDER_PATH, 'mock.csv')
        self.expected_data = [
            {"Date": "09/03/2020", "number": "1", "period_from": "00:00", "period_until": "00:15", "Capacity": "16616"},
            {"Date": "09/03/2020", "number": "2", "period_from": "00:15", "period_until": "00:30", "Capacity": "16616"},
            {"Date": "09/03/2020", "number": "3", "period_from": "00:30", "period_until": "00:45", "Capacity": "16616"},
            {"Date": "09/03/2020", "number": "4", "period_from": "00:45", "period_until": "01:00", "Capacity": "16620"},
        ]

    @patch('judah.sources.export_site.date_based.visit_website')
    def test_initialize_chrome(self, mock_visit_website):
        """
        Should initialize Chrome in case it is not yet initialized and visits the base url
        """
        date_based_export_site_source = ChildDateBasedExportSiteSource()
        self.assertIsNone(date_based_export_site_source.chrome)

        date_based_export_site_source._initialize_chrome()

        self.assertIsInstance(date_based_export_site_source.chrome, webdriver.Chrome)
        mock_visit_website.assert_called_once_with(
            driver=date_based_export_site_source.chrome, website_url=date_based_export_site_source.base_uri)

        date_based_export_site_source.chrome.close()
        date_based_export_site_source.chrome.quit()

    @patch('judah.sources.export_site.date_based.delete_parent_folder')
    @patch.object(ChildDateBasedExportSiteSource, '_initialize_chrome')
    @patch.object(ChildDateBasedExportSiteSource, '_download_file')
    def test_query_data_source(self, mock_download_file, mock_initialize_chrome, mock_delete_parent_folder):
        """
        Should query a given start and end date and return an iterator with data records and then deletes folder
        """
        # initializations
        mock_download_file.return_value = self.mock_csv_file_path
        date_based_export_site_source = ChildDateBasedExportSiteSource()
        start_date = date(year=2020, month=3, day=9)
        end_date = date(year=2020, month=3, day=13)

        # method call
        response = date_based_export_site_source._query_data_source(start_date=start_date, end_date=end_date)
        self.assertIsInstance(response, Iterator)

        data = [record for record in response]

        # assertions
        mock_initialize_chrome.assert_called_once()
        mock_download_file.assert_called_once_with(start_date=start_date, end_date=end_date)
        mock_delete_parent_folder.assert_called_once_with(self.mock_csv_file_path)
        self.assertListEqual(data, self.expected_data)

    @patch('judah.sources.export_site.date_based.delete_parent_folder')
    @patch.object(ChildDateBasedExportSiteSource, '_initialize_chrome')
    @patch.object(ChildDateBasedExportSiteSource, '_download_file')
    def test_query_data_source_no_file_downloaded(self, mock_download_file, mock_initialize_chrome,
                                                  mock_delete_parent_folder):
        """
        Should query a given start and end date and return an empty iterator if there is no file downloaded
        """
        # initializations
        mock_download_file.return_value = None
        date_based_export_site_source = ChildDateBasedExportSiteSource()
        start_date = date(year=2020, month=3, day=9)
        end_date = date(year=2020, month=3, day=13)

        # method call
        response = date_based_export_site_source._query_data_source(start_date=start_date, end_date=end_date)
        self.assertIsInstance(response, Iterator)

        data = [record for record in response]

        # assertions
        mock_initialize_chrome.assert_called_once()
        mock_download_file.assert_called_once_with(start_date=start_date, end_date=end_date)
        mock_delete_parent_folder.assert_not_called()
        self.assertListEqual(data, [])

    @patch.object(ChildDateBasedExportSiteSource, '_initialize_chrome')
    def test_del(self, mock_initialize_chrome):
        """
        Should quit chrome on deletion
        """
        date_based_export_site_source = ChildDateBasedExportSiteSource()
        date_based_export_site_source.chrome = Mock(spec=webdriver.Chrome)
        date_based_export_site_source.__del__()
        date_based_export_site_source.chrome.quit.assert_called_once()

    @patch.object(ChildDateBasedExportSiteSource, '_query_data_source')
    @patch.object(ChildDateBasedExportSiteSource, '_get_next_end_date')
    @patch.object(ChildDateBasedExportSiteSource, '_get_next_start_date')
    def test_get(self, mock_get_next_start_date, mock_get_next_end_date, mock_query_data_source):
        """
        Should return data from a given date to a given date as an iterator
        """
        start_date = date(year=2020, month=3, day=9)
        end_date = date(year=2020, month=3, day=13)
        mock_get_next_start_date.return_value = start_date
        mock_get_next_end_date.return_value = end_date
        mock_query_data_source.return_value = (record for record in self.expected_data)

        date_based_export_site_source = ChildDateBasedExportSiteSource()
        response = date_based_export_site_source.get()

        self.assertIsInstance(response, Iterator)

        data = [record for record in response]

        self.assertListEqual(data, self.expected_data)
        mock_get_next_start_date.assert_has_calls(
            [call(), call(days_to_increment_by=date_based_export_site_source.default_batch_size_in_days)])
        mock_get_next_end_date.assert_called_once()

    @patch.object(ChildDateBasedExportSiteSource, '_query_data_source')
    @patch.object(ChildDateBasedExportSiteSource, '_get_next_end_date')
    @patch.object(ChildDateBasedExportSiteSource, '_get_next_start_date')
    def test_get_end_date_earlier_than_start_date(self, mock_get_next_start_date, mock_get_next_end_date,
                                                  mock_query_data_source):
        """
        Should return an iterator of an empty list if the end_date is earlier than start_date
        """
        start_date = date(year=2020, month=3, day=30)
        end_date = date(year=2020, month=3, day=13)
        mock_get_next_start_date.return_value = start_date
        mock_get_next_end_date.return_value = end_date

        date_based_export_site_source = ChildDateBasedExportSiteSource()
        response = date_based_export_site_source.get()

        self.assertIsInstance(response, Iterator)

        data = [record for record in response]

        self.assertListEqual(data, [])
        mock_get_next_start_date.assert_called_once()
        mock_get_next_end_date.assert_called_once()
        mock_query_data_source.assert_not_called()

    def test_get_next_start_date_with_no_initial_start_date_and_no_end_date(self):
        """
        Should get the next start date as today minus the default_batch_size_in_days plus net day increment
        when only default_batch_size_in_days is given
        """
        today = date.today()
        days_to_increment_by = 4
        days_to_decrement_by = 8
        net_days_to_increment_by = days_to_increment_by - days_to_decrement_by

        date_based_export_site_source = ChildDateBasedExportSiteSource()
        next_start_date = date_based_export_site_source._get_next_start_date(
            days_to_increment_by=days_to_increment_by, days_to_decrement_by=days_to_decrement_by)

        expected_next_start_date = today + timedelta(days=net_days_to_increment_by) - timedelta(
            days=date_based_export_site_source.default_batch_size_in_days)
        self.assertEqual(next_start_date, expected_next_start_date)

    def test_get_next_start_date_given_end_date(self):
        """
        Should get the next start date as end_date minus the default_batch_size_in_days plus net day increment
        when default_batch_size_in_days and end_date are given
        """
        end_date = date(year=2020, month=3, day=13)
        days_to_increment_by = 4
        days_to_decrement_by = 8
        net_days_to_increment_by = days_to_increment_by - days_to_decrement_by

        date_based_export_site_source = ChildDateBasedExportSiteSource()
        date_based_export_site_source.end_date = end_date

        next_start_date = date_based_export_site_source._get_next_start_date(
            days_to_increment_by=days_to_increment_by, days_to_decrement_by=days_to_decrement_by)

        expected_next_start_date = end_date + timedelta(days=net_days_to_increment_by) - timedelta(
            days=date_based_export_site_source.default_batch_size_in_days)
        self.assertEqual(next_start_date, expected_next_start_date)

    def test_get_next_start_date_with_initial_start_date(self):
        """
        Should get the next start date as start_date plus net day increment when start_date is given,
        regardless of end_date and default_batch_size_in_days
        """
        end_date = date(year=2020, month=3, day=13)
        start_date = date(year=2020, month=3, day=12)
        days_to_increment_by = 4
        days_to_decrement_by = 8
        net_days_to_increment_by = days_to_increment_by - days_to_decrement_by

        date_based_export_site_source = ChildDateBasedExportSiteSource()
        date_based_export_site_source.end_date = end_date
        date_based_export_site_source.start_date = start_date

        next_start_date = date_based_export_site_source._get_next_start_date(
            days_to_increment_by=days_to_increment_by, days_to_decrement_by=days_to_decrement_by)

        expected_next_start_date = start_date + timedelta(days=net_days_to_increment_by)
        self.assertEqual(next_start_date, expected_next_start_date)

    def test_get_next_end_date_with_no_initial_start_date_and_no_end_date(self):
        """
        Should get the next end date as today plus net day increment
        when only default_batch_size_in_days is given: Assumption is today is end date if no increment or decrement
        """
        today = date.today()
        days_to_increment_by = 4
        days_to_decrement_by = 9
        net_days_to_increment_by = days_to_increment_by - days_to_decrement_by

        date_based_export_site_source = ChildDateBasedExportSiteSource()
        next_end_date = date_based_export_site_source._get_next_end_date(
            days_to_increment_by=days_to_increment_by, days_to_decrement_by=days_to_decrement_by)

        expected_next_end_date = today + timedelta(days=net_days_to_increment_by)
        self.assertEqual(next_end_date, expected_next_end_date)

    def test_get_next_end_date_given_start_date(self):
        """
        Should get the next end date as start_date plus the net day increment plus default_batch_size_in_days
        when default_batch_size_in_days and start_date are given and no initial end_date is given
        """
        start_date = date(year=2020, month=3, day=13)
        days_to_increment_by = 4
        days_to_decrement_by = 9
        net_days_to_increment_by = days_to_increment_by - days_to_decrement_by

        date_based_export_site_source = ChildDateBasedExportSiteSource()
        date_based_export_site_source.start_date = start_date

        next_end_date = date_based_export_site_source._get_next_end_date(
            days_to_increment_by=days_to_increment_by, days_to_decrement_by=days_to_decrement_by)

        expected_next_end_date = start_date + timedelta(days=net_days_to_increment_by) + timedelta(
            days=date_based_export_site_source.default_batch_size_in_days)
        self.assertEqual(next_end_date, expected_next_end_date)

    def test_get_next_end_date_with_initial_end_date(self):
        """
        Should get the next end date as end_date plus net day increment when end_date is given,
        regardless of start_date and default_batch_size_in_days
        """
        end_date = date(year=2020, month=3, day=13)
        start_date = date(year=2020, month=3, day=12)
        days_to_increment_by = 4
        days_to_decrement_by = 18
        net_days_to_increment_by = days_to_increment_by - days_to_decrement_by

        date_based_export_site_source = ChildDateBasedExportSiteSource()
        date_based_export_site_source.end_date = end_date
        date_based_export_site_source.start_date = start_date

        next_end_date = date_based_export_site_source._get_next_end_date(
            days_to_increment_by=days_to_increment_by, days_to_decrement_by=days_to_decrement_by)

        expected_next_end_date = end_date + timedelta(days=net_days_to_increment_by)
        self.assertEqual(next_end_date, expected_next_end_date)


if __name__ == '__main__':
    main()
