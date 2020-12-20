"""Module containing tests for the selenium utility functions"""
import os
from datetime import datetime, timedelta
from typing import Tuple, Any
from unittest import TestCase, main
from unittest.mock import Mock, patch

from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By

from judah.utils.selenium import (
    get_web_driver,
    get_html_element_by_xpath,
    visit_website,
    wait_for_download_to_complete,
    wait_for_element_to_load,
    WebDriverOptions
)

_PARENT_FOLDER = os.path.dirname(__file__)
_MOCK_ASSET_FOLDER_PATH = os.path.join(os.path.dirname(_PARENT_FOLDER), 'assets')


class TestSeleniumUtilities(TestCase):
    """Tests for the selenium utility functions"""

    def test_get_web_driver(self):
        """
        Should return a working instance of a chrome webdriver
        FIXME: Unfortunately, I am unable to check if any of the options have been applied appropriately
        """
        download_location = os.path.join(_MOCK_ASSET_FOLDER_PATH, 'csv')
        options = WebDriverOptions(downloads_folder_location=download_location)

        driver = get_web_driver(options)
        self.assertIsInstance(driver, webdriver.Chrome)

        driver.close()
        driver.quit()

    def test_visit_website(self):
        """
        Should make a GET request to the URL passed to it
        """
        mock_driver = Mock(spec=webdriver.Chrome)
        mock_driver.get = Mock()
        url = 'http://example.com'

        visit_website(mock_driver, website_url=url)

        mock_driver.get.assert_called_once_with(url=url)

    def test_get_html_element_by_xpath(self):
        """
        Should get the html element by given xpath, waiting for it only up to a given timeout
        and giving up
        """
        start_time = datetime.now()
        mock_driver = Mock(spec=webdriver.Chrome)
        timeout_in_seconds = 2
        seconds_before_element_shows_up = 1
        test_xpath = 'hello_world'
        test_element = {'foo': 'bar'}

        def mock_find_element_by_xpath(xpath: str):
            """Waits for a given time before returning an element"""
            if datetime.now() > start_time + timedelta(seconds=seconds_before_element_shows_up):
                return test_element

            return False

        mock_driver.find_element_by_xpath = Mock()
        mock_driver.find_element_by_xpath.side_effect = mock_find_element_by_xpath

        element = get_html_element_by_xpath(driver=mock_driver, xpath=test_xpath, timeout=timeout_in_seconds)

        mock_driver.find_element_by_xpath.assert_called_with(xpath=test_xpath)
        self.assertEqual(element, test_element)

    def test_timed_out_get_html_element_by_xpath(self):
        """
        Should raise TimeoutException if the element takes longer than the timeout setting to show up
        """
        mock_driver = Mock(spec=webdriver.Chrome)
        timeout_in_seconds = 2
        test_xpath = 'hello_world'

        mock_driver.find_element_by_xpath.return_value = False
        self.assertRaises(TimeoutException, get_html_element_by_xpath, driver=mock_driver, xpath=test_xpath,
                          timeout=timeout_in_seconds)

    @patch('os.path.isfile')
    @patch('os.path.exists')
    def test_wait_for_download_to_complete(self, mock_os_path_exists, mock_os_path_is_file):
        """
        Should wait for download to complete before returning
        """
        expected_file_path = 'dummy.csv'
        timeout = 2
        seconds_before_download_completes = 1
        mock_os_path_is_file.return_value = True
        start_time = datetime.now()

        def mock_os_path_exists_side_effect(file_name: str):
            """Waits for a given time before returning True"""
            if datetime.now() > start_time + timedelta(seconds=seconds_before_download_completes):
                return True

            return False

        mock_os_path_exists.side_effect = mock_os_path_exists_side_effect

        self.assertLessEqual((datetime.now() - start_time).total_seconds(), seconds_before_download_completes)

        wait_for_download_to_complete(expected_file_path=expected_file_path, timeout=timeout)

        self.assertGreater((datetime.now() - start_time).total_seconds(), seconds_before_download_completes)

    @patch('os.path.isfile')
    @patch('os.path.exists')
    def test_wait_for_download_to_complete_not_file(self, mock_os_path_exists, mock_os_path_is_file):
        """
        Should raise IOError if the file is not an actual file
        """
        expected_file_path = 'dummy.csv'
        timeout = 2
        mock_os_path_exists.return_value = True
        mock_os_path_is_file.return_value = False

        self.assertRaises(IOError, wait_for_download_to_complete,
                          expected_file_path=expected_file_path, timeout=timeout)

    @patch('judah.utils.selenium.EC')
    def test_wait_for_element_to_load(self, mock_ec):
        """
        Should wait for the element to load before returning
        """
        start_time = datetime.now()
        mock_driver = Mock(spec=webdriver.Chrome)
        timeout_in_seconds = 2
        seconds_before_element_shows_up = 1
        test_xpath = 'hello_world'
        test_element = {'foo': 'bar'}

        def mock_presence_of_element_located(search_item: Tuple[Any]):
            """Waits for a given time before returning an element"""
            if datetime.now() > start_time + timedelta(seconds=seconds_before_element_shows_up):
                return test_element

            return False

        mock_ec.presence_of_element_located = Mock()
        mock_ec.presence_of_element_located.return_value = mock_presence_of_element_located

        self.assertLessEqual((datetime.now() - start_time).total_seconds(), seconds_before_element_shows_up)

        wait_for_element_to_load(driver=mock_driver, xpath=test_xpath, timeout=timeout_in_seconds)

        self.assertGreater((datetime.now() - start_time).total_seconds(), seconds_before_element_shows_up)
        mock_ec.presence_of_element_located.assert_called_once_with((By.XPATH, test_xpath))


if __name__ == '__main__':
    main()
