"""
Module for base class for sources that download files in an order
based off the index of those files in a file list
"""

import asyncio
from typing import Iterator, Dict, Any, Optional

from selenium import webdriver

from judah.sources.base import BaseSource
from judah.utils.assets import FileType, FileOptions, get_csv_download_location, get_xml_download_location, \
    get_asset_path, read_file, delete_parent_folder
from judah.utils.selenium import get_web_driver, WebDriverOptions, visit_website


class IndexBasedExportSiteSource(BaseSource):
    """Data source for export sites where downloading is in the order of the files in a list on the site"""
    file_type: FileType = FileType.CSV
    file_options: FileOptions = FileOptions()
    number_of_indices: Optional[int] = None
    seconds_between_downloads: int = 3
    chrome: Optional[webdriver.Chrome] = None
    download_folder_path: Optional[str] = None

    class Config:
        arbitrary_types_allowed = True

    def _initialize_chrome(self):
        """Initializes Chrome in case it is not yet initialized"""
        if isinstance(self.chrome, webdriver.Chrome):
            return

        if self.file_type == FileType.CSV:
            self.download_folder_path = get_csv_download_location(dataset_name=self.name.replace(' ', '_'))
        elif self.file_type == FileType.XML:
            self.download_folder_path = get_xml_download_location(dataset_name=self.name.replace(' ', '_'))
        else:
            self.download_folder_path = get_asset_path()

        self.chrome = get_web_driver(
            WebDriverOptions(downloads_folder_location=self.download_folder_path))

        visit_website(driver=self.chrome, website_url=self.base_uri)

    def _download_file(self, current_option_index: int) -> Optional[str]:
        """Downloads the CSV from the export site and returns the path to it"""
        raise NotImplementedError('_download_file method should be implemented')

    def _query_data_source(self, **kwargs) -> Iterator[Dict[str, Any]]:
        """Queries a given source and returns an iterator with data records"""
        self._initialize_chrome()
        current_option_index = 0

        while True:
            file_path = self._download_file(current_option_index=current_option_index)

            if file_path is None:
                yield from []
            else:
                yield from read_file(
                    file_path=file_path, file_type=self.file_type, options=self.file_options)
                delete_parent_folder(file_path)

            current_option_index += 1

            if self.number_of_indices is None:
                raise Exception('self.number_of_indices should be set in the _download_file method')

            if current_option_index >= self.number_of_indices:
                self.number_of_indices = None
                break

            # to avoid Denial of Service (DOS) on the server
            asyncio.sleep(self.seconds_between_downloads)

    def __del__(self):
        """Clean up"""
        if isinstance(self.chrome, webdriver.Chrome):
            self.chrome.quit()
