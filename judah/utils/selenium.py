"""
Utility functions for selenium
https://towardsdatascience.com/using-python-and-selenium-to-automate-filling-forms-and-mouse-clicks-f87c74ed5c0f
"""
import os
import time

from datetime import timedelta, datetime

from selenium import webdriver
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager

from .assets import get_csv_download_location

from pydantic import BaseModel


class WebDriverOptions(BaseModel):
    downloads_folder_location: str = get_csv_download_location()
    headless: bool = True


def get_web_driver(options: WebDriverOptions) -> webdriver.Chrome:
    """Returns a working instance of a chrome webdriver"""
    download_folder_location = options.downloads_folder_location

    chrome_options = webdriver.ChromeOptions()
    if options.headless:
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--disable-extensions")

    prefs = {
        "profile.default_content_settings.popups": 0,
        "download.default_directory": download_folder_location,
        "directory_upgrade": True
    }
    chrome_options.add_experimental_option('prefs', prefs)

    return webdriver.Chrome(
        executable_path=ChromeDriverManager().install(), options=chrome_options)


def visit_website(driver: webdriver.Chrome, website_url: str):
    """Visits a given website using selenium and the chromedriver"""
    return driver.get(url=website_url)


def get_html_element_by_xpath(driver: webdriver.Chrome, xpath: str, timeout: int = 2) -> WebElement:
    """Returns an instance of the element so as to manipulate it"""

    def is_html_element_visible(web_driver: webdriver.Chrome):
        """Returns the element if visible or False if not visible"""
        element = web_driver.find_element_by_xpath(xpath=xpath)
        return element if element else False

    return WebDriverWait(driver=driver, timeout=timeout).until(is_html_element_visible)


def wait_for_download_to_complete(expected_file_path: str, timeout: int = 10):
    """Waits for the download to complete"""
    start_time = datetime.now()
    timeout_delta = timedelta(seconds=timeout)

    while not os.path.exists(expected_file_path) and datetime.now() - start_time < timeout_delta:
        time.sleep(1)

    if not os.path.isfile(expected_file_path):
        raise IOError(f"{expected_file_path} failed to download in {timeout} seconds")


def wait_for_element_to_load(driver: webdriver, xpath: str, timeout: int):
    WebDriverWait(driver=driver, timeout=timeout).until(
        EC.presence_of_element_located((By.XPATH, xpath)))
