"""Utility functions for handling assets"""
import logging
import os
import shutil
from csv import DictReader
from datetime import datetime
from enum import Enum
from typing import Iterator, Dict, Any, Optional, List

import xlrd
from pydantic import BaseModel
from xml_stream import read_xml_file

ASSET_FOLDER_PATH = os.path.join(os.getcwd(), 'assets')


class FileType(Enum):
    CSV = 'csv'
    XLS = 'xls'
    XML = 'xml'


class FileOptions(BaseModel):
    """Options to pass the read_file function"""
    xml_records_tag: Optional[str] = None
    xls_sheet_name: Optional[str] = None
    xls_sheet_index: Optional[int] = 1
    xls_or_csv_headers: Optional[List[str]] = None


def get_asset_path(asset_name: Optional[str] = None):
    """Returns the path of a given asset"""
    if asset_name is None:
        return ASSET_FOLDER_PATH

    return os.path.join(ASSET_FOLDER_PATH, asset_name)


def get_timestamped_folder_name(
        prefix: str = '', suffix: str = '', date_format: str = '%m_%d_%Y_%H_%M_%S'):
    """Returns a folder name of format name"""
    now = datetime.utcnow()
    return f"{prefix}{now.strftime(date_format)}{suffix}"


def get_timestamped_folder(
        root_path: str, prefix: str = '', suffix: str = '', date_format: str = '%m_%d_%Y_%H_%M_%S'):
    """Returns a folder path with the given folder created if it did not exist"""
    timestamped_folder_name = get_timestamped_folder_name(prefix=prefix, suffix=suffix, date_format=date_format)
    timestamped_folder = os.path.join(root_path, timestamped_folder_name)

    if not os.path.exists(timestamped_folder):
        os.makedirs(timestamped_folder)

    return timestamped_folder


def get_csv_download_location(dataset_name: str = ''):
    """Returns the path to the csv folder asset"""
    csv_folder_path = get_asset_path(asset_name='csv')
    return get_timestamped_folder(root_path=csv_folder_path, prefix=dataset_name)


def get_xml_download_location(dataset_name: str = ''):
    """Returns the path to the xml folder asset"""
    csv_folder_path = get_asset_path(asset_name='xml')
    return get_timestamped_folder(root_path=csv_folder_path, prefix=dataset_name)


def read_csv_file(file_path: str, xls_or_csv_headers: Optional[List[str]] = None, **kwargs) -> Iterator[Dict[str, Any]]:
    """Reads the CSV file and returns the rows in the file as an iterator"""
    with open(file_path, 'r') as csv_file:
        csv_dict_reader = DictReader(csv_file, fieldnames=xls_or_csv_headers)
        yield from csv_dict_reader


def read_xls_file(file_path: str,
                  xls_or_csv_headers: Optional[List[str]] = None,
                  xls_sheet_name: Optional[str] = None,
                  xls_sheet_index: Optional[int] = 0, **kwargs) -> Iterator[Dict[str, Any]]:
    """Reads the XLS file and returns the rows in the file as an iterator"""
    with xlrd.open_workbook(file_path, on_demand=True) as xls_file:
        if isinstance(xls_sheet_name, str):
            sheet = xls_file.sheet_by_name(xls_sheet_name)
        elif isinstance(xls_sheet_index, int):
            sheet = xls_file.sheet_by_index(xls_sheet_index)
        else:
            raise ValueError('A sheet_name or sheet_index should be provided')

        for row in sheet.get_rows():
            row_values = [cell.value for cell in row]

            # should run once on the first row
            if xls_or_csv_headers is None:
                xls_or_csv_headers = row_values
                continue

            yield dict(zip(xls_or_csv_headers, row_values))


def read_file(file_path: str,
              file_type: FileType = FileType.CSV,
              options: FileOptions = FileOptions()) -> Iterator[Dict[str, Any]]:
    """Reads the Downloaded file and returns the rows in the file as an iterator"""
    with open(file_path, 'r') as file:
        if file_type == FileType.CSV:
            yield from read_csv_file(file_path=file_path, **dict(options))

        elif file_type == FileType.XLS:
            yield from read_xls_file(file_path=file_path, **dict(options))

        elif file_type == FileType.XML:
            yield from read_xml_file(
                file_path=file_path, to_dict=True, records_tag=options.xml_records_tag)

        else:
            raise ValueError('The given file_type cannot be handled by program')


def delete_parent_folder(file_path: str):
    """Deletes the parent folder when given a file path"""
    parent_folder = os.path.dirname(file_path)

    try:
        shutil.rmtree(parent_folder)
    except OSError as exp:
        logging.error(exp)
