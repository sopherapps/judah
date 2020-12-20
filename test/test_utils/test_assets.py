"""Module containing tests for the assets utility functions"""
import os
import shutil
from collections import Iterator
from datetime import datetime, timedelta
from unittest import TestCase, main
from unittest.mock import Mock, patch
from judah.utils.assets import get_asset_path, get_timestamped_folder_name, get_timestamped_folder, \
    get_csv_download_location, get_xml_download_location, read_xls_file, read_csv_file, read_file, delete_parent_folder, \
    FileType, FileOptions

_ROOT_PATH = os.getcwd()
_PARENT_FOLDER = os.path.dirname(__file__)
_MOCK_ASSET_FOLDER_PATH = os.path.join(os.path.dirname(_PARENT_FOLDER), 'assets')


class TestAssetsUtilities(TestCase):
    """Tests for the assets utility functions"""

    def setUp(self) -> None:
        """Initialize a few variables"""
        self.default_time_format = '%m_%d_%Y_%H_%M_%S'

    def test_get_asset_path_default(self):
        """returns the asset folder path at the root of where the script is run when no parameter is passed"""
        expected_asset_folder_path = os.path.join(_ROOT_PATH, 'assets')
        self.assertEqual(get_asset_path(), expected_asset_folder_path)

    def test_get_asset_path(self):
        """returns the path to a given folder in the asset folder"""
        asset_name = 'photos'
        expected_asset_folder_path = os.path.join(_ROOT_PATH, 'assets', asset_name)
        self.assertEqual(get_asset_path(asset_name=asset_name), expected_asset_folder_path)

    def test_get_timestamped_folder_name(self):
        """returns a folder name with a timestamp in it name"""
        time_now = datetime.now()
        default_folder_name = get_timestamped_folder_name()
        default_folder_timestamp = datetime.strptime(default_folder_name, self.default_time_format)
        self.assertLessEqual(default_folder_timestamp - time_now, timedelta(minutes=1))

    def test_get_timestamped_folder_name_with_suffix(self):
        """returns a folder name with a timestamp in it name and the suffix"""
        time_now = datetime.now()
        suffix = 'hey'
        suffixed_folder_name = get_timestamped_folder_name(suffix=suffix)
        timestamp_string = suffixed_folder_name.split(suffix)[0]
        suffixed_folder_timestamp = datetime.strptime(timestamp_string, self.default_time_format)
        self.assertLessEqual(suffixed_folder_timestamp - time_now, timedelta(minutes=1))
        self.assertEqual(suffixed_folder_name.index(suffix), len(suffixed_folder_name) - len(suffix))

    def test_get_timestamped_folder_name_with_prefix(self):
        """returns a folder name with a timestamp in it name and the prefix"""
        time_now = datetime.now()
        prefix = 'hey'
        prefixed_folder_name = get_timestamped_folder_name(prefix=prefix)
        timestamp_string = prefixed_folder_name.split(prefix)[1]
        prefixed_folder_timestamp = datetime.strptime(timestamp_string, self.default_time_format)

        self.assertLessEqual(prefixed_folder_timestamp - time_now, timedelta(minutes=1))
        self.assertEqual(prefixed_folder_name.index(prefix), 0)

    def test_get_timestamped_folder_name_with_other_format(self):
        """returns a folder name with a timestamp in it with a different time format"""
        other_format = '%Y-%m'
        time_now = datetime.now()
        other_format_folder_name = get_timestamped_folder_name(date_format=other_format)
        default_folder_timestamp = datetime.strptime(other_format_folder_name, other_format)
        self.assertLessEqual(default_folder_timestamp - time_now, timedelta(minutes=1))

    def test_get_timestamped_folder(self):
        """returns a folder path with a timestamp in it name and creates the folder if it does not exist"""
        time_now = datetime.now()
        default_folder_path = get_timestamped_folder(root_path=_ROOT_PATH)
        default_folder_name = os.path.basename(default_folder_path)
        default_folder_timestamp = datetime.strptime(default_folder_name, self.default_time_format)

        self.assertLessEqual(default_folder_timestamp - time_now, timedelta(minutes=1))
        self.assertEqual(os.path.dirname(default_folder_path), _ROOT_PATH)

        self.assertTrue(os.path.isdir(default_folder_path))
        shutil.rmtree(default_folder_path)

    def test_get_timestamped_folder_with_suffix(self):
        """returns a folder path with a timestamp in it name and the suffix and creates the folder if not exist"""
        time_now = datetime.now()
        suffix = 'hey'
        suffixed_folder_path = get_timestamped_folder(suffix=suffix, root_path=_ROOT_PATH)
        suffixed_folder_name = os.path.basename(suffixed_folder_path)
        timestamp_string = suffixed_folder_name.split(suffix)[0]
        suffixed_folder_timestamp = datetime.strptime(timestamp_string, self.default_time_format)

        self.assertLessEqual(suffixed_folder_timestamp - time_now, timedelta(minutes=1))
        self.assertEqual(suffixed_folder_name.index(suffix), len(suffixed_folder_name) - len(suffix))
        self.assertEqual(os.path.dirname(suffixed_folder_path), _ROOT_PATH)

        self.assertTrue(os.path.isdir(suffixed_folder_path))
        shutil.rmtree(suffixed_folder_path)

    def test_get_timestamped_folder_with_prefix(self):
        """returns a folder path with a timestamp in it name and the prefix"""
        time_now = datetime.now()
        prefix = 'hey'
        prefixed_folder_path = get_timestamped_folder(prefix=prefix, root_path=_ROOT_PATH)
        prefixed_folder_name = os.path.basename(prefixed_folder_path)
        timestamp_string = prefixed_folder_name.split(prefix)[1]
        prefixed_folder_timestamp = datetime.strptime(timestamp_string, self.default_time_format)

        self.assertLessEqual(prefixed_folder_timestamp - time_now, timedelta(minutes=1))
        self.assertEqual(prefixed_folder_name.index(prefix), 0)
        self.assertEqual(os.path.dirname(prefixed_folder_path), _ROOT_PATH)

        self.assertTrue(os.path.isdir(prefixed_folder_path))
        shutil.rmtree(prefixed_folder_path)

    def test_get_timestamped_folder_with_other_format(self):
        """returns a folder path with a timestamp in it with a different time format"""
        other_format = '%Y-%m'
        time_now = datetime.now()
        other_format_folder_path = get_timestamped_folder(date_format=other_format, root_path=_ROOT_PATH)
        other_format_folder_name = os.path.basename(other_format_folder_path)
        folder_timestamp = datetime.strptime(other_format_folder_name, other_format)

        self.assertLessEqual(folder_timestamp - time_now, timedelta(minutes=1))
        self.assertTrue(os.path.isdir(other_format_folder_path))
        shutil.rmtree(other_format_folder_path)

    def test_get_csv_download_location(self):
        """
        returns the path to a timestamped folder prefixed by the dataset name
        within the csv folder in the assets folder on the root of the project
        """
        dataset_name = 'tmp'
        time_now = datetime.now()
        expected_csv_dataset_folder_path = os.path.join(_ROOT_PATH, 'assets', 'csv')
        downloaded_csv_folder_path = get_csv_download_location(dataset_name=dataset_name)

        parent_folder_of_downloaded_csv_folder = os.path.dirname(downloaded_csv_folder_path)
        downloaded_csv_folder_name = os.path.basename(downloaded_csv_folder_path)
        downloaded_csv_folder_timestamp = datetime.strptime(downloaded_csv_folder_name.strip(dataset_name),
                                                            self.default_time_format)

        self.assertEqual(parent_folder_of_downloaded_csv_folder, expected_csv_dataset_folder_path)
        self.assertLessEqual(downloaded_csv_folder_timestamp - time_now, timedelta(minutes=1))

        self.assertTrue(os.path.isdir(downloaded_csv_folder_path))
        shutil.rmtree(downloaded_csv_folder_path)

    def test_get_xml_download_location(self):
        """
        returns the path to a timestamped folder prefixed by the dataset name
        within the xml folder in the assets folder on the root of the project
        """
        dataset_name = 'tmp'
        time_now = datetime.now()
        expected_xml_dataset_folder_path = os.path.join(_ROOT_PATH, 'assets', 'xml')
        downloaded_xml_folder_path = get_xml_download_location(dataset_name=dataset_name)

        parent_folder_of_downloaded_xml_folder = os.path.dirname(downloaded_xml_folder_path)
        downloaded_xml_folder_name = os.path.basename(downloaded_xml_folder_path)
        downloaded_xml_folder_timestamp = datetime.strptime(downloaded_xml_folder_name.strip(dataset_name),
                                                            self.default_time_format)

        self.assertEqual(parent_folder_of_downloaded_xml_folder, expected_xml_dataset_folder_path)
        self.assertLessEqual(downloaded_xml_folder_timestamp - time_now, timedelta(minutes=1))

        self.assertTrue(os.path.isdir(downloaded_xml_folder_path))
        shutil.rmtree(downloaded_xml_folder_path)

    def test_read_csv_file(self):
        """Reads a CSV file returning dict by dict in an iterator"""
        mock_csv_file_path = os.path.join(_MOCK_ASSET_FOLDER_PATH, 'mock.csv')
        csv_iterator = read_csv_file(mock_csv_file_path)

        expected_data = [
            {"Date": "09/03/2020", "number": "1", "period_from": "00:00", "period_until": "00:15", "Capacity": "16616"},
            {"Date": "09/03/2020", "number": "2", "period_from": "00:15", "period_until": "00:30", "Capacity": "16616"},
            {"Date": "09/03/2020", "number": "3", "period_from": "00:30", "period_until": "00:45", "Capacity": "16616"},
            {"Date": "09/03/2020", "number": "4", "period_from": "00:45", "period_until": "01:00", "Capacity": "16620"},
        ]

        self.assertIsInstance(csv_iterator, Iterator)

        data = [record for record in csv_iterator]
        self.assertListEqual(data, expected_data)

    def test_read_xls_file(self):
        """Reads an XLS file returning dict by dict in an iterator"""
        mock_xls_file_path = os.path.join(_MOCK_ASSET_FOLDER_PATH, 'mock.xls')
        xls_iterator = read_xls_file(mock_xls_file_path, xls_sheet_index=0)

        expected_data = [
            {"number": 1.0, "power": 16616.0},
            {"number": 2.0, "power": 16616.0},
            {"number": 3.0, "power": 16616.0},
            {"number": 4.0, "power": 16616.0},
        ]

        self.assertIsInstance(xls_iterator, Iterator)

        data = [record for record in xls_iterator]
        self.assertListEqual(data, expected_data)

    @patch('judah.utils.assets.read_xml_file')
    def test_read_file_when_xml(self, mock_read_xml_file: Mock):
        """Reads a file as XML if file_type is XML"""
        mock_xml_file_path = os.path.join(_MOCK_ASSET_FOLDER_PATH, 'mock.xml')
        options = FileOptions()

        xml_iterator = read_file(mock_xml_file_path, file_type=FileType.XML, options=options)
        data = [record for record in xml_iterator]

        mock_read_xml_file.assert_called_once_with(file_path=mock_xml_file_path, to_dict=True,
                                                   records_tag=options.xml_records_tag)

    @patch('judah.utils.assets.read_xls_file')
    def test_read_file_when_xls(self, mock_read_xls_file: Mock):
        """Reads a file as XLS if file_type is XLS"""
        mock_xls_file_path = os.path.join(_MOCK_ASSET_FOLDER_PATH, 'mock.xls')
        options = FileOptions()

        xls_iterator = read_file(mock_xls_file_path, file_type=FileType.XLS, options=options)
        data = [record for record in xls_iterator]

        mock_read_xls_file.assert_called_once_with(file_path=mock_xls_file_path, **dict(options))

    @patch('judah.utils.assets.read_csv_file')
    def test_read_file_when_csv(self, mock_read_csv_file: Mock):
        """Reads a file as CSV if file_type is CSV"""
        mock_csv_file_path = os.path.join(_MOCK_ASSET_FOLDER_PATH, 'mock.csv')
        options = FileOptions()

        csv_iterator = read_file(mock_csv_file_path, file_type=FileType.CSV, options=options)
        data = [record for record in csv_iterator]

        mock_read_csv_file.assert_called_once_with(file_path=mock_csv_file_path, **dict(options))

    def test_delete_parent_folder(self):
        """Deletes the parent folder of a given file"""
        dummy_folder_path = os.path.join(_MOCK_ASSET_FOLDER_PATH, 'dummy')
        dummy_file_path = os.path.join(dummy_folder_path, 'dummy.csv')

        if not os.path.exists(dummy_folder_path):
            os.makedirs(dummy_folder_path)

        file_holder = open(dummy_file_path, 'w+')
        file_holder.close()

        delete_parent_folder(dummy_file_path)
        self.assertFalse(os.path.exists(dummy_folder_path))


if __name__ == '__main__':
    main()
