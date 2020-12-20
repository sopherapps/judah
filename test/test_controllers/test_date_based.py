"""Module containing tests for the date based controller"""
import collections
import os
from datetime import date, datetime, timedelta
from typing import Optional, Type, Iterator, Dict, Any, List, Union
from unittest import TestCase, main
from unittest.mock import patch, Mock

import pytz
from dotenv import load_dotenv
from freezegun import freeze_time
from sqlalchemy.ext.declarative import declarative_base
import sqlalchemy as orm

from judah.controllers.base.date_based import DateBasedBaseController
from judah.sources.base.date_based import DateBasedBaseSource
from judah.destinations.database.model import DatabaseBaseModel
from judah.destinations.database.connection import DatabaseConnectionConfig, DatabaseConnection
from judah.transformers.base import BaseTransformer
from judah.utils.cron import CronType, Hour
from test.utils import delete_tables_from_database, create_tables_in_database, drop_schema

_ROOT_PATH = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))

load_dotenv(os.path.join(_ROOT_PATH, '.env'))

_TEST_DB_URI = os.environ.get('TEST_POSTGRES_DB_URI')

_TEST_DB_BASE = declarative_base()
_TABLE_NAME = 'dummy2'
_SCHEMA_NAME = 'test_schema'


class ChildDateBasedModel(DatabaseBaseModel, _TEST_DB_BASE):
    """Child Date Based Model"""
    __tablename__ = _TABLE_NAME
    __table_args__: Dict = {'schema': _SCHEMA_NAME}
    _db_configuration: DatabaseConnectionConfig = DatabaseConnectionConfig(db_uri=_TEST_DB_URI)
    _base_declarative_class: Type[declarative_base()] = _TEST_DB_BASE
    _datetime_fields: Optional[List[str]] = ["Date"]

    Date = orm.Column(orm.Date, primary_key=True)
    number = orm.Column(orm.Integer, primary_key=True)
    Capacity = orm.Column(orm.Integer)
    Price = orm.Column(orm.Integer)


class ChildDateBasedSource(DateBasedBaseSource):
    """Child Date based source"""
    start_date: date = date(year=2019, month=8, day=9)
    end_date: date = date(year=2019, month=8, day=19)

    def _query_data_source(self, start_date: date, end_date: date) -> Iterator[Dict[str, Any]]:
        pass


class ChildDateBasedController(DateBasedBaseController):
    """Child date based controller"""
    start_date: Optional[date] = date(year=2020, month=1, day=1)
    end_date: Optional[date] = date(year=2020, month=1, day=1)
    source_class: Type[DateBasedBaseSource] = ChildDateBasedSource
    destination_model_class = ChildDateBasedModel


class TestDateBasedController(TestCase):
    """Tests for the DateBasedController"""

    def setUp(self) -> None:
        """Initialize some variables"""
        self.data_from_source = [
            {"Date": date(year=2020, month=3, day=9), "number": 1, "Capacity": 16616, "Price": 67},
            {"Date": date(year=2020, month=3, day=12), "number": 2, "Capacity": 16516, "Price": 567},
            {"Date": date(year=2020, month=3, day=10), "number": 3, "Capacity": 16616, "Price": 637},
            {"Date": date(year=2020, month=3, day=9), "number": 4, "Capacity": 16620, "Price": 617},
        ]

    @patch.object(ChildDateBasedModel, 'get_last_saved_timestamp')
    def test_update_last_timestamp(self, mock_get_last_saved_timestamp):
        """Should update the last timestamp property of the Controller class and the start_date"""
        timestamp = datetime.now()
        mock_get_last_saved_timestamp.return_value = timestamp

        self.assertIsNone(ChildDateBasedController._last_timestamp)
        self.assertNotEqual(ChildDateBasedController.start_date, timestamp)

        ChildDateBasedController._update_last_timestamp()
        mock_get_last_saved_timestamp.assert_called_once()

        self.assertEqual(ChildDateBasedController._last_timestamp, timestamp)
        self.assertEqual(ChildDateBasedController.start_date, timestamp)

    @patch.object(ChildDateBasedController, '_update_source')
    @patch.object(ChildDateBasedSource, '_query_data_source')
    def test_query_source(self, mock_query_data_source, mock_update_source):
        """
        Should query the data source for the given dates and return the iterator of data dictionaries
        as well as increment the start date by the default batch size in days
        """
        source = ChildDateBasedSource()
        mock_query_data_source.return_value = (record for record in self.data_from_source)
        # set the source of the controller
        ChildDateBasedController._source = source
        original_start_date = source.start_date
        original_end_date = source.end_date

        response = ChildDateBasedController._query_source()
        self.assertIsInstance(response, collections.Iterator)

        received_data = [record for record in response]

        mock_query_data_source.assert_called_once_with(
            start_date=original_start_date, end_date=original_end_date)
        mock_update_source.assert_called_once_with(
            start_date=ChildDateBasedController.start_date, end_date=ChildDateBasedController.end_date)

        self.assertListEqual(received_data, self.data_from_source)
        self.assertEqual(
            source.start_date, original_start_date + timedelta(days=source.default_batch_size_in_days))

    @patch.object(ChildDateBasedSource, '_query_data_source')
    def test_extract(self, mock_query_data_source):
        """Should extract data from the source and return the iterator of data"""
        mock_query_data_source.return_value = (record for record in self.data_from_source)

        response = ChildDateBasedController.extract()
        self.assertIsInstance(response, collections.Iterator)

        received_data = [record for record in response]

        self.assertListEqual(received_data, self.data_from_source)
        mock_query_data_source.assert_called_once_with(
            start_date=ChildDateBasedController.start_date, end_date=ChildDateBasedController.end_date)

    def test_transform(self):
        """Should transform the data row to another form"""
        sample_data = self.data_from_source[0]
        expected_data = {"Date": date(year=2020, month=3, day=9), "number": "1.0", "Capacity": "16616.0", "Price": "67.0"}

        class IntegerToFloatTransformer(BaseTransformer):
            """Transforms all integer values to floats"""

            @classmethod
            def run(cls, data: Dict[Any, Any]) -> Union[Dict[Any, Any], List[Any]]:
                return {key: float(value) if isinstance(value, int) else value for key, value in data.items()}

        class FloatToStringTransformer(BaseTransformer):
            """Transforms all float values to strings"""

            @classmethod
            def run(cls, data: Dict[Any, Any]) -> Union[Dict[Any, Any], List[Any]]:
                return {key: str(value) if isinstance(value, float) else value for key, value in data.items()}

        ChildDateBasedController.transformer_classes = [IntegerToFloatTransformer, FloatToStringTransformer]
        response = ChildDateBasedController.transform(sample_data)
        self.assertIsInstance(response, collections.Iterator)

        received_data = [record for record in response]
        self.assertEqual(len(received_data), 1)
        self.assertDictEqual(received_data[0], expected_data)

    def test_load(self):
        """Should load data into the destination and return the data iterator that was put in the destination"""
        create_tables_in_database(db_configuration=ChildDateBasedModel._db_configuration,
                                  model_base=_TEST_DB_BASE, schema_name=_SCHEMA_NAME)

        with DatabaseConnection.get_db_connection(
                db_connection_config=ChildDateBasedModel._db_configuration) as db_connection:
            raw_data = self.data_from_source[0]

            record_from_database_pre_load = db_connection.db_session.query(ChildDateBasedModel).filter_by(
                Date=raw_data['Date'], number=raw_data['number']).first()
            db_connection.db_session.commit()
            self.assertIsNone(record_from_database_pre_load)

            response = ChildDateBasedController.load(raw_data)

            loaded_data = [record for record in response]
            self.assertEqual(len(loaded_data), 1)
            self.assertDictEqual(loaded_data[0], raw_data)

            record_from_database_post_load = db_connection.db_session.query(ChildDateBasedModel).filter_by(
                Date=raw_data['Date'], number=raw_data['number']).first()
            db_connection.db_session.commit()
            self.assertIsInstance(record_from_database_post_load, ChildDateBasedModel)

            for field, value in raw_data.items():
                self.assertEqual(getattr(record_from_database_post_load, field), value)

    def test_update_source(self):
        """
        Should update the configuration of the source especially
        those attributes that are bound to change and update the source's attributes with those from the model
        """
        self.assertRaises(AttributeError, getattr, ChildDateBasedController, '_source')
        start_date = date(year=1990, month=4, day=7)
        end_date = date(year=2000, month=5, day=8)
        default_batch_size_in_days = 3
        expected_attributes = ChildDateBasedModel.get_attributes()

        ChildDateBasedController._update_source(start_date=start_date, end_date=end_date,
                                                default_batch_size_in_days=default_batch_size_in_days)

        source = ChildDateBasedController._source

        self.assertIsInstance(source, ChildDateBasedSource)
        self.assertEqual(source.start_date, start_date)
        self.assertEqual(source.end_date, end_date)
        self.assertEqual(source.default_batch_size_in_days, default_batch_size_in_days)
        self.assertListEqual(source.attributes, expected_attributes)

    def test_update_interval(self):
        """Should update the _interval basing on the interval_in_milliseconds"""
        ChildDateBasedController.interval_in_milliseconds = 3589

        self.assertIsNone(ChildDateBasedController._interval)

        ChildDateBasedController._update_interval()

        self.assertEqual(ChildDateBasedController._interval,
                         timedelta(milliseconds=ChildDateBasedController.interval_in_milliseconds))

    @patch.object(ChildDateBasedModel, 'get_last_saved_timestamp')
    def test_initialize(self, mock_get_last_saved_timestamp):
        """
        Should create the tables if these are non-existent and
        update the _interval and _last_timestamp if necessary
        """
        drop_schema(db_configuration=ChildDateBasedModel._db_configuration, schema_name=_SCHEMA_NAME)

        timestamp = datetime.now()
        mock_get_last_saved_timestamp.return_value = timestamp
        ChildDateBasedController.interval_in_milliseconds = 3589

        self.assertIsNone(ChildDateBasedController._interval)
        self.assertIsNone(ChildDateBasedController._last_timestamp)

        with DatabaseConnection.get_db_connection(
                db_connection_config=ChildDateBasedModel._db_configuration) as db_connection:
            table_check_sql = f"""
            SELECT EXISTS (
               SELECT FROM pg_tables
               WHERE  schemaname = '{_SCHEMA_NAME}'
               AND    tablename  = '{_TABLE_NAME}'
               )
            """
            self.assertFalse(db_connection.execute_sql(table_check_sql).first()[0])

            ChildDateBasedController._initialize()

            self.assertTrue(db_connection.execute_sql(table_check_sql).first()[0])

        mock_get_last_saved_timestamp.assert_called_once()
        self.assertEqual(ChildDateBasedController._interval,
                         timedelta(milliseconds=ChildDateBasedController.interval_in_milliseconds))
        self.assertEqual(ChildDateBasedController._last_timestamp, timestamp)

    @freeze_time('1997-04-13 08:46:30+00:00')
    def test_get_closest_next_timestamp(self):
        """
        Should get the timestamp from the available CronTypes that is the next to be run
        """
        utc_now = datetime.utcnow().astimezone(pytz.utc)
        cron_job_for_next_second = CronType(second=utc_now.second + 1)
        cron_job_after_8_second = CronType(second=utc_now.second + 8)
        cron_job_for_next_day = CronType(hour=Hour(utc_now.hour) - 2)

        ChildDateBasedController.cron_jobs = [
            cron_job_after_8_second,
            cron_job_for_next_second,
            cron_job_for_next_day,
        ]

        self.assertEqual(
            ChildDateBasedController._get_closest_next_timestamp(),
            cron_job_for_next_second.get_next_utc_timestamp())

    def test_wait_for_interval_to_elapse(self):
        """
        Should wait for the interval to elapse before starting again, given a start time.
        margin of error is 1 second
        """
        time_in_seconds_to_wait = 2
        expected_timedelta_to_wait = timedelta(seconds=time_in_seconds_to_wait)
        ChildDateBasedController._interval = expected_timedelta_to_wait

        timestamp_before_waiting = datetime.now(tz=pytz.utc)
        ChildDateBasedController._wait_for_interval_to_elapse(start_time_in_utc=timestamp_before_waiting)
        timestamp_after_waiting = datetime.now(tz=pytz.utc)

        actual_time_elapsed = timestamp_after_waiting - timestamp_before_waiting

        self.assertGreaterEqual(actual_time_elapsed, expected_timedelta_to_wait)
        self.assertLessEqual(actual_time_elapsed, expected_timedelta_to_wait + timedelta(seconds=1))

    @patch.object(ChildDateBasedController, '_get_closest_next_timestamp')
    def test_wait_for_interval_to_elapse_for_cron_jobs(self, mock_get_closest_next_timestamp):
        """
        Should wait for the interval between start time and the cron_job's next timestamp
        to elapse before starting again.
        margin of error is 1 second
        """
        time_in_seconds_to_wait = 2
        expected_timedelta_to_wait = timedelta(seconds=time_in_seconds_to_wait)

        timestamp_before_waiting = datetime.now(tz=pytz.utc)
        mock_get_closest_next_timestamp.return_value = (
                timestamp_before_waiting.astimezone(pytz.utc) + expected_timedelta_to_wait)

        ChildDateBasedController._wait_for_interval_to_elapse(start_time_in_utc=timestamp_before_waiting)
        timestamp_after_waiting = datetime.now(tz=pytz.utc)

        actual_time_elapsed = timestamp_after_waiting - timestamp_before_waiting

        self.assertGreaterEqual(actual_time_elapsed, expected_timedelta_to_wait)
        self.assertLessEqual(actual_time_elapsed, expected_timedelta_to_wait + timedelta(seconds=1))

    @patch.object(ChildDateBasedController, '_get_closest_next_timestamp')
    def test_wait_for_interval_to_elapse_for_both_cron_jobs_and_interval(self, mock_get_closest_next_timestamp):
        """
        Should wait only for the interval generated by the cron_job not that from the interval property
        margin of error is 1 second
        """
        time_in_seconds_to_wait = 2
        expected_timedelta_to_wait = timedelta(seconds=time_in_seconds_to_wait)
        ChildDateBasedController._interval = timedelta(seconds=10)

        timestamp_before_waiting = datetime.now(tz=pytz.utc)
        mock_get_closest_next_timestamp.return_value = (
                timestamp_before_waiting.astimezone(pytz.utc) + expected_timedelta_to_wait)

        ChildDateBasedController._wait_for_interval_to_elapse(start_time_in_utc=timestamp_before_waiting)
        timestamp_after_waiting = datetime.now(tz=pytz.utc)

        actual_time_elapsed = timestamp_after_waiting - timestamp_before_waiting

        self.assertGreaterEqual(actual_time_elapsed, expected_timedelta_to_wait)
        self.assertLess(actual_time_elapsed, ChildDateBasedController._interval)
        self.assertLessEqual(actual_time_elapsed, expected_timedelta_to_wait + timedelta(seconds=1))

    def tearDown(self) -> None:
        # reset the Controller class
        ChildDateBasedController.transformer_classes = []
        ChildDateBasedController._interval = None
        ChildDateBasedController._last_timestamp = None
        ChildDateBasedController.cron_jobs = []

        try:
            del ChildDateBasedController._source
        except AttributeError:
            pass

        try:
            delete_tables_from_database(db_configuration=ChildDateBasedModel._db_configuration,
                                        table_name=_TABLE_NAME, schema_name=_SCHEMA_NAME)
        except Exception:
            pass

        DatabaseConnection.close_all_connections()
        DatabaseConnection.remove_all_connections()


if __name__ == '__main__':
    main()
