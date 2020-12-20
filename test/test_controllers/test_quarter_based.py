"""Module containing tests for the quarter based controller"""
import collections
import os
from datetime import date, datetime, timedelta
from typing import Optional, Type, Iterator, Dict, Any, List, Union, Tuple
from unittest import TestCase, main
from unittest.mock import patch

import pytz
from dotenv import load_dotenv
from freezegun import freeze_time
from sqlalchemy.ext.declarative import declarative_base
import sqlalchemy as orm

from judah.controllers.base.quarter_based import QuarterBasedBaseController
from judah.sources.base.quarter_based import QuarterBasedBaseSource
from judah.destinations.database.model import DatabaseBaseModel
from judah.destinations.database.connection import DatabaseConnectionConfig, DatabaseConnection
from judah.transformers.base import BaseTransformer
from judah.utils.cron import CronType, Hour
from judah.utils.dates import update_quarter_year_tuple
from test.utils import delete_tables_from_database, create_tables_in_database, drop_schema

_ROOT_PATH = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))

load_dotenv(os.path.join(_ROOT_PATH, '.env'))

_TEST_DB_URI = os.environ.get('TEST_POSTGRES_DB_URI')

_TEST_DB_BASE = declarative_base()
_TABLE_NAME = 'dummy3'
_SCHEMA_NAME = 'test_schema'


class ChildQuarterBasedModel(DatabaseBaseModel, _TEST_DB_BASE):
    """Child Quarter Based Model"""
    __tablename__ = _TABLE_NAME
    __table_args__: Dict = {'schema': _SCHEMA_NAME}
    _db_configuration: DatabaseConnectionConfig = DatabaseConnectionConfig(db_uri=_TEST_DB_URI)
    _base_declarative_class: Type[declarative_base()] = _TEST_DB_BASE
    _datetime_fields: Optional[List[str]] = ["Date"]

    Date = orm.Column(orm.Date, primary_key=True)
    number = orm.Column(orm.Integer, primary_key=True)
    Capacity = orm.Column(orm.Integer)
    Price = orm.Column(orm.Integer)


class ChildQuarterBasedSource(QuarterBasedBaseSource):
    """Child Quarter based source"""
    start_quarter_and_year: Optional[Tuple[int, int]] = (1, 1998)
    end_quarter_and_year: Optional[Tuple[int, int]] = (3, 2000)

    def _query_data_source(
            self, start_quarter_and_year: Tuple[int, int],
            end_quarter_and_year: Tuple[int, int]) -> Iterator[Dict[str, Any]]:
        pass


class ChildQuarterBasedController(QuarterBasedBaseController):
    """Child quarter based controller"""
    start_quarter_and_year: Optional[Tuple[int, int]] = (2, 2020)
    end_quarter_and_year: Optional[Tuple[int, int]] = (4, 2021)
    source_class: Type[QuarterBasedBaseSource] = ChildQuarterBasedSource
    destination_model_class = ChildQuarterBasedModel


class TestQuarterBasedController(TestCase):
    """Tests for the QuarterBasedController"""

    def setUp(self) -> None:
        """Initialize some variables"""
        self.data_from_source = [
            {"Date": date(year=2020, month=3, day=9), "number": 1, "Capacity": 16616, "Price": 67},
            {"Date": date(year=2020, month=3, day=12), "number": 2, "Capacity": 16516, "Price": 567},
            {"Date": date(year=2020, month=3, day=10), "number": 3, "Capacity": 16616, "Price": 637},
            {"Date": date(year=2020, month=3, day=9), "number": 4, "Capacity": 16620, "Price": 617},
        ]

    @patch.object(ChildQuarterBasedModel, 'get_last_saved_timestamp')
    def test_update_last_timestamp(self, mock_get_last_saved_timestamp):
        """Should update the last timestamp property of the Controller class and the start_quarter_and_year"""
        timestamp = datetime(year=2015, month=10, day=6, hour=13, minute=45, second=10)
        mock_get_last_saved_timestamp.return_value = timestamp
        expected_start_quarter_and_year = (4, 2015)

        self.assertIsNone(ChildQuarterBasedController._last_timestamp)
        self.assertNotEqual(ChildQuarterBasedController.start_quarter_and_year, expected_start_quarter_and_year)

        ChildQuarterBasedController._update_last_timestamp()
        mock_get_last_saved_timestamp.assert_called_once()

        self.assertEqual(ChildQuarterBasedController._last_timestamp, timestamp)
        self.assertEqual(ChildQuarterBasedController.start_quarter_and_year, expected_start_quarter_and_year)

    @patch.object(ChildQuarterBasedController, '_update_source')
    @patch.object(ChildQuarterBasedSource, '_query_data_source')
    def test_query_source(self, mock_query_data_source, mock_update_source):
        """
        Should query the data source for the given quarter_year tuples and return the iterator of data dictionaries
        as well as increment the start quarter_and_year by the default batch size in quarters
        """
        source = ChildQuarterBasedSource()
        mock_query_data_source.return_value = (record for record in self.data_from_source)

        # set the source of the controller
        ChildQuarterBasedController._source = source
        original_start_quarter_and_year = source.start_quarter_and_year
        original_end_quarter_and_year = source.end_quarter_and_year

        response = ChildQuarterBasedController._query_source()
        self.assertIsInstance(response, collections.Iterator)

        received_data = [record for record in response]
        next_quarter_and_year = update_quarter_year_tuple(
            original_start_quarter_and_year,
            quarters_to_increment_by=source.default_batch_size_in_quarters,
            quarters_to_decrement_by=0
        )

        mock_query_data_source.assert_called_once_with(
            start_quarter_and_year=original_start_quarter_and_year,
            end_quarter_and_year=original_end_quarter_and_year)
        mock_update_source.assert_called_once_with(
            start_quarter_and_year=ChildQuarterBasedController.start_quarter_and_year,
            end_quarter_and_year=ChildQuarterBasedController.end_quarter_and_year)

        self.assertListEqual(received_data, self.data_from_source)
        self.assertEqual(source.start_quarter_and_year, next_quarter_and_year)

    @patch.object(ChildQuarterBasedSource, '_query_data_source')
    def test_extract(self, mock_query_data_source):
        """Should extract data from the source and return the iterator of data"""
        mock_query_data_source.return_value = (record for record in self.data_from_source)

        response = ChildQuarterBasedController.extract()
        self.assertIsInstance(response, collections.Iterator)

        received_data = [record for record in response]

        self.assertListEqual(received_data, self.data_from_source)
        mock_query_data_source.assert_called_once_with(
            start_quarter_and_year=ChildQuarterBasedController.start_quarter_and_year,
            end_quarter_and_year=ChildQuarterBasedController.end_quarter_and_year)

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

        ChildQuarterBasedController.transformer_classes = [IntegerToFloatTransformer, FloatToStringTransformer]
        response = ChildQuarterBasedController.transform(sample_data)
        self.assertIsInstance(response, collections.Iterator)

        received_data = [record for record in response]
        self.assertEqual(len(received_data), 1)
        self.assertDictEqual(received_data[0], expected_data)

    def test_load(self):
        """Should load data into the destination and return the data iterator that was put in the destination"""
        create_tables_in_database(db_configuration=ChildQuarterBasedModel._db_configuration,
                                  model_base=_TEST_DB_BASE, schema_name=_SCHEMA_NAME)

        with DatabaseConnection.get_db_connection(
                db_connection_config=ChildQuarterBasedModel._db_configuration) as db_connection:
            raw_data = self.data_from_source[0]

            record_from_database_pre_load = db_connection.db_session.query(ChildQuarterBasedModel).filter_by(
                Date=raw_data['Date'], number=raw_data['number']).first()
            db_connection.db_session.commit()
            self.assertIsNone(record_from_database_pre_load)

            response = ChildQuarterBasedController.load(raw_data)

            loaded_data = [record for record in response]
            self.assertEqual(len(loaded_data), 1)
            self.assertDictEqual(loaded_data[0], raw_data)

            record_from_database_post_load = db_connection.db_session.query(ChildQuarterBasedModel).filter_by(
                Date=raw_data['Date'], number=raw_data['number']).first()
            db_connection.db_session.commit()
            self.assertIsInstance(record_from_database_post_load, ChildQuarterBasedModel)

            for field, value in raw_data.items():
                self.assertEqual(getattr(record_from_database_post_load, field), value)

    def test_update_source(self):
        """
        Should update the configuration of the source especially
        those attributes that are bound to change and update the source's attributes with those from the model
        """
        self.assertRaises(AttributeError, getattr, ChildQuarterBasedController, '_source')
        start_quarter_and_year = (2, 1997)
        end_quarter_and_year = (4, 2003)
        default_batch_size_in_quarters = 10
        expected_attributes = ChildQuarterBasedModel.get_attributes()

        ChildQuarterBasedController._update_source(
            start_quarter_and_year=start_quarter_and_year, end_quarter_and_year=end_quarter_and_year,
            default_batch_size_in_quarters=default_batch_size_in_quarters)

        source = ChildQuarterBasedController._source

        self.assertIsInstance(source, ChildQuarterBasedSource)
        self.assertTupleEqual(source.start_quarter_and_year, start_quarter_and_year)
        self.assertTupleEqual(source.end_quarter_and_year, end_quarter_and_year)
        self.assertEqual(source.default_batch_size_in_quarters, default_batch_size_in_quarters)
        self.assertListEqual(source.attributes, expected_attributes)

    def test_update_interval(self):
        """Should update the _interval basing on the interval_in_milliseconds"""
        ChildQuarterBasedController.interval_in_milliseconds = 3569

        self.assertIsNone(ChildQuarterBasedController._interval)

        ChildQuarterBasedController._update_interval()

        self.assertEqual(ChildQuarterBasedController._interval,
                         timedelta(milliseconds=ChildQuarterBasedController.interval_in_milliseconds))

    @patch.object(ChildQuarterBasedModel, 'get_last_saved_timestamp')
    def test_initialize(self, mock_get_last_saved_timestamp):
        """
        Should create the tables if these are non-existent and
        update the _interval and _last_timestamp if necessary
        """
        drop_schema(db_configuration=ChildQuarterBasedModel._db_configuration, schema_name=_SCHEMA_NAME)

        timestamp = datetime.now()
        mock_get_last_saved_timestamp.return_value = timestamp
        ChildQuarterBasedController.interval_in_milliseconds = 3289

        self.assertIsNone(ChildQuarterBasedController._interval)
        self.assertIsNone(ChildQuarterBasedController._last_timestamp)

        with DatabaseConnection.get_db_connection(
                db_connection_config=ChildQuarterBasedModel._db_configuration) as db_connection:
            table_check_sql = f"""
            SELECT EXISTS (
               SELECT FROM pg_tables
               WHERE  schemaname = '{_SCHEMA_NAME}'
               AND    tablename  = '{_TABLE_NAME}'
               )
            """
            self.assertFalse(db_connection.execute_sql(table_check_sql).first()[0])

            ChildQuarterBasedController._initialize()

            self.assertTrue(db_connection.execute_sql(table_check_sql).first()[0])

        mock_get_last_saved_timestamp.assert_called_once()
        self.assertEqual(ChildQuarterBasedController._interval,
                         timedelta(milliseconds=ChildQuarterBasedController.interval_in_milliseconds))
        self.assertEqual(ChildQuarterBasedController._last_timestamp, timestamp)

    @freeze_time('1997-04-13 08:46:30+00:00')
    def test_get_closest_next_timestamp(self):
        """
        Should get the timestamp from the available CronTypes that is the next to be run
        """
        utc_now = datetime.utcnow().astimezone(pytz.utc)
        cron_job_for_next_second = CronType(second=utc_now.second + 1)
        cron_job_after_8_second = CronType(second=utc_now.second + 8)
        cron_job_for_next_day = CronType(hour=Hour(utc_now.hour) - 2)

        ChildQuarterBasedController.cron_jobs = [
            cron_job_after_8_second,
            cron_job_for_next_second,
            cron_job_for_next_day,
        ]

        self.assertEqual(
            ChildQuarterBasedController._get_closest_next_timestamp(),
            cron_job_for_next_second.get_next_utc_timestamp())

    def test_wait_for_interval_to_elapse(self):
        """
        Should wait for the interval to elapse before starting again, given a start time.
        margin of error is 1 second
        """
        time_in_seconds_to_wait = 2
        expected_timedelta_to_wait = timedelta(seconds=time_in_seconds_to_wait)
        ChildQuarterBasedController._interval = expected_timedelta_to_wait

        timestamp_before_waiting = datetime.now(tz=pytz.utc)
        ChildQuarterBasedController._wait_for_interval_to_elapse(start_time_in_utc=timestamp_before_waiting)
        timestamp_after_waiting = datetime.now(tz=pytz.utc)

        actual_time_elapsed = timestamp_after_waiting - timestamp_before_waiting

        self.assertGreaterEqual(actual_time_elapsed, expected_timedelta_to_wait)
        self.assertLessEqual(actual_time_elapsed, expected_timedelta_to_wait + timedelta(seconds=1))

    @patch.object(ChildQuarterBasedController, '_get_closest_next_timestamp')
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

        ChildQuarterBasedController._wait_for_interval_to_elapse(start_time_in_utc=timestamp_before_waiting)
        timestamp_after_waiting = datetime.now(tz=pytz.utc)

        actual_time_elapsed = timestamp_after_waiting - timestamp_before_waiting

        self.assertGreaterEqual(actual_time_elapsed, expected_timedelta_to_wait)
        self.assertLessEqual(actual_time_elapsed, expected_timedelta_to_wait + timedelta(seconds=1))

    @patch.object(ChildQuarterBasedController, '_get_closest_next_timestamp')
    def test_wait_for_interval_to_elapse_for_both_cron_jobs_and_interval(self, mock_get_closest_next_timestamp):
        """
        Should wait only for the interval generated by the cron_job not that from the interval property
        margin of error is 1 second
        """
        time_in_seconds_to_wait = 2
        expected_timedelta_to_wait = timedelta(seconds=time_in_seconds_to_wait)
        ChildQuarterBasedController._interval = timedelta(seconds=10)

        timestamp_before_waiting = datetime.now(tz=pytz.utc)
        mock_get_closest_next_timestamp.return_value = (
                timestamp_before_waiting.astimezone(pytz.utc) + expected_timedelta_to_wait)

        ChildQuarterBasedController._wait_for_interval_to_elapse(start_time_in_utc=timestamp_before_waiting)
        timestamp_after_waiting = datetime.now(tz=pytz.utc)

        actual_time_elapsed = timestamp_after_waiting - timestamp_before_waiting

        self.assertGreaterEqual(actual_time_elapsed, expected_timedelta_to_wait)
        self.assertLess(actual_time_elapsed, ChildQuarterBasedController._interval)
        self.assertLessEqual(actual_time_elapsed, expected_timedelta_to_wait + timedelta(seconds=1))

    def tearDown(self) -> None:
        # reset the Controller class
        ChildQuarterBasedController.transformer_classes = []
        ChildQuarterBasedController._interval = None
        ChildQuarterBasedController._last_timestamp = None
        ChildQuarterBasedController.cron_jobs = []

        try:
            del ChildQuarterBasedController._source
        except AttributeError:
            pass

        try:
            delete_tables_from_database(db_configuration=ChildQuarterBasedModel._db_configuration,
                                        table_name=_TABLE_NAME, schema_name=_SCHEMA_NAME)
        except Exception:
            pass

        DatabaseConnection.close_all_connections()
        DatabaseConnection.remove_all_connections()


if __name__ == '__main__':
    main()
