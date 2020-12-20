"""Module containing a base controller class"""
import logging
import time
from datetime import datetime, timedelta
from typing import Type, Optional, Dict, Any, Iterator, List, Tuple

import pytz
from selenium.common.exceptions import WebDriverException
from urllib3.exceptions import ProtocolError

from ...destinations.base import DestinationBaseModel
from ...sources.base import BaseSource
from ...transformers.base import BaseTransformer
from email_notifier import ExceptionNotifier

from ...utils.cron import CronType
from ...utils.testing import TestingTimeoutError


class BaseController:
    """
    Class for the controller that gets data from a Source,
    transforms it
    and loads it in a destination basing on a given model
    """
    _source: BaseSource
    _interval: Optional[timedelta] = None
    _last_timestamp: Optional[datetime] = None

    destination_model_class: Type[DestinationBaseModel]
    source_class: Type[BaseSource]
    transformer_classes: List[Type[BaseTransformer]] = []
    interval_in_milliseconds: Optional[int] = None
    # cron_jobs override the interval; use one or the other
    cron_jobs: List[CronType] = []
    # exceptions
    _known_extraction_exceptions: Tuple[Exception] = (
        ProtocolError,
        WebDriverException,
        OSError,
        ConnectionError,
    )
    _known_transformation_exceptions: Tuple[Exception] = (OSError,)
    _known_loading_exceptions: Tuple[Exception] = (OSError,)
    _fatal_extraction_exceptions: Tuple[Exception] = (TestingTimeoutError,)
    _fatal_transformation_exceptions: Tuple[Exception] = (TestingTimeoutError,)
    _fatal_loading_exceptions: Tuple[Exception] = (TestingTimeoutError,)

    @classmethod
    def extract(cls) -> Iterator[Dict[Any, Any]]:
        """Initializes the extraction of the data from the source"""
        cls._initialize()
        # for intervals, no waiting before yield on first run.
        # for cron job, wait for the exact time before any yield
        should_wait_before_yielding = len(cls.cron_jobs) > 0

        while True:
            try:
                start_time = datetime.now(tz=pytz.utc)

                if should_wait_before_yielding:
                    cls._wait_for_interval_to_elapse(start_time_in_utc=start_time)

                yield from cls._query_source()

                if cls._interval is None and len(cls.cron_jobs) == 0:
                    break

                should_wait_before_yielding = True

            except cls._fatal_extraction_exceptions as exp:
                raise exp
            except cls._known_extraction_exceptions as exp:
                logging.error(f'{cls._source.name} Extraction \n{exp}')
                yield from []
            except Exception as unknown_exception:
                notifier = ExceptionNotifier(subject=f'[Unknown] {cls._source.name} Extraction')
                notifier.notify(unknown_exception)

    @classmethod
    def transform(cls, data: Optional[Dict[Any, Any]]) -> Iterator[Optional[Dict[Any, Any]]]:
        """Transforms the data row by row into something else"""
        if data is None:
            yield None

        try:
            transformed_data = data

            for transformer in cls.transformer_classes:
                if isinstance(transformed_data, list):
                    list_output = []

                    for datum in transformed_data:
                        data_from_current_transformer = transformer.run(data=datum)

                        # spread the returned list into the final output
                        if isinstance(data_from_current_transformer, list):
                            list_output = list_output + data_from_current_transformer
                        # append the returned item to the list output
                        else:
                            list_output.append(data_from_current_transformer)

                    transformed_data = list_output
                else:
                    transformed_data = transformer.run(data=transformed_data)

            if isinstance(transformed_data, list):
                yield from transformed_data
            else:
                yield transformed_data
        except cls._fatal_transformation_exceptions as exp:
            raise exp
        except cls._known_transformation_exceptions as exp:
            logging.error(f'{cls._source.name} Transformation \n{exp}')
            yield None
        except Exception as unknown_exception:
            notifier = ExceptionNotifier(subject=f'[Unknown] {cls._source.name} Transformation')
            notifier.notify(unknown_exception)

    @classmethod
    def load(cls, data: Optional[Dict[Any, Any]]) -> Iterator[Optional[Dict[Any, Any]]]:
        """Loads the data into the destination"""
        if data is None:
            yield None

        try:
            yield cls.destination_model_class.upsert(data=data)
        except cls._fatal_loading_exceptions as exp:
            raise exp
        except cls._known_loading_exceptions as exp:
            logging.error(f'{cls._source.name} Loading \n{exp}')
            yield None
        except Exception as unknown_exception:
            notifier = ExceptionNotifier(subject=f'[Unknown] {cls._source.name} Loading')
            notifier.notify(unknown_exception)

    @classmethod
    def _update_source(cls, *args, **kwargs) -> None:
        """
        Updates the configuration of the source especially
        those attributes that are bound to change
        """
        if not hasattr(cls, '_source'):
            cls._source = cls.source_class(
                attributes=cls.destination_model_class.get_attributes(),
                *args, **kwargs)

    @classmethod
    def _update_last_timestamp(cls):
        """Updates the last timestamp property of this class"""
        cls._last_timestamp = cls.destination_model_class.get_last_saved_timestamp()

    @classmethod
    def _update_interval(cls) -> None:
        """Updates the _interval basing on the interval_in_milliseconds"""
        if isinstance(cls.interval_in_milliseconds, int):
            cls._interval = timedelta(milliseconds=cls.interval_in_milliseconds)

    @classmethod
    def _initialize(cls) -> None:
        """Creates the tables if these are non-existent and updates the _interval if necessary"""
        cls._update_interval()
        cls.destination_model_class.initialize()
        cls._update_last_timestamp()

    @classmethod
    def _query_source(cls, ) -> Iterator[Dict[Any, Any]]:
        """Queries the data source"""
        cls._update_source()
        return cls._source.get()

    @classmethod
    def _wait_for_interval_to_elapse(cls, start_time_in_utc: datetime) -> None:
        """Waits for the interval to elapse before starting again"""
        next_timestamp = cls._get_closest_next_timestamp()

        if not isinstance(cls._interval, timedelta) and not isinstance(next_timestamp, datetime):
            return

        interval = cls._interval

        if next_timestamp is not None:
            interval = next_timestamp - start_time_in_utc

        time_used_up_so_far = datetime.now(tz=pytz.utc) - start_time_in_utc
        seconds_left_off_interval = (interval - time_used_up_so_far).total_seconds()

        if seconds_left_off_interval > 0:
            for index in range(round(seconds_left_off_interval)):
                time.sleep(1)

    @classmethod
    def _get_closest_next_timestamp(cls) -> Optional[datetime]:
        """
        Gets the timestamp from the available CronTypes that is the next to be run
        i.e. the earliest of all the available ‘next timestamps
        """
        timestamps = [cron_job.get_next_utc_timestamp() for cron_job in cls.cron_jobs]
        timestamps_without_nones = [timestamp for timestamp in timestamps if timestamp is not None]
        timestamps_without_nones.sort()

        try:
            return timestamps_without_nones[0]
        except IndexError:
            return None
