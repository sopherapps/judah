"""Module containing the utilities that are to do with CRON jobs"""
from datetime import date, time, datetime, timedelta
from typing import Optional, Dict, Any

import pytz
from pydantic import BaseModel
from enum import IntEnum

from judah.utils.dates import add_month_to_datetime_but_maintain_day


class Month(IntEnum):
    JANUARY = 1
    FEBRUARY = 2
    MARCH = 3
    APRIL = 4
    MAY = 5
    JUNE = 6
    JULY = 7
    AUGUST = 8
    SEPTEMBER = 9
    OCTOBER = 10
    NOVEMBER = 11
    DECEMBER = 12


class WeekDay(IntEnum):
    MONDAY = 0
    TUESDAY = 1
    WEDNESDAY = 2
    THURSDAY = 3
    FRIDAY = 4
    SATURDAY = 5
    SUNDAY = 6


class Hour(IntEnum):
    TWELVE_AM = 0
    ONE_AM = 1
    TWO_AM = 2
    THREE_AM = 3
    FOUR_AM = 4
    FIVE_AM = 5
    SIX_AM = 6
    SEVEN_AM = 7
    EIGHT_AM = 8
    NINE_AM = 9
    TEN_AM = 10
    ELEVEN_AM = 11
    TWELVE_PM = 12
    ONE_PM = 13
    TWO_PM = 14
    THREE_PM = 15
    FOUR_PM = 16
    FIVE_PM = 17
    SIX_PM = 18
    SEVEN_PM = 19
    EIGHT_PM = 20
    NINE_PM = 21
    TEN_PM = 22
    ELEVEN_PM = 23


class CronType(BaseModel):
    day_of_week: Optional[WeekDay] = None
    year: Optional[int] = None
    month: Optional[Month] = None
    day: Optional[int] = None
    hour: Optional[Hour] = None
    minute: Optional[int] = None
    second: Optional[int] = None
    timezone_string: Optional[str] = 'UTC'

    class Config:
        arbitrary_types_allowed = True

    def get_next_utc_timestamp(self) -> Optional[datetime]:
        """
        The main function that is called by the Controller to get the next UTC timestamp
        from which to extract the appropriate delay that the controller will have to wait for
        """
        utc_now = datetime.now(tz=pytz.UTC)
        generated_timestamp = self.__get_timestamp_from_fields(default_timestamp=utc_now)

        if generated_timestamp >= utc_now:
            next_timestamp = generated_timestamp
        else:
            next_timestamp = self.__get_next_timestamp(generated_timestamp)

        return next_timestamp if next_timestamp >= utc_now else None

    def __update_fields_with_defaults(self):
        """Updates the fields with the generated default values"""
        defaults = self._generate_field_defaults()

        for key, value in defaults.items():
            setattr(self, key, value)

    def __get_timestamp_from_fields(self, default_timestamp: datetime):
        """Constructs a timestamp from the fields, replacing None values with those from the default timestamp"""
        self.__update_fields_with_defaults()

        next_date = self._get_date()
        next_time = self._get_time()
        timezone = pytz.timezone(self.timezone_string)

        datetime_options = dict(
            year=default_timestamp.year, month=default_timestamp.month, day=default_timestamp.day,
            hour=default_timestamp.hour, minute=default_timestamp.minute, second=default_timestamp.second)

        if next_date is not None:
            datetime_options = {
                **datetime_options, **dict(day=next_date.day, year=next_date.year, month=next_date.month)}

        if next_time is not None:
            datetime_options = {
                **datetime_options, **dict(hour=next_time.hour, minute=next_time.minute, second=next_time.second)}

        return datetime(**datetime_options, tzinfo=timezone).astimezone(pytz.UTC)

    def __get_next_timestamp(self, current_timestamp: datetime):
        """Gets the timestamp after the current timestamp"""
        if isinstance(self.year, int):
            # once a year is set, only one timestamp can be derived from it.
            return current_timestamp
        elif isinstance(self.day_of_week, WeekDay):
            # shift it by 7 days
            return current_timestamp + timedelta(days=7)
        elif isinstance(self.month, Month):
            # forward it by a year
            return current_timestamp.replace(year=current_timestamp.year + 1)
        elif isinstance(self.day, int):
            # forward it by a month
            return add_month_to_datetime_but_maintain_day(current_timestamp)
        elif isinstance(self.hour, Hour):
            # forward it by a day
            return current_timestamp + timedelta(days=1)
        elif isinstance(self.minute, int):
            # forward it by an hour
            return current_timestamp + timedelta(hours=1)
        elif isinstance(self.second, int):
            # forward it by a minute
            return current_timestamp + timedelta(minutes=1)
        return current_timestamp

    def _get_next_date_from_day_of_week(self) -> Optional[date]:
        """
        Constructs the date of the next day_of_week e.g. next Sunday if day_of_week = WeekDay.SUNDAY
        """
        if self.day_of_week is None:
            return None

        today = date.today()

        current_weekday = today.weekday()
        number_of_days_ahead = self.day_of_week.value - current_weekday

        if number_of_days_ahead < 0:
            # the self.day_of_week has already passed for current week so go to next week
            number_of_days_ahead += 7

        return today + timedelta(days=number_of_days_ahead)

    def _get_date(self) -> Optional[date]:
        """
        Constructs the datetime.date basing on the ‘day_of_week’, ‘year’, ‘month’ and ‘day’
        """
        next_date_for_day_of_week = self._get_next_date_from_day_of_week()

        if isinstance(next_date_for_day_of_week, date):
            return next_date_for_day_of_week

        today = date.today()
        date_fields = ('day', 'year',)
        date_options = {key: getattr(self, key) for key in date_fields if getattr(self, key) is not None}

        if self.month is not None:
            date_options['month'] = self.month.value

        if len(date_options) == 0:
            return None

        # fill up the date options with the defaults
        date_options = {'day': today.day, 'year': today.year, 'month': Month(today.month), **date_options}

        return date(**date_options)

    def _get_time(self) -> Optional[time]:
        """
        Constructs the datetime.time basing on the ‘hour’, ‘minute’, ‘second’, 
        """
        time_fields = ('minute', 'second',)
        time_options = {key: getattr(self, key) for key in time_fields if getattr(self, key) is not None}
        utc_now = datetime.now(tz=pytz.UTC)

        if self.hour is not None:
            time_options['hour'] = self.hour.value

        if len(time_options) == 0:
            return None

        # fill up the time options with the defaults
        time_options = {'hour': utc_now.hour, 'minute': utc_now.minute, 'second': utc_now.second, **time_options}

        return time(**time_options)

    def _generate_field_defaults(self) -> Dict[str, Any]:
        """
        Generates the default values of the other fields if a particular field is set
        e.g. if the ‘hour’ field is not None, the ‘minute’, ‘second’,  fields
        will automatically be set to zero (0)
        """
        defaults = self.dict().copy()
        date_fields = ('day_of_week', 'year', 'month', 'day',)

        should_set_month = False
        should_set_day = False
        should_set_hour = False
        should_set_minute = False
        should_set_second = False

        if defaults['year'] is not None:
            should_set_month = defaults['month'] is None
            should_set_day = defaults['day'] is None
        if defaults['month'] is not None:
            should_set_day = defaults['day'] is None
        if any([defaults[field] is not None for field in date_fields]):
            should_set_hour = defaults['hour'] is None
            should_set_minute = defaults['minute'] is None
            should_set_second = defaults['second'] is None
        if self.hour is not None:
            should_set_minute = defaults['minute'] is None
            should_set_second = defaults['second'] is None
        if self.minute is not None:
            should_set_second = defaults['second'] is None

        if should_set_month:
            defaults['month'] = Month.JANUARY
        if should_set_day:
            defaults['day'] = 1
        if should_set_hour:
            defaults['hour'] = Hour.TWELVE_AM
        if should_set_minute:
            defaults['minute'] = 0
        if should_set_second:
            defaults['second'] = 0

        return defaults
