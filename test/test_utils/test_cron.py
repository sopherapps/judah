"""Module containing tests for the cron utility functions"""
from datetime import time, date, timedelta, datetime
from unittest import TestCase, main
from unittest.mock import patch

import pytz
from freezegun import freeze_time

from judah.utils.cron import CronType, WeekDay, Hour, Month


class TestCronType(TestCase):
    """Tests for the CronType class"""

    def setUp(self) -> None:
        """Initialize some variables"""
        self.timezone_name = "Europe/Paris"

    def test_generate_field_defaults(self):
        """
        Should return a dictionary of the default values of the other fields if a particular field is set
        """
        cron_job = CronType()
        expected_defaults = dict(
            day_of_week=None, year=None, month=None, day=None, hour=None,
            minute=None, second=None, timezone_string='UTC',
        )
        self.assertDictEqual(cron_job._generate_field_defaults(), expected_defaults)

    def test_generate_field_defaults_when_day_of_week_is_set(self):
        """
        Should return a dictionary with hour set to Hour.TWELVE_AM, and minute, second, millisecond set to 0
        and day_of_week set to that given day_of_week
        """
        cron_job = CronType(day_of_week=WeekDay.THURSDAY)
        expected_defaults = dict(
            day_of_week=WeekDay.THURSDAY, year=None, month=None, day=None, hour=Hour.TWELVE_AM,
            minute=0, second=0, timezone_string='UTC',
        )
        self.assertDictEqual(cron_job._generate_field_defaults(), expected_defaults)

    def test_generate_field_defaults_when_year_is_set(self):
        """
        Should return a dictionary with
        year set to that year
        month set to Month.January
        day set to 1
        hour set to Hour.TWELVE_AM, and minute, second, millisecond set to 0
        """
        cron_job = CronType(year=1999)
        expected_defaults = dict(
            day_of_week=None, year=1999, month=Month.JANUARY, day=1, hour=Hour.TWELVE_AM,
            minute=0, second=0, timezone_string='UTC',
        )
        self.assertDictEqual(cron_job._generate_field_defaults(), expected_defaults)

    def test_generate_field_defaults_when_month_is_set(self):
        """
        Should return a dictionary with
        month set to that month
        day set to 1
        hour set to Hour.TWELVE_AM, and minute, second, millisecond set to 0
        """
        cron_job = CronType(month=Month.MAY)
        expected_defaults = dict(
            day_of_week=None, year=None, month=Month.MAY, day=1, hour=Hour.TWELVE_AM,
            minute=0, second=0, timezone_string='UTC',
        )
        self.assertDictEqual(cron_job._generate_field_defaults(), expected_defaults)

    def test_generate_field_defaults_when_day_is_set(self):
        """
        Should return a dictionary with
        day set to that day
        hour set to Hour.TWELVE_AM, and minute, second, millisecond set to 0
        """
        cron_job = CronType(day=4)
        expected_defaults = dict(
            day_of_week=None, year=None, month=None, day=4, hour=Hour.TWELVE_AM,
            minute=0, second=0, timezone_string='UTC',
        )
        self.assertDictEqual(cron_job._generate_field_defaults(), expected_defaults)

    def test_generate_field_defaults_when_hour_is_set(self):
        """
        Should return a dictionary with
        hour set to that hour,
        and minute, second, millisecond set to 0
        """
        cron_job = CronType(hour=Hour.TEN_AM)
        expected_defaults = dict(
            day_of_week=None, year=None, month=None, day=None, hour=Hour.TEN_AM,
            minute=0, second=0, timezone_string='UTC',
        )
        self.assertDictEqual(cron_job._generate_field_defaults(), expected_defaults)

    def test_generate_field_defaults_when_minute_is_set(self):
        """
        Should return a dictionary with
        minute is set to that minute,
        and second, millisecond set to 0
        """
        cron_job = CronType(minute=45)
        expected_defaults = dict(
            day_of_week=None, year=None, month=None, day=None, hour=None,
            minute=45, second=0, timezone_string='UTC',
        )
        self.assertDictEqual(cron_job._generate_field_defaults(), expected_defaults)

    def test_generate_field_defaults_when_second_is_set(self):
        """
        Should return a dictionary with
        second is set to that second,
        millisecond set to 0
        """
        cron_job = CronType(second=44)
        expected_defaults = dict(
            day_of_week=None, year=None, month=None, day=None, hour=None,
            minute=None, second=44, timezone_string='UTC',
        )
        self.assertDictEqual(cron_job._generate_field_defaults(), expected_defaults)

    def test_generate_field_defaults_when_timezone_string_is_set(self):
        """
        Should return a dictionary with
        timezone_string set to that timezone_string
        """
        cron_job = CronType(timezone_string=self.timezone_name)
        expected_defaults = dict(
            day_of_week=None, year=None, month=None, day=None, hour=None,
            minute=None, second=None, timezone_string=self.timezone_name,
        )
        self.assertDictEqual(cron_job._generate_field_defaults(), expected_defaults)

    def test_generate_field_defaults_when_all_fields_are_set(self):
        """
        Should return a dictionary with all fields appropriately set
        """
        cron_job = CronType(day_of_week=WeekDay.MONDAY, year=1232, month=Month.SEPTEMBER, day=14,
                            hour=Hour.NINE_AM, minute=23, second=10,
                            timezone_string=self.timezone_name)
        expected_defaults = dict(day_of_week=WeekDay.MONDAY, year=1232, month=Month.SEPTEMBER, day=14,
                                 hour=Hour.NINE_AM, minute=23, second=10,
                                 timezone_string=self.timezone_name)
        self.assertDictEqual(cron_job._generate_field_defaults(), expected_defaults)

    def test_get_time(self):
        """
        Should return the time part of the CronType, got from hour, minute, second
        """
        hour = Hour.EIGHT_PM
        minute = 34
        second = 23

        cron_job = CronType(hour=hour, minute=minute, second=second, day_of_week=WeekDay.MONDAY)
        expected_time = time(hour=hour.value, minute=minute, second=second)

        self.assertEqual(expected_time, cron_job._get_time())

    def test_get_date_no_day_of_week(self):
        """
        Should return the date part of the CronType, got from day, month, year
        """
        year = 1998
        month = Month.AUGUST
        day = 22

        cron_job = CronType(year=year, month=month, day=day, hour=Hour.NINE_AM, minute=4)
        expected_date = date(day=day, month=month, year=year)

        self.assertEqual(expected_date, cron_job._get_date())

    @patch('judah.utils.cron.CronType._get_next_date_from_day_of_week')
    def test_get_date_with_day_of_week(self, mock_get_next_date_from_day_of_week):
        """
        Should return the date part of the CronType, got from day_of_week
        """
        expected_date = date(day=3, month=5, year=1994)
        mock_get_next_date_from_day_of_week.return_value = expected_date
        cron_job = CronType(year=1998, day_of_week=WeekDay.SATURDAY)

        self.assertEqual(expected_date, cron_job._get_date())

    def test_get_next_date_from_day_of_week(self):
        """
        Should get the date of the next day_of_week e.g. next Sunday if day_of_week = WeekDay.SUNDAY
        """
        day_of_week = WeekDay.WEDNESDAY
        cron_job = CronType(year=1783, day_of_week=day_of_week)

        today = date.today()
        current_day_of_week = today.weekday()

        if current_day_of_week > day_of_week.value:
            number_of_days_from_today = 7 + day_of_week.value - current_day_of_week
        else:
            number_of_days_from_today = day_of_week.value - current_day_of_week

        expected_date = today + timedelta(days=number_of_days_from_today)
        self.assertEqual(cron_job._get_next_date_from_day_of_week(), expected_date)

    def test_get_next_utc_timestamp_with_day_of_week(self):
        """
        Should return the timestamp for the next day_of_week e.g. the next Sunday
        """
        day_of_week = WeekDay.SUNDAY
        cron_job = CronType(day_of_week=day_of_week)

        today = datetime.now(tz=pytz.utc).astimezone(pytz.UTC)
        current_day_of_week = today.weekday()

        if current_day_of_week >= day_of_week.value:
            number_of_days_from_today = 7 + day_of_week.value - current_day_of_week
        else:
            number_of_days_from_today = day_of_week.value - current_day_of_week

        expected_date = today + timedelta(days=number_of_days_from_today)
        expected_utc_timestamp = datetime(
            year=expected_date.year, month=expected_date.month, day=expected_date.day,
            hour=0, minute=0, second=0, microsecond=0, tzinfo=pytz.utc
        ).astimezone(pytz.UTC)

        next_timestamp = cron_job.get_next_utc_timestamp()

        self.assertEqual(next_timestamp, expected_utc_timestamp)

    def test_get_next_utc_timestamp_when_only_year_is_set(self):
        """
        Should return the timestamp for 1st January of that year at midnight
        if the timestamp is later than now, else returns None
        """
        today = datetime.utcnow()
        year_in_the_past = 1887
        year_in_future = today.year + 3
        cron_job_in_past_year = CronType(year=year_in_the_past)
        cron_job_in_the_future = CronType(year=year_in_future)

        expected_future_utc_timestamp = datetime(
            year=year_in_future, month=1, day=1,
            hour=0, minute=0, second=0, microsecond=0, tzinfo=pytz.utc
        ).astimezone(pytz.UTC)
        self.assertEqual(cron_job_in_the_future.get_next_utc_timestamp(), expected_future_utc_timestamp)
        self.assertIsNone(cron_job_in_past_year.get_next_utc_timestamp())

    @freeze_time('1997-04-03 11:46:00')
    def test_get_next_utc_timestamp_when_only_month_is_set(self):
        """
        Should return the timestamp for 1st that month of that year at midnight
        if the timestamp is later than now, else returns 1st Month of the next year
        """
        mock_now = datetime.utcnow()
        current_year = mock_now.year
        month_in_the_past = mock_now.month - 2
        month_in_future = mock_now.month + 3
        cron_job_for_past_month = CronType(month=Month(month_in_the_past))
        cron_job_for_future_month = CronType(month=Month(month_in_future))

        expected_utc_timestamp_for_month_in_past = datetime(
            year=current_year + 1, month=month_in_the_past, day=1,
            hour=0, minute=0, second=0, microsecond=0, tzinfo=pytz.utc
        ).astimezone(pytz.UTC)
        expected_utc_timestamp_for_month_in_future = datetime(
            year=current_year, month=month_in_future, day=1,
            hour=0, minute=0, second=0, microsecond=0, tzinfo=pytz.utc
        ).astimezone(pytz.UTC)

        self.assertEqual(
            cron_job_for_past_month.get_next_utc_timestamp(), expected_utc_timestamp_for_month_in_past)
        self.assertEqual(
            cron_job_for_future_month.get_next_utc_timestamp(), expected_utc_timestamp_for_month_in_future)

    @freeze_time('1997-04-13 11:46:00')
    def test_get_next_utc_timestamp_when_only_day_is_set(self):
        """
        Should return the timestamp for that day that month of that year at midnight
        if the timestamp is later than now, else return that day next month
        """
        mock_now = datetime.utcnow()
        current_year = mock_now.year
        current_month = mock_now.month
        day_in_the_past = mock_now.day - 2
        day_in_future = mock_now.day + 3
        cron_job_for_day_in_past = CronType(day=day_in_the_past)
        cron_job_for_day_in_future = CronType(day=day_in_future)

        expected_utc_timestamp_for_day_in_past = datetime(
            year=current_year, month=current_month + 1, day=day_in_the_past,
            hour=0, minute=0, second=0, microsecond=0, tzinfo=pytz.utc
        ).astimezone(pytz.UTC)
        expected_utc_timestamp_for_day_in_future = datetime(
            year=current_year, month=current_month, day=day_in_future,
            hour=0, minute=0, second=0, microsecond=0, tzinfo=pytz.utc
        ).astimezone(pytz.UTC)

        self.assertEqual(
            cron_job_for_day_in_past.get_next_utc_timestamp(), expected_utc_timestamp_for_day_in_past)
        self.assertEqual(
            cron_job_for_day_in_future.get_next_utc_timestamp(), expected_utc_timestamp_for_day_in_future)

    @freeze_time('1997-04-13 08:46:00+00:00')
    def test_get_next_utc_timestamp_when_only_hour_is_set(self):
        """
        Should return the timestamp for that hour of that day that month of that year at minute 0, second 0
        if the timestamp is later than now, else return that hour next day
        """
        mock_now = datetime.now(tz=pytz.UTC)
        current_year = mock_now.year
        current_month = mock_now.month
        current_day = mock_now.day

        hour_in_the_past = mock_now.hour - 2
        hour_in_future = mock_now.hour + 3
        cron_job_for_hour_in_past = CronType(hour=Hour(hour_in_the_past))
        cron_job_for_hour_in_future = CronType(hour=Hour(hour_in_future))

        expected_utc_timestamp_for_hour_in_past = datetime(
            year=current_year, month=current_month, day=current_day + 1,
            hour=hour_in_the_past, minute=0, second=0, microsecond=0, tzinfo=pytz.utc
        ).astimezone(pytz.UTC)
        expected_utc_timestamp_for_hour_in_future = datetime(
            year=current_year, month=current_month, day=current_day,
            hour=hour_in_future, minute=0, second=0, microsecond=0, tzinfo=pytz.utc
        ).astimezone(pytz.UTC)

        self.assertEqual(
            cron_job_for_hour_in_past.get_next_utc_timestamp(), expected_utc_timestamp_for_hour_in_past)
        self.assertEqual(
            cron_job_for_hour_in_future.get_next_utc_timestamp(), expected_utc_timestamp_for_hour_in_future)

    @freeze_time('1997-04-13 08:46:00+00:00')
    def test_get_next_utc_timestamp_when_only_minute_is_set(self):
        """
        Should return the timestamp for that minute of that hour of that day that month of that year at second 0
        if the timestamp is later than now, else return that minute next hour
        """
        mock_now = datetime.now(tz=pytz.UTC)
        current_year = mock_now.year
        current_month = mock_now.month
        current_day = mock_now.day
        current_hour = mock_now.hour

        minute_in_the_past = mock_now.minute - 20
        minute_in_future = mock_now.minute + 10
        cron_job_for_minute_in_past = CronType(minute=minute_in_the_past)
        cron_job_for_minute_in_future = CronType(minute=minute_in_future)

        expected_utc_timestamp_for_minute_in_past = datetime(
            year=current_year, month=current_month, day=current_day,
            hour=current_hour + 1, minute=minute_in_the_past, second=0, microsecond=0, tzinfo=pytz.utc
        ).astimezone(pytz.UTC)
        expected_utc_timestamp_for_minute_in_future = datetime(
            year=current_year, month=current_month, day=current_day,
            hour=current_hour, minute=minute_in_future, second=0, microsecond=0, tzinfo=pytz.utc
        ).astimezone(pytz.UTC)

        self.assertEqual(
            cron_job_for_minute_in_past.get_next_utc_timestamp(), expected_utc_timestamp_for_minute_in_past)
        self.assertEqual(
            cron_job_for_minute_in_future.get_next_utc_timestamp(), expected_utc_timestamp_for_minute_in_future)

    @freeze_time('1997-04-13 08:46:30+00:00')
    def test_get_next_utc_timestamp_when_only_second_is_set(self):
        """
        Should return the timestamp for that second of that minute of that hour of that day that month of that year
        if the timestamp is later than now, else return that second the next minute
        """
        mock_now = datetime.now(tz=pytz.UTC)
        current_year = mock_now.year
        current_month = mock_now.month
        current_day = mock_now.day
        current_hour = mock_now.hour
        current_minute = mock_now.minute

        second_in_the_past = mock_now.second - 20
        second_in_future = mock_now.second + 10
        cron_job_for_second_in_past = CronType(second=second_in_the_past)
        cron_job_for_second_in_future = CronType(second=second_in_future)

        expected_utc_timestamp_for_second_in_past = datetime(
            year=current_year, month=current_month, day=current_day,
            hour=current_hour, minute=current_minute + 1, second=second_in_the_past, microsecond=0, tzinfo=pytz.utc
        ).astimezone(pytz.UTC)
        expected_utc_timestamp_for_second_in_future = datetime(
            year=current_year, month=current_month, day=current_day,
            hour=current_hour, minute=current_minute, second=second_in_future, microsecond=0, tzinfo=pytz.utc
        ).astimezone(pytz.UTC)

        self.assertEqual(
            cron_job_for_second_in_past.get_next_utc_timestamp(), expected_utc_timestamp_for_second_in_past)
        self.assertEqual(
            cron_job_for_second_in_future.get_next_utc_timestamp(), expected_utc_timestamp_for_second_in_future)


if __name__ == '__main__':
    main()
