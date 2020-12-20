import os
from datetime import date, timedelta, datetime
from typing import Optional, Tuple, Union

import pytz


def convert_date_to_string(
        date_value: date, date_format: str = '%Y-%m-%dT%T%ZZ', timezone_separator: Optional[str] = None,
        timezone_name: Optional[str] = None) -> str:
    """Converts a date to the expected string"""
    timezone = pytz.utc if timezone_name is None else pytz.timezone(timezone_name)

    datetime_value = datetime(
        year=date_value.year, month=date_value.month, day=date_value.day, tzinfo=pytz.utc).astimezone(
        timezone)
    # remove any time shift due to timezones
    datetime_value = datetime_value - datetime_value.utcoffset()
    formatted_datetime_string = datetime_value.strftime(date_format)

    if isinstance(timezone_separator, str):
        offset_hours = formatted_datetime_string[:-2]
        offset_minutes = formatted_datetime_string[-2:]

        formatted_datetime_string = f'{offset_hours}{timezone_separator}{offset_minutes}'

    return formatted_datetime_string


def get_default_start_year() -> Optional[int]:
    """Returns the default year"""
    start_year = os.getenv('HISTORICAL_STARTING_YEAR', None)

    if start_year is None:
        return None

    return int(start_year)


def get_default_start_month() -> Optional[int]:
    """Returns the default start month"""
    start_month = os.getenv('HISTORICAL_STARTING_MONTH', None)

    if start_month is None:
        return None

    return int(start_month)


def get_quarter_from_month(month: int):
    """Returns the quarter for a given month"""
    month_quarter_map = {1: 1, 2: 1, 3: 1, 4: 2, 5: 2, 6: 2, 7: 3, 8: 3, 9: 3, 10: 4, 11: 4, 12: 4}
    return month_quarter_map[month]


def get_default_start_quarter() -> Optional[int]:
    """Returns the default start quarter"""
    start_month = get_default_start_month()

    if start_month is None:
        return None

    return get_quarter_from_month(month=start_month)


def get_current_year() -> int:
    """Get's the current year basing on the system time"""
    return datetime.now().year


def get_current_quarter() -> int:
    """Get's the current quarter basing on the system time"""
    return get_quarter_from_month(month=datetime.now().month)


def get_default_historical_start_date() -> Optional[date]:
    """Extracts the default start_date from the environment"""
    start_year = get_default_start_year()
    start_month = int(os.getenv('HISTORICAL_STARTING_MONTH', 1))
    start_day = int(os.getenv('HISTORICAL_STARTING_DAY', 1))

    if start_year is None:
        return None

    return date(year=start_year, month=start_month, day=start_day)


def get_default_historical_start_quarter_and_year() -> Optional[Tuple[int, int]]:
    """Extracts the default start_date from the environment"""
    start_year = get_default_start_year()
    start_quarter = get_default_start_quarter()

    if start_year is None:
        return None

    if start_quarter is None:
        start_quarter = 1

    return start_quarter, start_year


def update_date(date_to_update: date, days_to_increment_by: int, days_to_decrement_by: int):
    """Updates a given date by incrementing and/or decrementing it"""
    return date_to_update + timedelta(days=days_to_increment_by) - timedelta(days=days_to_decrement_by)


def update_quarter_year_tuple(
        quarter_year_tuple: Tuple[int, int],
        quarters_to_increment_by: int,
        quarters_to_decrement_by: int) -> Tuple[int, int]:
    """Updates a given quarter_year_tuple by incrementing and/or decrementing it"""
    quarter, year = quarter_year_tuple
    net_quarters_delta = quarters_to_increment_by - quarters_to_decrement_by
    number_of_quarters_per_year = 4

    # convert (quarter, year) tuple to quarters since year 1, quarter 1
    total_quarters_since_year_1_quarter_1 = (quarter - 1) + ((year - 1) * number_of_quarters_per_year)
    new_total_quarters_since_year_1_quarter_1 = total_quarters_since_year_1_quarter_1 + net_quarters_delta

    years_after_year_1 = int(new_total_quarters_since_year_1_quarter_1 / number_of_quarters_per_year)
    updated_year = 1 + years_after_year_1

    quarters_after_quarter_1 = new_total_quarters_since_year_1_quarter_1 % number_of_quarters_per_year
    updated_quarter = 1 + quarters_after_quarter_1

    return updated_quarter, updated_year


def convert_date_to_quarter_year_tuple(date_value: date) -> Tuple[int, int]:
    """Converts a given date to a quarter_year tuple"""
    year = date_value.year
    month = date_value.month
    quarter = get_quarter_from_month(month=month)
    return quarter, year


def change_datetime_format(datetime_value: str, old: str, new: str):
    """Converts a date string's format to another format"""
    return datetime.strptime(datetime_value, old).strftime(new)


def add_month_to_datetime_but_maintain_day(datetime_value: Union[datetime, date]) -> Optional[Union[date, datetime]]:
    """Adds a month to the given date or datetime, maintaining the day itself"""
    try:
        return datetime_value.replace(month=datetime_value.month + 1)
    except ValueError:
        if datetime_value.month == 12:
            return datetime_value.replace(year=datetime_value.year + 1, month=1)
    return None
