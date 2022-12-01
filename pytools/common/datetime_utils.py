"""
https://en.wikipedia.org/wiki/ISO_8601
https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBMapper.DataTypes.html
https://stackoverflow.com/questions/40561484/what-data-type-should-be-use-for-timestamp-in-dynamodb
https://stackoverflow.com/questions/9321809/format-date-in-mysql-select-as-iso-8601
"""

import re
from datetime import datetime, timedelta, tzinfo
from enum import Enum
from typing import Optional, Tuple

import pytz
from dateutil.parser import parser as DateutilParser
from dateutil.relativedelta import relativedelta

from pytools.common.logger import Logger

LOCAL_TIMEZONE_NAME = "America/Los_Angeles"

UNIX_DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"
ISO_DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
S3_DATETIME_FORMAT = "%Y%m%d-%H%M%S.%f"
MYSQL_DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"
YYYYMMDD_DATETIME_FORMAT = "%Y%m%d"
S3_KEY_NAME_FORMAT = "%H%M%S.%f"
mysql_format = "%Y-%m-%d %H:%M:%S"
iso_format = "%Y-%m-%dT%H:%M:%SZ"
google_format = "%Y-%m-%dT%H:%M:%S.%fZ"
timespan_format = "P%YY%mM%dDT%HH:%MM:%SS"
simple_date_format = "%Y-%m-%d"
simple_slash_date_format = "%Y/%m/%d"


class TimeTags(Enum):
    START = "start"
    END = "end"


#######################################
def convert_time_to_google_format(time_str: str) -> str:
    """
    Arguments:
        time_str -- a string in format '2019-03-08T18:49:23' or '2019-03-08T18:49:23Z'

    Returns:
        A string in format '2019-03-08T18:49:23.000Z'
    """
    ts = time_str.replace("Z", "").replace("T", " ")
    return "T".join(re.split(" ", ts)) + ".000Z"


#######################################
def convert_google_format_to_time(time_str: str) -> datetime:
    """
    Arguments:
        time_str -- a string in format '2019-03-08T18:49:23.000Z' or '2019-03-08T18:49:23Z'

    Returns:
        `datetime` object parsed from `time_str`
    """
    try:
        dt_object = datetime.strptime(time_str, google_format)
    except ValueError:
        dt_object = datetime.strptime(time_str, iso_format)

    return dt_object


#######################################
def anchor_timestamp_to_tag(time_str: str, time_tag: Optional[TimeTags] = None) -> str:
    """
    Arguments:
        time_str -- a string in format '2019-03-08T18:49:23' or '2019-03-08T18:49:23Z
        time_tag -- TimeTags obj to set 00:00:00:000 or 23:59:59:999 to the given time_str

    Returns:
        A string in format '%Y-%m-%dT%H:%M:%SZ'
    """
    if time_tag is not None:
        datetime_obj = None
        if TimeTags.START == time_tag:
            datetime_obj = parse(time_str).replace(hour=0, minute=0, second=0, microsecond=0)
        elif TimeTags.END == time_tag:
            datetime_obj = parse(time_str).replace(hour=23, minute=59, second=59, microsecond=0)

        if datetime_obj is not None:
            time_str = datetime_obj.strftime(iso_format)
    return time_str


#######################################
def get_days_difference(time_a: str, time_b: str) -> int:
    """
    Get number of days between `time_a` and `time_b` datetime strings.

    Arguments:
        time_a -- a string in format '2019-03-08T18:49:23.000Z' or '2019-03-08T18:49:23Z'
        time_b -- a string in format '2019-03-08T18:49:23.000Z' or '2019-03-08T18:49:23Z'

    Returns:
        Days between `time_b` and `time_а`
    """
    date_a = convert_google_format_to_time(time_a)
    date_b = convert_google_format_to_time(time_b)
    delta = date_b - date_a
    return delta.days


#######################################
def get_date_n_days_ago(days: int) -> datetime:
    """
    Gets the date from a specified number of days ago

    Example:
    ```python
    last_week = get_date_n_days_ago(7)
    print(last_week..strftime('%Y-%m-%d')))
    ```

    Arguments:
        days -- The number of n days ago to get the date for

    Returns:
        A datetime object specified by the parameter `days`
    """
    return get_current_utc_datetime() - timedelta(days=days)


#######################################
def convert_period_to_time(period: str) -> Tuple[int, int, int, int, int, int]:
    years = int(period.split("Y")[0][1:])
    months = int(period.split("Y")[1].split("M")[0])
    days = int(period.split("Y")[1].split("M")[1].split("D")[0])
    hours = int(period.split("Y")[1].split("M")[1].split("D")[1].split("H")[0][1:])
    minutes = int(period.split("Y")[1].split("M")[1].split("D")[1].split("H")[1].split("M")[0])
    seconds = int(period.split("Y")[1].split("M")[2][:-1])

    return years, months, days, hours, minutes, seconds


#######################################
def subtract_times(time_str: str, time_span: str) -> str:
    """
    https://docs.python.org/3/library/datetime.html
    https://stackoverflow.com/questions/13897246/python-time-subtraction
    https://stackoverflow.com/questions/5259882/subtract-two-times-in-python

    Arguments:
        time_str -- A string in format '2019-03-08T18:49:23.000Z' or '2019-03-08T18:49:23Z'
        time_span -- A string in format 'P0000Y00M08DT01H02M03S'

    Returns:
        A string in format '2019-03-08T18:49:23Z'
    """
    years, months, days, hours, minutes, seconds = convert_period_to_time(time_span)
    new_dt = convert_google_format_to_time(time_str) - timedelta(
        days=days, hours=hours, minutes=minutes, seconds=seconds
    )
    new_dt = convert_google_format_to_time(time_str) - relativedelta(
        years=years, months=months, days=days, hours=hours, minutes=minutes, seconds=seconds
    )

    return datetime.strftime(new_dt, iso_format)


#######################################
def add_times(time_str: str, time_span: str) -> str:
    """
    Add `time_span` to `time_str` and return a new datetime in ISO format.

    Arguments:
        time_str -- A string in format '2019-03-08T18:49:23.000Z' or '2019-03-08T18:49:23Z'
        time_span -- A string in format 'P0000Y00M08DT01H02M03S'

    Returns:
        A string with datetime in ISO format.
    """
    _years, _months, days, hours, minutes, seconds = convert_period_to_time(time_span)
    new_dt = convert_google_format_to_time(time_str) + timedelta(
        days=days, hours=hours, minutes=minutes, seconds=seconds
    )

    return datetime.strftime(new_dt, iso_format)


#######################################
def get_curr_utc_time() -> str:
    """
    Returns the current time in UTC in ISO 8601 format
    output format: (string) '2019-03-08T18:49:23Z'
    https://en.wikipedia.org/wiki/ISO_8601

    Returns:
        A string in format '2019-03-08T18:49:23Z'
    """
    return datetime.utcnow().strftime(iso_format)


#######################################
def format_iso_datetime_for_mysql(dt_string: str) -> str:
    """
    Arguments:
        dt_string -- A string representing a datetime in any parseable format

    Returns:
        A string representing a datetime value in MySQL format, e.g. "'2019-03-08 18:49:23'"
    """
    d = parse(dt_string)
    return d.strftime(MYSQL_DATETIME_FORMAT)


#######################################
def format_box_datetime_for_mysql(dt_string: str) -> str:
    """
    Arguments:
        dt_string -- A string representing a datetime in any parseable format

    Returns:
        A string representing a datetime value in MySQL format, e.g. "'2019-03-08 18:49:23'"
    """
    d = parse(dt_string)
    return d.strftime(MYSQL_DATETIME_FORMAT)


#######################################
def format_datetime_to_simple_date(dt_string: str) -> str:
    """
    Arguments:
        dt_string -- A string representing a datetime value in iso format

    Returns:
        A string representing a date value in E8601DAw format, e.g. '2008-09-15'
    """
    d = parse(dt_string)
    return d.strftime(simple_date_format)


#######################################
# https://stackoverflow.com/questions/6999726/how-can-i-convert-a-datetime-object-to-milliseconds-since-epoch-unix-time-in-p
# https://stackoverflow.com/questions/8777753/converting-datetime-date-to-utc-timestamp-in-python
# https://docs.python.org/3.3/library/datetime.html#datetime.timestamp
# http://pytz.sourceforge.net/
# https://howchoo.com/g/ywi5m2vkodk/working-with-datetime-objects-and-timezones-in-python
# https://stackoverflow.com/questions/18812638/get-timezone-used-by-datetime-datetime-fromtimestamp


def convert_datetime_to_epoch(date_time: datetime) -> float:
    """
    Convert `datetime` object to Unix epoch.

    Arguments:
        date_time - Datetime object

    Returns:
        Unix epoch
    """
    tz = date_time.tzinfo or pytz.UTC
    epoch = datetime.utcfromtimestamp(0).replace(tzinfo=tz)
    return (date_time - epoch).total_seconds()


def convert_epoch_to_time_string(
    epoch: float, ts_format: Optional[str] = None, tzone: Optional[tzinfo] = None
) -> str:
    """
    Arguments:
        epoch -- Unix epoch
        ts_format -- Format string for strftime
        tzone -- Timezone to set

    Returns:
        Formatted datetime string
    """
    tz = tzone or pytz.UTC
    fmt = ts_format or iso_format
    date_time = datetime.utcfromtimestamp(epoch).replace(tzinfo=tz)
    return datetime.strftime(date_time, fmt)


def convert_epoch_to_datetime(epoch: float, tzone: Optional[tzinfo] = None) -> datetime:
    """
    Convert Unix epoch to `datetime` object.

    Arguments:
        epoch -- Unix epoch
        tzone -- Timezone to set

    Returns:
        `datetime` object parsed from `time_str`
    """
    tz = tzone or pytz.UTC
    date_time = datetime.utcfromtimestamp(epoch).replace(tzinfo=tz)
    return date_time


#######################################

#######################################


def get_utc_offset(ts: int) -> timedelta:
    """
    Compare two datetime strings and return `True` if the second is greater
    than the first one.

    Arguments:
        ts -- Any opech timestamp (actually, it is not needed, left for campatibility)

    Returns:
        `datetime.timedelta` detween local timezone and UTC.
    """
    return datetime.fromtimestamp(ts) - datetime.utcfromtimestamp(ts)


#######################################


def compare_dt_strings(dt_str_1: str, dt_str_2: str) -> bool:
    """
    Compare two datetime strings and return `True` if the second is greater
    than the first one.

    Arguments:
        dt_str_1 -- A string with datetime in any parseable format
        dt_str_2 -- A string with datetime in any parseable format

    Returns:
        True if `dt_str_2` is later than `dt_str_1`.
    """
    return parse(dt_str_1) < parse(dt_str_2)


def get_curr_local_time() -> datetime:
    """
    Get current time in default (America/Los_Angeles) timezone.

    Returns:
        Current `datetime` for local timezone
    """
    return datetime.now(pytz.timezone(LOCAL_TIMEZONE_NAME))


def get_current_utc_datetime() -> datetime:
    """
    Get current UTC datetime object

    Returns:
        Current UTC `datetime`
    """
    return datetime.utcnow()


def parse(string: str) -> datetime:
    """
    Parse datetime from a string. All popular formats are supported.
    If parsed datetime is naive, sets UTC timezone.

    Examples:

        ```python
        parse('2019-03-08T18:49:23.000Z')
        # datetime.datetime(2019, 3, 8, 18, 49, 23, tzinfo=pytz.UTC)

        parse('03/08/2019 18:49:23')
        # datetime.datetime(2019, 3, 8, 18, 49, 23, tzinfo=pytz.UTC)
        ```

    Returns:
        Timezone-aware `datetime` object.
    """
    dt = DateutilParser().parse(string)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=pytz.UTC)
    return dt


def get_humanized_date(date_time: datetime) -> str:
    """
    Get date in words

    Returns:
        date in words
    """
    return date_time.strftime("%b %d %Y")


#######################################


def main() -> None:
    """
    Usage examples for all functions in this module
    """
    logger = Logger(__name__, level=Logger.DEBUG)
    tzone = pytz.timezone("America/Los_Angeles")
    date_time = datetime.strptime("2019-04-20T00:00:00Z", iso_format).replace(tzinfo=tzone)
    date_time_epoch = convert_datetime_to_epoch(date_time)
    convert_epoch_to_time_string(date_time_epoch, iso_format, tzone)
    convert_epoch_to_datetime(date_time_epoch, tzone)
    logger.debug(subtract_times(get_curr_utc_time(), "P0000Y00M85DT00H00M00S"))

    logger.debug(format_datetime_to_simple_date("2019-08-17"))
    logger.debug(format_datetime_to_simple_date("20190817"))
    logger.debug(format_datetime_to_simple_date("2019-08-17 12:00:00"))
    logger.debug(format_datetime_to_simple_date("2019-8-17 12:00:00"))
    logger.debug(format_datetime_to_simple_date("2019-8-17 12"))
    logger.debug(format_datetime_to_simple_date("19-8-17 12"))
    logger.debug(format_datetime_to_simple_date("2019-07-25T01:13:03.898+0000"))

    dt1 = parse("2019-07-25T01:13:03.898+0000")
    dt2 = parse("2019-01-01T00:00:00.000+0000")
    logger.debug(f"{dt1 < dt2}")

    dt1 = parse("2019-07-25T01:13:03.898")
    dt2 = parse("2019-01-01T00:00:00.000")
    logger.debug(f"{dt1 < dt2}")

    logger.debug(str(get_date_n_days_ago(2)))

    logger.debug(get_humanized_date(date_time))


if __name__ == "__main__":
    main()
