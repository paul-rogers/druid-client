# Copyright 2022 Paul Rogers
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from urllib.parse import urlparse
from datetime import datetime, timedelta
from . import consts

#-------- Misc. --------

def is_blank(s):
    """
    Returns True if the given string is None or blank (after stripping spaces),
    False otherwise.
    """
    return s is None or len(s.strip()) == 0

def nullify(s):
    if s is None:
        return None
    s = s.strip()
    if len(s) == 0:
        return None
    return s

def dict_get(dict, key, default=None):
    """
    Returns the value of key in the given dict, or the default value if
    the key is not found. 
    """
    if dict is None:
        return default
    return dict.get(key, default)

#-------- Network --------

def endpoint(host, port):
    """
    Return the "host:port" string for the given host and port.
    """
    return host + ":" + str(port)

def service_url(protocol, host, port):
    """
    Return the service endpoint URL given the protocol, host and port.
    """
    return protocol + "://" + endpoint(host, port)

def split_host_url(url):
    """
    Split a service endpoint into a triple of (protocol, host, port).
    """
    parsed = urlparse(url)
    return (parsed.scheme.lower(), parsed.hostname, parsed.port)

#-------- SQL --------

def quote_col(col):
    return '"{}"'.format(col)

def sql_equality(s):
    if s is None:
        return 'IS NULL'
    s = s.replace("'", "''")
    return "= '" + s + "'"

def datetime_to_sql(dt):
    return dt.isoformat().replace('T', ' ')

def druid_ts_to_sql(ts):
    '''
    Convert a Druid timestamp falue to a SQL TIMESTAMP value.
    '''
    return ts[:-1].replace('T', ' ')

def label_non_null_cols(results):
    if results is None or len(results) == 0:
        return []
    is_null = {}
    for key in results[0].keys():
        is_null[key] = True
    for row in results:
        for key, value in row.items():
            if type(value) == str:
                if value != '':
                    is_null[key] = False
            elif type(value) == float:
                if value != 0.0:
                    is_null[key] = False
            elif value is not None:
                is_null[key] = False
    return is_null

def filter_null_cols(results):
    '''
    Filter columns from a Druid result set by removing all null-like
    columns. A column is considered null if all values for that column
    are null. A value is null if it is either a JSON null, an empty
    string, or a numeric 0. All rows are preserved, as is the order
    of the remaining columns.
    '''
    if results is None or len(results) == 0:
        return results
    is_null = label_non_null_cols(results)
    revised = []
    for row in results:
        new_row = {}
        for key, value in row.items():
            if is_null[key]:
                continue
            new_row[key] = value
        revised.append(new_row)
    return revised

#-------- Data Types --------

def druid_to_sql_type(druid_type):
    try:
        return  consts.druid_to_sql[druid_type.lower]
    except KeyError:
        return "CUSTOM<" + druid_type + ">"

def sql_to_druid_type(sql_type):
    try:
        return consts.sql_to_druid[sql_type.upper()]
    except KeyError:
        return "UNKNOWN<" + sql_type + ">"

#-------- Time, Intervals --------

def encode_interval(interval_id):
    return interval_id.replace("/", "_")

def to_datetime(druid_ts) -> datetime:
    '''
    Convert a Druid ISO UTC timestamp string to a Python
    datetime.
    '''
    return datetime.fromisoformat(druid_ts[0:-1])

def druid_timestamp(dt) -> str:
    '''
    Convert a Python datetime object to a Druid ISO UTC
    timestamp string.
    '''
    return dt.isoformat() + "Z"

def druid_range(start, end):
    if type(start) == datetime:
        start = druid_timestamp(start)
    if type(end) == datetime:
        end= druid_timestamp(end)
    return start + "/" + end

def delta_to_period(delta) -> timedelta:
    return secs_to_period(delta.total_seconds())

def secs_to_period(secs) -> str:
    period = "P"
    days = secs // consts.SECS_PER_DAY
    if days > 0:
        period += "{}D".format(days)
        secs = int(secs % consts.SECS_PER_DAY)
    if secs == 0:
        return period
    period += "T"
    hours = secs // consts.SECS_PER_HOUR
    if hours > 0:
        period += "{}H".format(hours)
        secs = int(secs % consts.SECS_PER_HOUR)
    mins = secs // consts.SECS_PER_MIN
    if mins > 0:
        period += "{}M".format(mins)
        secs = int(secs % consts.SECS_PER_MIN)
    if secs > 0:
        period += "{}S".format(int(secs))
    return period

def delta_to_period(delta) -> timedelta:
    return secs_to_period(delta.total_seconds())
