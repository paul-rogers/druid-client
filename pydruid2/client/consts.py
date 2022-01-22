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

from datetime import timedelta

# Protocols
PLAIN_TEXT_PROTOCOL = 'http'
TLS_PROTOCOL = 'https'

# Service (role) names as reported by Druid
ROUTER = 'router'
BROKER = 'broker'
COORDINATOR = 'coordinator'
OVERLORD = 'overlord'
HISTORICAL ='historical'
INDEXER = 'indexer'
MIDDLE_MANAGER = 'middle_manager'
PEON = 'peon'
ROLES = [ROUTER, BROKER, COORDINATOR, OVERLORD, HISTORICAL, INDEXER, MIDDLE_MANAGER, PEON]

# System schemas and table names. Note: case must match in Druid, though
# SQL itself is supposed to be case-insensitive.
SYS_SCHEMA = 'sys'
SCHEMA_SCHEMA = 'INFORMATION_SCHEMA'
DRUID_SCHEMA = 'druid'

SERVERS_TABLE = SYS_SCHEMA + '.servers'
SEGMENTS_TABLE = SYS_SCHEMA + '.segments'
SUPERVISORS_TABLE = SYS_SCHEMA + '.supervisors'
SERVER_SEGMENTS_TABLE = SYS_SCHEMA + ".server_segments"
TASKS_TABLE = SYS_SCHEMA + '.tasks'

SCHEMAS_TABLE = SCHEMA_SCHEMA + '.SCHEMATA'
TABLES_TABLE = SCHEMA_SCHEMA + '.TABLES'
COLUMNS_TABLE = SCHEMA_SCHEMA + '.COLUMNS'

# Task states
PENDING_TASK = 'pending'
COMPLETE_TASK = 'complete'
RUNNING_TASK = 'running'
WAITING_TASK = 'waiting'

# Default tier name
DEFAULT_TIER = "_default_tier"

# SQL request formats
SQL_OBJECT = 'object'
SQL_ARRAY = 'array'
SQL_ARRAY_WITH_TRAILER = 'arrayWithTrailer'
SQL_CSV = 'csv'

# SQL and Native context fields
HEADERS_KEY = 'headers'
SQL_TYPE_HEADERS_KEY = 'sqlTypesHeader'
DRUID_TYPE_HEADERS_KEY = 'typesHeader'

# Type names as known to Druid and mentioned in documentation.
DRUID_STRING_TYPE = "string"
DRUID_LONG_TYPE = "long"
DRUID_FLOAT_TYPE = "float"
DRUID_DOUBLE_TYPE = "double"
DRUID_TIMESTAMP_TYPE = "timestamp"

# SQL type names as returned from the INFORMATION_SCHEMA
SQL_VARCHAR_TYPE = "VARCHAR"
SQL_BIGINT_TYPE = "BIGINT"
SQL_FLOAT_TYPE = "FLOAT"
SQL_DOUBLE_TYPE = "DOUBLE"
SQL_TIMESTAMP_TYPE = "TIMESTAMP"

druid_to_sql = {
    DRUID_STRING_TYPE: SQL_VARCHAR_TYPE,
    DRUID_LONG_TYPE: SQL_BIGINT_TYPE,
    DRUID_FLOAT_TYPE: SQL_FLOAT_TYPE,
    DRUID_DOUBLE_TYPE: SQL_DOUBLE_TYPE,
    DRUID_TIMESTAMP_TYPE: SQL_TIMESTAMP_TYPE,
    }

sql_to_druid = {
    SQL_VARCHAR_TYPE: DRUID_STRING_TYPE,
    SQL_BIGINT_TYPE: DRUID_LONG_TYPE,
    SQL_FLOAT_TYPE: DRUID_FLOAT_TYPE,
    SQL_DOUBLE_TYPE: DRUID_FLOAT_TYPE,
    SQL_TIMESTAMP_TYPE: DRUID_TIMESTAMP_TYPE,
}

# Predefined time column
TIME_COL = "__time"

# Ingest metric types
COUNT_METRIC = 'count'
DOUBLE_SUM_METRIC = 'doubleSum'
DOUBLE_MIN_METRIC = 'doubleMin'
DOUBLE_MAX_METRIC = 'doubleMax'

# Druid granularities
# See https://druid.apache.org/docs/latest/querying/granularities.html
NO_GRAIN = 'none'
ALL_GRAIN = 'all'
SECOND_GRAIN = 'second'
MINUTE_GRAIN = 'minute'
MINUTE_15_GRAIN = 'fifteen_minute'
MINUTE_30_GRAIN = 'thirty_minute'
HOUR_GRAIN = 'hour'
DAY_GRAIN = 'day'
WEEK_GRAIN = 'week'
MONTH_GRAIN = 'month'
QUARTER_GRAIN = 'quarter'
YEAR_GRAIN = 'year'

druid_grains = {
    SECOND_GRAIN: timedelta(seconds=1),
    MINUTE_GRAIN: timedelta(minutes=1),
    MINUTE_15_GRAIN: timedelta(minutes=15),
    MINUTE_30_GRAIN: timedelta(minutes=30),
    HOUR_GRAIN: timedelta(hours=1),
    DAY_GRAIN: timedelta(days=1),
    WEEK_GRAIN: timedelta(weeks=1),
    # timedelta is not a Period, it can't represent odd units such
    # as "one month". So, we approximate.
    MONTH_GRAIN: timedelta(days=30),
    QUARTER_GRAIN: timedelta(days=90),
    YEAR_GRAIN: timedelta(days=365),
}

# Misc. handy constants

ONE_KB = 1024
ONE_MB = ONE_KB * 1024
ONE_GB = ONE_MB * 1024
ONE_WEEK = 6
ONE_MONTH = 30
ONE_YEAR = 365

SECS_PER_MIN = 60
SECS_PER_HOUR = SECS_PER_MIN * 60
SECS_PER_DAY = SECS_PER_HOUR * 24