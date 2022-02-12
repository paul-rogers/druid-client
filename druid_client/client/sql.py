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

import requests
import json
from . import consts
from .util import filter_null_cols
from .text_table import TextTable

class ColumnSchema:

    def __init__(self, name, sql_type, druid_type):
        self.name = name
        self.sql_type = sql_type
        self.druid_type = druid_type

    def __str__(self):
        return "{{name={}, SQL type={}, Druid type={}}}".format(self.name, self.sql_type, self.druid_type)

class SqlRequest:

    def __init__(self, client, sql):
        self.client = client
        self.sql = sql
        self.context = None
        self.params = None
        self.header = False
        self.result_format = None
        self.headers = None
        self.types = None
        self.sqlTypes = None
    
    def with_format(self, format):
        self.format = format
        return self
    
    def with_headers(self, sqlTypes=False, druidTypes=False):
        self.headers = True
        self.types = druidTypes
        self.sqlTypes = sqlTypes
        return self
    
    def with_context(self, context):
        if self.context is None:
            self.context = context
        else:
            self.context.update(context)
        return self

    def with_format(self, format):
        self.format = format
        return self

    def response_header(self):
        self.header = True

    def request_headers(self, headers):
        self.headers = headers
    
    def to_request(self):
        query_obj = {"query": self.sql}
        if self.context is not None and len(self.context) > 0:
            query_obj['context'] = self.context
        if self.params is not None and len(self.params) > 0:
            query_obj['parameters'] = self.params
        if self.header:
            query_obj['header'] = True
        if self.result_format is not None:
            query_obj['resultFormat'] = self.result_format
        if self.sqlTypes:
            query_obj['sqlTypesHeader'] = self.sqlTypes
        if self.types:
            query_obj['typesHeader'] = self.types
        return query_obj

    def format(self):
        if self.result_format is None:
            return consts.SQL_OBJECT
        return self.result_format.lower()

    def run(self):
        return self.client.sql_query(self)

def parse_object_schema(results):
    schema = []
    if len(results) == 0:
        return schema
    row = results[0]
    for k, v in row.items():
        druid_type = None
        sql_type = None
        if type(v) is str:
            druid_type = consts.DRUID_STRING_TYPE
            sql_type = consts.SQL_VARCHAR_TYPE
        elif type(v) is int or type(v) is float:
            druid_type = consts.DRUID_LONG_TYPE
            sql_type = consts.SQL_BIGINT_TYPE
        schema.append(ColumnSchema(k, sql_type, druid_type))
    return schema

def parse_array_schema(context, results):
    schema = []
    if len(results) == 0:
        return schema
    has_headers = context.get(consts.HEADERS_KEY, False)
    if not has_headers:
        return schema
    has_sql_types = context.get(consts.SQL_TYPES_HEADERS_KEY, False)
    has_druid_types = context.get(consts.DRUID_TYPE_HEADERS_KEY, False)
    size = len(results[0])
    for i in range(size):
        druid_type = None
        if has_druid_types:
            druid_type = results[1][i]
        sql_type = None
        if has_sql_types:
            sql_type = results[2][i]
        schema.append(ColumnSchema(results[0][i], sql_type, druid_type))
    return schema

def parse_schema(fmt, context, results):
    if fmt == consts.SQL_OBJECT:
        return parse_object_schema(results)
    elif fmt == consts.SQL_ARRAY or fmt == consts.consts.SQL_ARRAY_WITH_TRAILER:
        return parse_array_schema(context, results)
    else:
        return []

def parse_rows(fmt, context, results):
    if fmt == consts.SQL_ARRAY_WITH_TRAILER:
        rows = results['results']
    elif fmt == consts.SQL_ARRAY:
        rows = results
    else:
        return results
    if not context.get(consts.HEADERS_KEY, False):
        return rows
    header_size = 1
    if context.get(consts.SQL_TYPES_HEADERS_KEY, False):
        header_size += 1
    if context.get(consts.DRUID_TYPE_HEADERS_KEY, False):
        header_size += 1
    return rows[header_size:]

class AbstractSqlQueryResult:
    """
    Defines the core protocol for Druid SQL queries.
    """

    def __init__(self, request, response):
        self.request = request
        self.http_response = response
        code = response.status_code
        if self.is_response_ok():
            self._error = None
            return
        try:
            self._error = response.json()
        except Exception:
            self._error = response.text
            if self._error is None or len(self.error == 0):
                self._error = "Failed with HTTP status {}".format(code)

    def format(self):
        return self.request.format()

    def ok(self):
        """
        Reports if the query succeeded.

        The query rows and schema are available only if ok() returns True.
        """
        return self.is_response_ok()
    
    def is_response_ok(self):
        code = self.http_response.status_code
        return code == requests.codes.ok or code == requests.codes.accepted
 
    def error(self):
        """
        If the query fails, returns the error, if any provided by Druid.
        """
        return self._error

    def id(self):
        """
        Returns the unique identifier for the query.
        """
        raise NotImplementedError

    def rows(self):
        """
        Returns the rows of data for the query.

        Druid supports many data formats. The method makes its best
        attempt to map the format into an array of rows of some sort.
        """
        raise NotImplementedError

    def schema(self):
        """
        Returns the data schema as a list of ColumnSchema objects.

        Druid supports many data formats, not all of them provide
        schema information. This method makes its best attempt to
        extract the schema from the query results.
        """
        raise NotImplementedError

    def display(self, non_null=False):
        raise NotImplementedError

class SqlQueryResult(AbstractSqlQueryResult):

    def __init__(self, request, response):
        AbstractSqlQueryResult.__init__(self, request, response)
        self._json = None
        self._rows = None
        self._schema = None

    def error(self):
        if self.ok():
            return None
        try:
            json = self.json()
            if json is None:
                return "unknown"
            return json['error']
        except KeyError:
            return None

    def id(self):
        if self.http_response is None:
            return None
        try:
            return self.http_response.headers['X-Druid-SQL-Query-Id']
        except KeyError:
            return None
    
    def json(self):
        if not self.ok():
            return None
        if self._json is None:
            self._json = self.http_response.json()
        return self._json
    
    def rows(self):
        if self._rows is None:
            json = self.json()
            if json is None:
                return self.response.text
            self._rows = parse_rows(self.format(), self.request.context, json)
        return self._rows

    def schema(self):
        if self._schema is None:
            self._schema = parse_schema(self.format(), self.request.context, self.json())
        return self._schema

    def profile(self):
        """
        Experimental feature to return the query profile.

        Not yet in the Druid master branch.
        """
        json = self.json()
        if json is None:
            return None
        fmt = self.format()
        if fmt != consts.SQL_ARRAY_WITH_TRAILER:
            return None
        try:
            return json['context']['profile']
        except KeyError:
            return None
    
    def df(self):
        """
        Convert the query result to a Pandas data frame.

        Requires that Pandas be installed.
        
        Ensure the query returns a limited number of rows as all data
        is held in memory, where "limited" depends on your needs and memory.

        The SQL type should be "object" (consts.SQL_OBJECT) as the
        "array" type won't return the column headers.
        """
        if not self.ok():
            return None
        import pandas as pd
        fmt = self.format()
        if fmt == consts.SQL_ARRAY or fmt == consts.SQL_OBJECT:
            return pd.read_json(self.request.text)
        elif self.request.result_format == consts.ARRAY_WITH_TRAILER:
            return pd.DataFrame(self.rows())
        else:
            return None

    def non_null(self):
        if not self.ok():
            return None
        if self.format() != consts.SQL_OBJECT:
            return None
        return filter_null_cols(self.rows())

    def as_array(self):
        if self.format() == consts.SQL_OBJECT:
            rows = []
            for obj in self.rows():
                rows.append([v for v in obj.values()])
            return rows
        else:
            return self.rows()

    def show(self, non_null=False):
        data = None
        if non_null:
            data = self.non_null()
        if data is None:
            data = self.as_array()
        disp = self.request.client._display().table()
        disp.headers([c.name for c in self.schema()])
        disp.show(data)

    def show_schema(self):
        disp = self.request.client._display().table()
        disp.headers(['Name', 'SQL Type', 'Druid Type'])
        data = []
        for c in self.schema():
            data.append([c.name, c.sql_type, c.druid_type])
        disp.show(data)

PLAN_MARKER = 'DruidQueryRel(query=['
SIG_MARKER = '], signature=[{'
TAIL_MARKER = '}])'

class QueryPlan:
    """
    Description of a Druid query plan.

    For regular plans, parses the representation into its component parts.

    Plans for system tables provide limited information.
    """

    def __init__(self, row):
        self._results = row
        # PLAN = 'DruidQueryRel(query=[<json>], signature=[...'
        self._plan_text = row['PLAN']
        self._columns = []
        self._types = []
        sig_posn = self._plan_text.find(SIG_MARKER)
        if (sig_posn == -1):
            # Not a full Calcite plan
            # TODO: Parse table out of system table plan
            # 'BindableTableScan(table=[[sys, servers]])\n'
            self._plan_json = None
            self._tables = None
            return
        json_text = self._plan_text[len(PLAN_MARKER):sig_posn]
        self._plan_json = json.loads(json_text)
        cols = self._plan_text[sig_posn + len(SIG_MARKER):-len(TAIL_MARKER) - 1].split(", ")
        for col in cols:
            parts = col.split(":")
            self._columns.append(parts[0])
            self._types.append(parts[1])
        self._tables = json.loads(row['RESOURCES'])
    
    def row(self):
        return self._results
    
    def plan(self):
        return self._plan_text

    def plan_details(self):
        return self._plan_json
    
    def columns(self):
        return self._columns
    
    def types(self):
        return self._types
    
    def tables(self):
        return self._tables
    
    def __str__(self):
        return self.plan()
