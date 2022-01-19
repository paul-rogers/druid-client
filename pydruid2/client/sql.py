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

class ColumnSchema:

    def __init__(self, name, sql_type, druid_type):
        self.name = name
        self.sql_type = sql_type
        self.druid_type = druid_type

    def __str__(self):
        return "\{name={}, SQL type={}, Druid type={}}".format(self.name, self.sql_type, self.druid_type)

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

    def run(self):
        return self.client.sql(self)

class SqlResponse:

    def __init__(self, request, response):
        self.request = request
        self.http_response = response
        self._json = None
        self._rows = None
        if not self.ok():
            return
 
    def format(self):
        fmt = self.request.result_format
        if fmt is None:
            return consts.SQL_OBJECT
        else:
            return fmt.lower()

    def ok(self):
        return self.http_response.status_code == requests.codes.ok
    
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
                return None
            fmt = self.format()
            if fmt == consts.SQL_ARRAY_WITH_TRAILER:
                self._rows = json['results']
            elif fmt == consts.SQL_OBJECT or fmt == consts.SQL_ARRAY:
                self._rows = json
        return self._rows

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
