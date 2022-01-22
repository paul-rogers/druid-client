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

import time
from pydruid2.client.error import DruidError
from pydruid2.client import consts as base_consts
from pydruid2.client.sql import SqlRequest, ColumnSchema, parse_schema, parse_rows
from . import consts

def async_type(context):
    if context is None:
        return consts.ASYNC_BROKER_ENGINE
    value = context.get(consts.TALARIA_KEY, None)
    if value is None:
        return consts.ASYNC_BROKER_ENGINE
    if value is True:
        return consts.ASYNC_TALARIA_INDEXER_ENGINE
    if value == consts.TALARIA_INDEXER or value == consts.TALARIA_SERVER:
        return consts.ASYNC_TALARIA_SERVER_ENGINE
    return consts.ASYNC_BROKER_ENGINE

def is_talaria(context):
    engine = async_type(context)
    return engine == consts.ASYNC_TALARIA_INDEXER_ENGINE or engine == consts.ASYNC_TALARIA_SERVER_ENGINE

class ImplySqlRequest(SqlRequest):

    def __init__(self, client, sql):
        SqlRequest.__init__(self, client.client, sql)
        self.imply_client = client
        self.async_mode = None

    def with_async(self, query_mode=consts.ASYNC_QUERY):
        self.async_mode = query_mode
        if query_mode == consts.TALARIA_INDEXER:
            self.with_context({consts.TALARIA_KEY: True})
        elif query_mode == consts.TALARIA_SERVER:
            self.with_context({consts.TALARIA_KEY: query_mode})
        return self

    def is_async(self):
        return self.async_mode is not None

    def async_type(self):
        return async_type(self.context)

    def format(self):
        if is_talaria(self.context):
            return base_consts.SQL_ARRAY
        else:
            return SqlRequest.format(self)

    def run(self):
        if self.is_async():
            return self.imply_client.async_sql(self)
        else:
            return self.imply_client.sql_query(self)
    
class AsyncResponse:

    def __init__(self, request, response):
        self.imply_client = request.imply_client
        self._status = None
        self._error = None
        self.http_response = response
        self.report = None
        self.request = request

        # Typical response:
        # {'asyncResultId': '6f7b514a446d4edc9d26a24d4bd03ade_fd8e242b-7d93-431d-b65b-2a512116924c_bjdlojgj',
        # 'state': 'INITIALIZED'}
        self._id = None
        self._state = None
        self.response_obj = None
        self._results = None
        self._details = None
        self._schema = None
        self._rows = None
        if self._error is not None:
            return
        
        self.response_obj = response.json()
        self._id = self.response_obj['asyncResultId']
        self._state = self.response_obj['state']

    def format(self):
        return self.request.format()

    def id(self):
        return self._id

    def state(self):
        return self._state

    def done(self):
        return self._state == consts.ASYNC_FAILED or self._state == consts.ASYNC_COMPLETE

    def ok(self):
        return self._state == consts.ASYNC_COMPLETE

    def error(self):
        return self._error

    def status(self):
        self._status = self.imply_client.async_status(self._id)
        self._state = self._status['state']
        if self._state == consts.ASYNC_FAILED:
            try:
                self.error = self._status['error']['errorMessage']
            except KeyError:
                try:
                    self.error = self._status['error']['error']
                except KeyError:
                    self.error = self._status['error']
        return self._status
        
    def join(self):
        if not self.done():
            self.status()
            while not self.done():
                time.sleep(0.1)
                self.status()
        return self.done()

    def wait_done(self):
        if not self.join():
            raise DruidError("Query failed: " + self._error)

    def results(self):
        if self._results is None:
            self.wait_done()
            self._results = self.imply_client.async_results(self._id)
        return self._results

    def details(self):
        if self._details is None:
            self.wait_done()
            self._details = self.imply_client.async_details(self._id)
        return self._details

    def schema(self):
        if self._schema is None:
            results = self.results()
            if is_talaria(self.request.context):
                size = len(results[0])
                self._schema = []
                for i in range(size):
                    self._schema.append(ColumnSchema(results[0][i], results[2][i], results[1][i]))
            else:
                self._schema = parse_schema(self.format(), self.request.context, results)
        return self._schema

    def rows(self):
        if self._rows is None:
            results = self.results()
            if is_talaria(self.request.context):
                self._rows = results[3:]
            elif results is None:
                return self.response.text
            else:
                self._rows = parse_rows(self.format(), self.request.context, results)
        return self._rows
        
    def join(self):
        if not self.done():
            self.status()
            while not self.done():
                time.sleep(0.1)
                self.status()
        return self.done()

    def wait(self):
        self.wait_done()
        return self.rows()
        