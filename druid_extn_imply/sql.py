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
from druid_client.client.error import ClientError, DruidError
from druid_client.client import consts as base_consts
from druid_client.client.sql import SqlRequest, ColumnSchema, AbstractSqlQueryResult, parse_schema, parse_rows
import druid_client.client.consts as druid_consts
from . import consts

class ImplySqlRequest(SqlRequest):

    def __init__(self, client, sql):
        SqlRequest.__init__(self, client.client, sql)
        self.imply_client = client
        self.engine = consts.BROKER_ENGINE

    def as_async(self):
        self.engine = consts.ASYNC_BROKER_ENGINE
        return self
    
    def as_task(self):
        self.engine = consts.TASK_ENGINE
        return self

    def is_async(self):
        return self.engine == consts.ASYNC_BROKER_ENGINE

    def is_task(self):
        return self.engine == consts.TASK_ENGINE

    def set_defaults(self):
        if self.engine == consts.TASK_ENGINE:
            if self.context is None:
                self.context = {}
            if self.context.get(consts.MSQE_TASKS_KEY) is None:
                self.context[consts.MSQE_TASKS_KEY] = 2

    def format(self):
        if self.engine == consts.TASK_ENGINE:
            return base_consts.SQL_ARRAY
        else:
            return SqlRequest.format(self)

    def run(self):
        if self.engine == consts.ASYNC_BROKER_ENGINE:
            return self.imply_client.async_sql(self)
        elif self.engine == consts.TASK_ENGINE:
            return self.imply_client.sql_task(self)
        else:
            return self.imply_client.sql_query(self)

    def to_request(self):
        #self.set_defaults()
        return super().to_request()
    
class AbstractAsyncQueryResult(AbstractSqlQueryResult):

    def __init__(self, request, response):
        AbstractSqlQueryResult.__init__(self, request, response)
        self.imply_client = request.imply_client
        self._status = None
        self._results = None
        self._details = None
        self._schema = None
        self._rows = None

    def status(self):
        """
        Polls Druid for an update on the query run status.

        Format of the return value depends on the Async protocol used.
        """
        raise NotImplementedError

    def details(self):
        """
        Returns excecution details for the query, if any.
        """
        raise NotImplementedError

    def done(self):
        """
        Reports if the query is done: succeeded or failed.
        """
        raise NotImplementedError

    def finished(self):
        """
        Reports if the query is succeeded.
        """
        raise NotImplementedError

    def state(self):
        """
        Reports the engine-specific query state.

        Updated after each call to status().
        """
        raise NotImplementedError

    def join(self):
        if not self.done():
            self.status()
            while not self.done():
                time.sleep(0.1)
                self.status()
        return self.finished()

    def wait_done(self):
        if not self.join():
            raise DruidError("Query failed: " + self._error)

    def wait(self):
        self.wait_done()
        return self.rows()

class AsyncQueryResult(AbstractAsyncQueryResult):

    def __init__(self, request, response):
        AbstractAsyncQueryResult.__init__(self, request, response)

        # Typical response:
        # {'asyncResultId': '6f7b514a446d4edc9d26a24d4bd03ade_fd8e242b-7d93-431d-b65b-2a512116924c_bjdlojgj',
        # 'state': 'INITIALIZED'}
        if self.is_response_ok():
            self.response_obj = response.json()
            self._id = self.response_obj['asyncResultId']
            self._state = self.response_obj['state']
        else:
            self.response_obj = None
            self._id = None
            self._state = consts.ASYNC_FAILED

    def id(self):
        return self._id

    def state(self):
        return self._state

    def done(self):
        return self._state == consts.ASYNC_FAILED or self._state == consts.ASYNC_COMPLETE

    def finished(self):
        return self._state == consts.ASYNC_COMPLETE

    def error(self):
        return self._error

    def check_valid(self):
        if self._id is None:
            raise ClientError("Operation is invalid on a failed query")

    def status(self):
        self.check_valid()
        self._status = self.imply_client.async_status(self._id)
        self._state = self._status['state']
        if self._state == consts.ASYNC_FAILED:
            try:
                self._error = self._status['error']['errorMessage']
            except KeyError:
                try:
                    self._error = self._status['error']['error']
                except KeyError:
                    try:
                        self._error = self._status['error']
                    except KeyError:
                        self.error = "Unknown error"
        return self._status
        
    def results(self):
        self.check_valid()
        if self._results is None:
            self.wait_done()
            self._results = self.imply_client.async_results(self._id)
        return self._results

    def details(self):
        self.check_valid()
        if self._details is None:
            self.join()
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

class SqlTaskResult(AbstractAsyncQueryResult):

    def __init__(self, request, response):
        AbstractAsyncQueryResult.__init__(self, request, response)
        self._reports = None
        self._schema = None
        self._results = None

        # Typical response:
        # {'taskId': '6f7b514a446d4edc9d26a24d4bd03ade_fd8e242b-7d93-431d-b65b-2a512116924c_bjdlojgj',
        # 'state': 'RUNNING'}
        if self.is_response_ok():
            self.response_obj = response.json()
            self._id = self.response_obj['taskId']
            self._state = self.response_obj['state']
        else:
            self.response_obj = None
            self._id = None
            self._state = consts.SQL_TASK_FAILED

    def id(self):
        return self._id

    def state(self):
        return self._state

    def done(self):
        return self._state == druid_consts.FAILED_STATE or self._state == druid_consts.SUCCESS_STATE

    def finished(self):
        return self._state == druid_consts.SUCCESS_STATE

    def error(self):
        return self._error

    def check_valid(self):
        if self._id is None:
            raise ClientError("Operation is invalid on a failed query")

    def overlord(self):
        return self.imply_client.client.cluster().overlord()

    def status(self):
        self.check_valid()
        # Example:
        # {'task': 'talaria-sql-w000-b373b68d-2675-4035-b4d2-7a9228edead6', 
        # 'status': {
        #   'id': 'talaria-sql-w000-b373b68d-2675-4035-b4d2-7a9228edead6', 
        #   'groupId': 'talaria-sql-w000-b373b68d-2675-4035-b4d2-7a9228edead6', 
        #   'type': 'talaria0', 'createdTime': '2022-04-28T23:19:50.331Z', 
        #   'queueInsertionTime': '1970-01-01T00:00:00.000Z', 
        #   'statusCode': 'RUNNING', 'status': 'RUNNING', 'runnerStatusCode': 'PENDING', 
        #   'duration': -1, 'location': {'host': None, 'port': -1, 'tlsPort': -1}, 
        #   'dataSource': 'w000', 'errorMsg': None}}
        self._status = self.overlord().task_status(self._id)
        self._state = self._status['status']['status']
        if self._state == druid_consts.FAILED_STATE:
            self._error = self._status['status']['errorMsg']
        return self._status
        
    def reports(self) -> dict:
        self.check_valid()
        if self._reports is None:
            self.join()
            self._reports = self.overlord().task_reports(self._id)
        return self._reports

    def results(self):
        if self._results is None:
            rpts = self.reports()
            self._results = rpts['multiStageQuery']['payload']['results']
        return self._results

    def schema(self):
        if self._schema is None:
            results = self.results()
            sig = results['signature']
            sqlTypes = results['sqlTypeNames']
            size = len(sig)
            self._schema = []
            for i in range(size):
                self._schema.append(ColumnSchema(sig[i]['name'], sqlTypes[i], sig[i]['type']))
        return self._schema

    def rows(self):
        if self._rows is None:
            results = self.results()
            self._rows = results['results']
        return self._rows
