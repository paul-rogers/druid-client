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

from druid_client.client.error import DruidError
from druid_client.client.sql import ColumnSchema
from .sql import AbstractAsyncQueryResult

TALARIA_WAITING_STATE = 'WAITING'
TALARIA_RUNNING_STATE = 'RUNNING'
TALARIA_COMPLETED_STATE = 'COMPLETED'
TALARIA_FAILED_STATE = 'FAILED'
TALARIA_CANCELLED_STATE = 'CANCELLED'

class TalariaQueryResult(AbstractAsyncQueryResult):
    """
    Async query that works directly against a Talaria server.

    Primarily for testing and exploration. Use the Async API
    for normal use.
    """

    def __init__(self, request, response):
        AbstractAsyncQueryResult.__init__(self, request, response)
        candidates = request.client.cluster().talaria_role()
        if len(candidates) == 0:
            raise DruidError("No Talaria servers available")
        if len(candidates) > 1:
            raise DruidError("Client does not yet support multiple Talaria servers")
        self.server = candidates[0]
        row = response.json()[0]
        self._id = row['TASK']
        self._state = row['state']
        self._error = None # TODO
        self._details = None
        self._results = None
        self._schema = None

        # Create an artificial initial status that mimics the server
        self._status = {
            'id': self._id,
            'state': self._state}

    def id(self):
        return self._id

    def state(self):
        return self._state

    def done(self):
        return self._state != TALARIA_WAITING_STATE and self._state != TALARIA_RUNNING_STATE

    def ok(self):
        return self._state == TALARIA_COMPLETED_STATE

    def error(self):
        return self._error

    def status(self):
        self._status = self.server.status(self._id)
        self._state = self._status['state']
        return self._status
       
    def results(self):
        if self._results is None:
            self.wait_done()
            self._results = self.server.results(self._id)
        return self._results

    def details(self):
        if self._details is None:
            self.wait_done()
            self._details = self.server.details(self._id)
        return self._details

    def schema(self):
        if self._schema is None:
            cols = self.results()['schema']
            self._schema = []
            for col in cols:
                self._schema.append(ColumnSchema(
                    col['name'], col['sqlType'], col['druidType']))
        return self._schema

    def rows(self):
        return self.results()['results']
