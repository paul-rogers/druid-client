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

from druid_client.client.client import REQ_ROUTER_SQL
from druid_client.client.error import ClientError
from druid_client.client.util import is_blank
from .sql import ImplySqlRequest, AsyncQueryResult, SqlTaskResult

REQ_ROUTER_ASYNC_SQL = REQ_ROUTER_SQL + "/async"
ASYNC_QUERY_BASE = REQ_ROUTER_ASYNC_SQL + '/{}'
REQ_ROUTER_ASYNC_DETAILS = ASYNC_QUERY_BASE
REQ_ROUTER_ASYNC_STATUS = ASYNC_QUERY_BASE + "/status"
REQ_ROUTER_ASYNC_RESULTS = ASYNC_QUERY_BASE + "/results"

REQ_ROUTER_SQL_TASK = REQ_ROUTER_SQL + "/task"

class ImplyClient:

    def __init__(self, client):
        self.client = client

    def sql_query(self, request):
        """
        Imply-specific version of Client.sql_query which handles async Talaria
        queries.

        If the response appears to be an async Talaria response, returns an
        async result rather than the usual SqlQueryResult.
        """
        return self.client.sql_query(request)

    def sql_request(self, sql):
        return ImplySqlRequest(self, sql)

    def sql_async(self, request):
        response = self.client.post_only_json(REQ_ROUTER_ASYNC_SQL, 
                    request.to_request(), 
                    headers=request.headers)
        return AsyncQueryResult(request, response)

    def sql_task_request(self, sql):
        return ImplySqlRequest(self, sql).as_task()
    
    def sql_async_request(self, sql):
        return ImplySqlRequest(self, sql).as_async()

    def async_status(self, id, timeout_sec=10):
        return self.client.get_json(REQ_ROUTER_ASYNC_STATUS, args=[id],
                    params={'timeout': str(timeout_sec * 1000)})

    def async_details(self, id):
        return self.client.get_json(REQ_ROUTER_ASYNC_DETAILS, args=[id])

    def async_results(self, id):
        return self.client.get_json(REQ_ROUTER_ASYNC_RESULTS, args=[id])

    def sql_task(self, request):
        if request is None:
            raise ClientError("No query provided.")
        if type(request) == str:
            request = self.sql_task_request(request)
        if is_blank(request.sql):
            raise ClientError("No query provided.")
        if self.client.cluster_config.trace:
            print(request.sql)
        response = self.client.post_only_json(
                    REQ_ROUTER_SQL_TASK, 
                    request.to_request(), 
                    headers=request.headers)
        return SqlTaskResult(request, response)
