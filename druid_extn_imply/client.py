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
from .talaria_query import TalariaQueryResult
from .sql import ImplySqlRequest, AsyncQueryResult, is_talaria
from . import consts

REQ_ROUTER_ASYNC_SQL = REQ_ROUTER_SQL + "/async"
ASYNC_QUERY_BASE = REQ_ROUTER_ASYNC_SQL + '/{}'
REQ_ROUTER_ASYNC_DETAILS = ASYNC_QUERY_BASE
REQ_ROUTER_ASYNC_STATUS = ASYNC_QUERY_BASE + "/status"
REQ_ROUTER_ASYNC_RESULTS = ASYNC_QUERY_BASE + "/results"

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
        resp = self.client.sql_query(request)
        if not resp.ok():
            return resp
        if is_talaria(request.context):
            resp = AsyncQueryResult(request, resp.http_response)
        return resp

    def sql_request(self, sql):
        return ImplySqlRequest(self, sql)

    def async_sql(self, request):
        response = self.client.post_only_json(REQ_ROUTER_ASYNC_SQL, 
                    request.to_request(), 
                    headers=request.headers)
        return AsyncQueryResult(request, response)

    def async_status(self, id, timeout_sec=10):
        return self.client.get_json(REQ_ROUTER_ASYNC_STATUS, args=[id],
                    params={'timeout': str(timeout_sec * 1000)})

    def async_details(self, id):
        return self.client.get_json(REQ_ROUTER_ASYNC_DETAILS, args=[id])

    def async_results(self, id):
        return self.client.get_json(REQ_ROUTER_ASYNC_RESULTS, args=[id])

    def native_talaria(self, request):
        if request is None:
            raise ClientError("No query provided.")
        if type(request) == str:
            request = self.client.sql_request(request)
        request.with_context({consts.TALARIA_KEY: consts.TALARIA_SERVER})
        resp = self.client.sql_query(request)
        return TalariaQueryResult(request, resp.http_response)
