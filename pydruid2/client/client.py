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

from .service import Service, trace_enabled
from .error import ClientError
from .sql import SqlRequest, SqlResponse, QueryPlan
from .util import is_blank, dict_get

ROUTER_BASE = '/druid/v2'
REQ_ROUTER_QUERY = ROUTER_BASE
REQ_ROUTER_SQL = ROUTER_BASE + '/sql'

class Client(Service):
    """
    Represents a Druid query client.

    The underlying Druid service may be a Router or a Broker: this class
    is agnostic as it only provides the common, query-related services, and
    the status APIs available on all Druid nodes.
    """

    def __init__(self, cluster_config, endpoint):
        Service.__init__(self, cluster_config, endpoint)
        self._query_client = None
    
    def service(self):
        return 'client'
   
    #-------- Query --------

    def sql_query(self, request) -> SqlResponse:
        '''
        Submit a SQL query with control over the context, parameters and other
        options. Returns a response with either a detailed error message, or
        the rows and query ID.
        '''
        if request is None:
            raise ClientError("No query provided.")
        if type(request) == str:
            request = SqlRequest(request)
        if is_blank(request.sql):
            raise ClientError("No query provided.")
        if trace_enabled():
            print(request.sql)
        query_obj = request.to_request()
        url = self.build_url(REQ_ROUTER_SQL)
        if trace_enabled():
            print("url:", url)
            print("body:", query_obj)
        r = self.session.post(url, json=query_obj, headers=request.headers)
        return SqlResponse(request, r)

    def sql(self, sql, *args):
        if len(args) > 0:
            sql = sql.format(*args)
        resp = self.sql_query(sql)
        if resp.ok():
            return resp.rows()
        raise ClientError(resp.error())

    def explain_sql(self, query) -> QueryPlan:
        """
        Run an EXPLAIN PLAN FOR query for the given query.

        Returns
        -------
        An object with the plan JSON parsed into Python objects:
        plan: the query plan
        columns: column schema
        tables: dictionary of name/type pairs
        """
        if is_blank(query):
            raise ClientError("No query provided.")
        results = self.sql('EXPLAIN PLAN FOR ' + query)
        return QueryPlan(results[0])
   
    #-------- Cluster Services --------

    def cluster(self): # -> Cluster: but don't want to import unless requested
        if self.cluster_config.cluster is None:
            from ..cluster.cluster import Cluster
            self.cluster_config.cluster = Cluster(self)
        return self.cluster_config.cluster

    def metadata(self): # -> ClusterMetadata: but don't want to import unless requested
        return self.cluster().metadata()

    def table(self, table_name): # -> TableMetadata: but don't want to import unless requested
        return self.cluster().table(table_name)
   
    #-------- Misc. --------

    # Really belongs in ClusterMetadata, but is so common it is put here,
    # depite having to load the cluster package.
    def version(self):
        status = self.cluster().coord().status()
        return dict_get(status, 'version')

    def query_client(self):
        """
        Returns a client for `pydruid` configured as for this client.
        """
        if self._query_client is None:
            import pydruid.client as pydruid_client
            coord = self.router()
            self._query_client = pydruid_client.PyDruid(coord.node.config.url_prefix(), 'druid/v2')
            self._query_client.cafile = self.config.tls_cert
        return self._query_client
