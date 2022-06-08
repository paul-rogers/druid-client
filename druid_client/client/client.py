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

from .service import Service
from .error import ClientError
from .sql import SqlRequest, SqlQueryResult, QueryPlan
from .util import is_blank
from .display import Display

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
        self._extn_cache = {}
        self.cluster_config.display = Display()
        self._reports = None
    
    def service(self):
        return 'client'
   
    #-------- Query --------

    def _prepare_query(self, request):
        if request is None:
            raise ClientError("No query provided.")
        if type(request) == str:
            request = self.sql_request(request)
        if is_blank(request.sql):
            raise ClientError("No query provided.")
        if self.cluster_config.trace:
            print(request.sql)
        query_obj = request.to_request()
        return (request, query_obj)

    def sql_query(self, request) -> SqlQueryResult:
        '''
        Submit a SQL query with control over the context, parameters and other
        options. Returns a response with either a detailed error message, or
        the rows and query ID.
        '''
        request, query_obj = self._prepare_query(request)
        r = self.post_only_json(REQ_ROUTER_SQL, query_obj, headers=request.headers)
        return SqlQueryResult(request, r)

    def sql(self, sql, *args):
        if len(args) > 0:
            sql = sql.format(*args)
        resp = self.sql_query(sql)
        if resp.ok():
            return resp.rows()
        raise ClientError(resp.error_msg())

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
    
    def sql_request(self, sql):
        return SqlRequest(self, sql)
   
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
    
    #-------- Display --------

    def show_as_text(self):
        self.cluster_config.display.text()

    def show_as_html(self):
        self.cluster_config.display.html()

    def _display(self):
        return self.cluster_config.display

    def show(self):
        if self._reports is None:
            from ..show.show import Reports
            self._reports = Reports(self)
        return self._reports
  
    #-------- Misc. --------

    # Really belongs in ClusterMetadata, but is so common it is put here,
    # depite having to load the cluster package.
    def version(self):
        status = self.cluster().coordinator().status()
        return status.get('version')

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
   
    #-------- Extensions --------
    #
    # In addition to this method, a method named <extn>_extn is defined
    # for each extension that provides client services.

    def extn(self, name):
        extn = self._extn_cache.get(name, None)
        if extn is not None:
            return extn
        extn = self.cluster_config.client_for(self, name)
        if extn is None:
            return None
        self._extn_cache[name] = extn
        return extn
    
    def extn_names(self):
        return self.cluster_config.extension_names()
    
    def extns(self):
        return self.cluster_config.extension_list()

