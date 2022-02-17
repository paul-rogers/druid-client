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

from ..client.client import Client
from ..client import consts

ROUTER_BASE = '/druid/router/v1'
REQ_BROKERS = ROUTER_BASE + '/brokers'
REQ_CLUSTER = ROUTER_BASE + '/cluster'

class Router(Client):
    """
    Client for the Druid Router service.
    
    Deprecated APIs
    ---------------
    As Druid has evolved, the API functionality has changed. This client does not
    implement the now-deprecated APIs:

    * /druid/v2/datasources
    * /druid/v2/datasources/{dataSourceName}
    * /druid/v2/datasources/{dataSourceName}/dimensions
    * /druid/v2/datasources/{dataSourceName}/metrics

    Druid recommends issuing SQL queries against the system tables instead.
    """

    def __init__(self, cluster_config, endpoint):
        Client.__init__(self, cluster_config, endpoint)
    
    def service(self):
        return consts.ROUTER

    def brokers(self):
        return self.get_json(REQ_BROKERS)

    def servers(self):
        return self.get_json(REQ_CLUSTER)

