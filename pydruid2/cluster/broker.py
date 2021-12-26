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
from ..client.util import dict_get

BROKER_BASE = '/druid/broker/v1'
REQ_BROKER_STATUS = BROKER_BASE + '/loadstatus'

class Broker(Client):
    """
    Represents a Druid Broker.

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
        return consts.BROKER

    #-------- General --------
 
    def is_ready(self):
        """
        Calls the /druid/broker/v1/loadstatus API.

        Returns True if the Broker knows about all segments in the cluster. 
        This can be used to learn when a Broker process is ready to be 
        queried after a restart.

        See https://druid.apache.org/docs/latest/operations/api-reference.html#load-status
        """
        json = self.get_json(REQ_BROKER_STATUS)
        return dict_get(json, 'inventoryInitialized', False)
