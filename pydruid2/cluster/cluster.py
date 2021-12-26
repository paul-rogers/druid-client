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

from ..client.util import dict_get, endpoint, service_url
from ..client.error import ConfigError, DruidError, ClientError
from ..client import consts
from .coord import Coordinator
from .overlord import Overlord
from .router import Router
from .broker import Broker
from .metadata import ClusterMetadata
from .table import TableMetadata

service_map = {
    consts.COORDINATOR: Coordinator,
    consts.OVERLORD: Overlord,
    consts.ROUTER: Router,
    consts.BROKER: Broker
    }

def register_service(role, service_class):
    """
    Registers a custom service client class.

    Parameters
    ----------
    role : str
        The role name to register.

    service_class : class
        The subclass of the Service class to register for the role.
    """
    service_map[role] = service_class

def service_key(host, port):
    if port == -1:
        return None
    return endpoint(host, port)

def index_key(http_key, tls_key):
    key = '' if http_key is None else http_key
    if tls_key is not None:
        key += '' if key == '' else '/'
        key += tls_key
    return key

class Role:

    def __init__(self, record):
       self.role = record['server_type']
       self.is_lead = record['is_leader']
       self.client = None

    def close(self):
        if self.client is not None:
            self.client.close()
            self.client = None

class ServiceConfig:

    def __init__(self, config, record):
        self.cluster_config = config
        self.record = record
        remote_host = record['host']
        role = Role(record)
        self._roles = {role.role: role}
        self.http_key = service_key(remote_host, record['plaintext_port'])
        self.tls_key = service_key(remote_host, record['tls_port'])
        self.index_key = index_key(self.http_key, self.tls_key)
        self._url = None
        self.local_key = None

    def map_url(self):
        mapper = self.cluster_config.service_mapper
        prefer_tls = self.cluster_config.prefer_tls
        (protocol, host, port) = mapper.url_for(self.record['host'], self.record['plaintext_port'], self.record['tls_port'], prefer_tls)
        self._url = service_url(protocol, host, port)
        self.local_key = service_key(host, port)

    def roles(self):
        return [role.role for role in self._roles.values()]

    def is_a(self, role):
        return role in self._roles

    def has_roles(self, roles):
        for role in roles:
            if not role in self._roles:
                return False
        return True
    
    def url(self):
        return self._url

    def descrip(self):
        return self.record

    def is_lead(self, role):
        try:
            role_def = self._roles[role]
            return role_def.is_lead
        except KeyError:
            return False

    def client(self, role):
        try:
            role_def = self._roles[role]
        except KeyError:
            return ClientError("Server {} does not provide role {}".format(self._url, role))
        if role_def.client is not None:
            return role_def.client
        cls = dict_get(service_map, role)
        if cls is None:
            raise ConfigError("No client class defined for role " + role)
        role_def.client = cls(self.cluster_config, self._url)
        return role_def.client

    def close(self):
        for role in self._roles.values:
            role.close()

class Cluster:
    """
    Represents a Druid cluster.

    The cluster is "bootstrapped" from a client which is used to get a list of the
    services that make up the cluster. Clients can then request specific services
    (Coordinator or Overlord), or all nodes that offer a particular role.
    """

    def __init__(self, client):
        self._client = client
        self._config = client.cluster_config
        self._config.cluster = self
        self._services = {}
        self._broker = None
        self._coordinator = None
        self._overlord = None
        self._metadata = None
        self._table_metadata = {}
        self.refresh()
    
    def client(self):
        """
        Return the query client for this cluster.
        """
        return self._client

    def refresh(self):
        """
        Update the set of services within the cluster.

        Call this if nodes are added, removed or if the lead service changes.
        """
        self._servers = self._client.sql('SELECT * FROM sys.servers')
        servers = {}
        for server_row in self._servers:
            service = ServiceConfig(self._config, server_row)
            try:
                server = servers[service.index_key]
                server._roles.update(service._roles)
            except KeyError:
                 servers[service.index_key] = service
        old_services = self._services
        self._services = {}
        for key, service in servers.items():
            old_service = dict_get(old_services, key)
            if old_service is None or not old_service.has_roles(service.roles()):
                service.map_url()
                self._services[key] = service
            else:
                self._services[key] = old_service
                del(old_services, key)
                if self._coordinator == old_service:
                    self._coordinator = None
                if self._overlord == old_service:
                    self._overlord = None
        for service in old_services.values():
            service.close()
    
    def servers(self):
        """
        Returns the contents of the Druid system.SERVERS table.
        """
        return self._servers

    def lead(self, role):
        """
        Internal method to return the lead service for a role.
        """
        for service in self._services.values():
            if service.is_lead(role):
                return service
        return None
    
    def for_role(self, role):
        services = []
        for service in self._services.values():
            if service.is_a(role):
                services.append(service)
        return services

    def coord(self):
        """
        Returns the client for the lead Coordinator.
        """
        if self._coordinator is None:
            self._coordinator = self.lead(consts.COORDINATOR)
            if self._coordinator is None:
                raise DruidError("No lead Coordinator is available.")
        return self._coordinator.client(consts.COORDINATOR)

    def overlord(self):
        """
        Returns the client for the lead Overlord.
        """
        if self._overlord is None:
            self._overlord = self.lead(consts.OVERLORD)
            if self._overlord is None:
                raise DruidError("No lead Overlord is available.")
        return self._overlord.client(consts.OVERLORD)

    def broker(self):
        """
        Returns a client for a Broker.
        """
        if self._broker is None:
            brokers = self.for_role(consts.BROKER)
            if len(brokers) == 0:
                raise DruidError("No Broker is available.")
            ## Arbitrarily pick the first one
            self._broker = brokers[0]
        return self._broker.client(consts.BROKER)

    def metadata(self) -> ClusterMetadata:
        if self._metadata is None:
            self._metadata = ClusterMetadata(self)
        return self._metadata

    def table(self, table_name) -> TableMetadata:
        try:
            return self._table_metadata[table_name]
        except KeyError:
            table = TableMetadata(self, table_name)
            self._table_metadata[table_name] = table
            return table
    
