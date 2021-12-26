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

from .util import dict_get, split_host_url, service_url
from . import consts

class ServiceMapper:
    """
    Maps cluster services to local URLs.

    This is a base class for Docker, AWS or other mappings. This
    implementation is a "null mapping": addresses map to themselves.
    """

    def url_for(self, host, http_port, tls_port, prefer_tls):
        """
        Create a URL for a service given the remote host and ports.

        Parameters
        ----------
        host : str
            The remote host.

        http_port : int
            The remote plain-text (HTTP) port, or -1 if not available.

        tls_port : int
            The remote TLS (HTTPS) port, or -1 if not available.

        prefer_tls : bool
            If True, prefer to map the TLS port before the HTTP port.

        Returns
        -------
        A triple with the protocol (AKA schema): http or https, the
        local host name and the local port. The combination, when converted
        by the caller to a URL, must be accessible to the machine on which
        the pydruid2 library is running.
        """
        if (prefer_tls and tls_port != -1) or http_port == -1:
            return (consts.TLS_PROTOCOL, host, tls_port)
        else:
            return (consts.PLAIN_TEXT_PROTOCOL, host, http_port)

class DockerMapper(ServiceMapper):
    """
    Maps cluster services to local URLs for Druid running in Docker.

    The default configuration assumes the Druid cluster runs on
    localhost, with no port changes. Provide an alternative Docker host
    name, or a set of port mappings, to change the defaults.
    """

    def __init__(self, docker_host="localhost", port_map=[]):
        """
        Constructor.

        Parameters
        ----------
        docker_host : str, Default = "localhost"
            The IP address or name of the host on which Docker is running.

        port_map : [], Default = []
            An array of Docker-style port mappings, where each entry is
            a two-element array or tuple of the form
            (external-port, internal_port).
        """
        self.docker_host = docker_host
        self.port_map = {}
        for pair in port_map:
            self.port_map[pair[1]] = pair[0]

    def url_for(self, host, http_port, tls_port, prefer_tls):
        local_http_port = dict_get(self.port_map, http_port)
        if prefer_tls or local_http_port is None:
            port = dict_get(self.port_map, tls_port)
            if port is not None:
                return (consts.TLS_PROTOCOL, self.docker_host, port)
        if local_http_port is not None:
            return (consts.PLAIN_TEXT_PROTOCOL, self.docker_host, local_http_port)
        return ServiceMapper.url_for(self, self.docker_host, http_port, tls_port, prefer_tls)

class ClusterConfig:

    def __init__(self, config):
        self.options = config
        self.cluster = None
        self.service_mapper = dict_get(config, 'mapper', ServiceMapper())
        self.tls_cert = dict_get(config, 'tls_cert')
        self.prefer_tls = dict_get(config, 'prefer_tls', False)

    def map_endpoint(self, remote_url):
        """
        Maps a remote endpoint URL to the local equivalent.
        """
        scheme, host, port = split_host_url(remote_url)
        http_port = port if scheme == consts.PLAIN_TEXT_PROTOCOL else None
        tls_port = port if scheme == consts.TLS_PROTOCOL else None
        scheme, host, port = self.service_mapper.url_for(host, http_port, tls_port, False)
        return service_url(scheme, host, port)
