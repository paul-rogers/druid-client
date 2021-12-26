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

"""
Python wrapper for the Apache Druid REST API.

The client provides common operations such as running a query. You
can run a query and get the raw JSON results, or you can convert
the results to a Pandas data frame or other representation.

The cluster class provides access to the API for the Coordinator,
Overlord and other Druid services.

The metadata class provides easy access to system metadata, choosing
to use either the system tables, or the native API as needed to
obtain the information. The table metadata class does the same, but
for a single Druid table (data source).

Typical usage:

  import pydruid2

  client = pydruid2.connect("http://localhost:8888")
  client.sql("SELECT * FROM wikipedia LIMIT 10")

See the documentation at (url needed).

See pydruid (https://github.com/druid-io/pydruid) for a wrapper for the native query API.
Obtain a pydruid client as:

  pyd_client = client.query_client()

See the API reference (https://druid.apache.org/docs/latest/operations/api-reference.html)
for details of the API. The API detail docstrings are courtesy of this page.
"""

from .client.config import ClusterConfig
from .client.client import Client
from .client import service

def connect(url, **kwargs):
    """
    Connect to a Druid server.
    
    Parameters
    ----------
    url : str
        URL of your Router or Coordinator node.
    
    url_map : map, default = None
        If your cluster is in Docker, Kubernetes, AWS, or otherwise
        has URLs different on your local system than those known within
        the cluster itself, this map provides a mapping from the locally
        known URLs (such as those exposed from Docker) to the URLs known
        within the cluster.

    tls_cert : string, default = None
        Path to a certificate for a private TLS key used for private
        connections within your own cluser or data center.
    """
    return Client(ClusterConfig(kwargs), url)

def set_trace(option=True):
    """
    Enable or disable URL tracing.

    When enabled, pydruid2 prints the URL before each call to aid debugging.
    """
    service.trace = option
