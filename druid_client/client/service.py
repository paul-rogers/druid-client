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

from urllib.parse import quote
import requests
from .util import is_blank, dict_get

def check_error(response):
    """
    Raises a requests HttpError if the response code is not OK or Accepted.

    If the response inclded a JSON payload, then the message is extracted
    from that payload, else the message is from requests. The JSON
    payload, if any, is returned in the json field of the error.
    """
    code = response.status_code
    if code == requests.codes.ok or code == requests.codes.accepted:
        return
    error = None
    json = None
    try:
        json = response.json()
        msg = dict_get(json, 'error')
        if not is_blank(msg):
            error = msg
    except Exception:
        pass
    if code == requests.codes.not_found and error is None:
        error = "Not found"
    if error is not None:
        response.reason = error
    try:
        response.raise_for_status()
    except Exception as e:
        e.json = json
        raise e

STATUS_BASE = "/status"
REQ_STATUS = STATUS_BASE
REQ_HEALTH = STATUS_BASE + "/health"
REQ_PROPERTIES = STATUS_BASE + "/properties"
REQ_IN_CLUSTER = STATUS_BASE + "/selfDiscovered/status"

class Service:

    def __init__(self, cluster_config, endpoint):
        self.cluster_config = cluster_config
        self.endpoint = endpoint
        self.session = requests.Session()
        self.session.verify = cluster_config.tls_cert

    def close(self):
        self.session.close()
    
    #-------- REST --------
    
    def base_url(self):
        """Returns the base (external) URL for this service used by this client."""
        return self.endpoint
    
    def build_url(self, req, args=None) -> str:
        """
        Returns the full URL for a REST call given the relative request API and
        optional parameters to fill placeholders within the request URL.
        
        Parameters
        ----------
        req : str
            relative URL, with optional {} placeholders

        args : list
            optional list of values to match {} placeholders
            in the URL.
        """
        url = self.endpoint + req
        if args is not None:
            quoted = [quote(arg) for arg in args]
            url = url.format(*quoted)
        return url
    
    def get(self, req, args=None, params=None, require_ok=True) -> requests.Request:
        '''
        Generic GET request to this service.

        Parameters
        ----------
        req: str
            The request URL without host, port or query string.
            Example: `/status`

        args: [str], default = None
            Optional parameters to fill in to the URL.
            Example: `/customer/{}`

        params: dict, default = None
            Optional map of query variables to send in
            the URL. Query parameters are the name/values pairs
            that appear after the `?` marker.

        require_ok: bool, default = True
            Whether to require an OK (200) response. If `True`, and
            the request returns a different response code, then raises
            a `RestError` exception.

        Returns
        -------
        The `requests` `Request` object.
        '''
        url = self.build_url(req, args)
        if self.cluster_config.trace:
            print("GET:", url)
        r = self.session.get(url, params=params)
        if require_ok:
            check_error(r)
        return r

    def get_json(self, url_tail, args=None, params=None):
        '''
        Generic GET request which expects a JSON response.
        '''
        r = self.get(url_tail, args, params)
        return r.json()

    def post(self, req, body, args=None, headers=None, require_ok=True) -> requests.Request:
        """
        Issues a POST request for the given URL on this
        node, with the given payload and optional URL query 
        parameters.
        """
        url = self.build_url(req, args)
        if self.cluster_config.trace:
            print("POST:", url)
            print("body:", body)
        r = self.session.post(url, data=body, headers=headers)
        if require_ok:
            check_error(r)
        return r

    def post_json(self, req, body, args=None, headers=None):
        """
        Issues a POST request for the given URL on this
        node, with the given payload and optional URL query 
        parameters. The payload is serialized to JSON.
        """
        r = self.post_only_json(req, body, args, headers)
        check_error(r)
        return r.json()

    def post_only_json(self, req, body, args=None, headers=None) -> requests.Request:
        """
        Issues a POST request for the given URL on this
        node, with the given payload and optional URL query 
        parameters. The payload is serialized to JSON.

        Does not parse error messages: that is up to the caller.
        """
        url = self.build_url(req, args)
        if self.cluster_config.trace:
            print("POST:", url)
            print("body:", body)
        return self.session.post(url, json=body, headers=headers)

    def delete_json(self, req, args=None, headers=None):
        url = self.build_url(req, args)
        if self.cluster_config.trace:
            print("DELETE:", url)
        r = self.session.delete(url, headers=headers)
        return r.json()

    #-------- Common --------

    def service(self):
        """The name of this service."""
        raise NotImplemented

    def status(self):
        """
        Returns the Druid version, loaded extensions, memory used, total memory 
        and other useful information about the process.

        GET `/status`

        See https://druid.apache.org/docs/latest/operations/api-reference.html#process-information
        """
        return self.get_json(REQ_STATUS)
    
    def is_healthy(self) -> bool:
        """
        Returns `True` if the node is healthy, an exception otherwise.
        Useful for automated health checks.

        GET `/status/health`

        See https://druid.apache.org/docs/latest/operations/api-reference.html#process-information
        """
        try:
            return self.get_json(REQ_HEALTH)
        except Exception:
            return False
    
    def properties(self) -> map:
        """
        Returns the effective set of Java properties used by the service, including
        system properties and properties from the `common_runtime.propeties` and
        `runtime.properties` files.

        GET `/status/properties`

        See https://druid.apache.org/docs/latest/operations/api-reference.html#process-information
        """
        return self.get_json(REQ_PROPERTIES)
    
    def in_cluster(self):
        """
        Returns `True` if the node is visible wihtin the cluster, `False` if not.
        (That is, returns the value of the `{"selfDiscovered": true/false}`
        field in the response.

        GET `/status/selfDiscovered/status`

        See https://druid.apache.org/docs/latest/operations/api-reference.html#process-information
        """
        try:
            result = self.get_json(REQ_IN_CLUSTER)
            return dict_get(result, 'selfDiscovered', False)
        except ConnectionError:
            return False

    def trace(self, option=True):
        self.cluster_config.trace = option
