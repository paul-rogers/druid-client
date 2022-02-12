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

import importlib
import pkgutil

EXTN_PREFIX = 'druid_extn_'

discovered_plugins = {
    name: importlib.import_module(name)
    for _, name, _
    in pkgutil.iter_modules()
    if name.startswith(EXTN_PREFIX)
}

extension_manager = None

class ExtensionPoints:

    def __init__(self, name):
        self.summary = name.title() + " extension"
        self.client = None
        self.roles = None

class ExtensionDescriptor:

    def __init__(self, name, module):
        self.name = name
        self.module = module
        self.key = name[len(EXTN_PREFIX):]
        self.extn_points = ExtensionPoints(self.key)

client_code = \
'''
from .client import Client

def {}_extn(self):
    """
    Client for {}.
    """
    return self.extn('{}')
    
Client.{}_extn = {}_extn
'''

service_code = \
'''
from ..cluster.cluster import Cluster

def {}_role(self) -> "{}":
    """
    Servers providing the {} role from {}.
    """
    return self.clients_for_role('{}')
    
Cluster.{}_role = {}_role
'''

def load_extensions():
    global extension_manager
    if extension_manager is None:
        extension_manager = Extensions()
    return extension_manager

class Extensions:

    def __init__(self):
        self.services_registered = False
        self.extensions = {}
        for name, module in discovered_plugins.items():
            try:
                defn = ExtensionDescriptor(name, module)
                module.register_client(defn.extn_points)
                self.extensions[defn.key] = defn
            except Exception:
                # Print a message, but continue, so that a bad extension does not
                # prevent druid-client from working.
                print("Error when registering module '{}'', error: {}".format(name), str(e))
        for key, defn in self.extensions.items():
            if defn.extn_points.client is not None:
                exec(client_code.format(key, defn.extn_points.summary, key, key, key))
    
    def client_for(self, client, extn):
        defn = self.extensions.get(extn, None)
        if defn is None or defn.extn_points.client is None:
            return None
        return defn.extn_points.client(client)

    def names(self):
        return [defn.key for defn in self.extensions.values()]

    def list(self):
        return [[defn.key, defn.name, defn.extn_points.summary]
                for defn in self.extensions.values()]

    def register_services(self, service_map):
        if self.services_registered:
            return
        for key, defn in self.extensions.items():
            if defn.extn_points.roles is not None:
                service_map.update(defn.extn_points.roles)
                for name, role in defn.extn_points.roles.items():
                    exec(service_code.format(name, role.__name__, name,
                        defn.extn_points.summary, name, name, name))
        self.services_registerd = True
