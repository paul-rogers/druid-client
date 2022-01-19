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
from .client import Client

discovered_plugins = {
    name: importlib.import_module(name)
    for _, name, _
    in pkgutil.iter_modules()
    if name.startswith('pydruid2_')
}

extensions = None

# Just to keep the IDE happy
dummy = Client

class ExtensionPoints:

    def __init__(self, name):
        self.client = None
        self.summary = name.title() + " extension"

class ExtensionDescriptor:

    def __init__(self, name, module):
        self.name = name
        self.module = module
        self.key = name[len('pydruid2_'):]
        self.extn_points = ExtensionPoints(name)

clientCode = \
'''
def {}_extn(self):
    """
    Client for {}.
    """
    return self.extn('{}')
    
Client.{}_extn = {}_extn
'''

def load_extensions():
    global extensions
    if extensions is not None:
        return extensions
    extensions = {}
    for name, module in discovered_plugins.items():
        try:
            defn = ExtensionDescriptor(name, module)
            module.register_client(defn.extn_points)
            extensions[defn.key] = defn
        except Exception:
            # Print a message, but continue, so that a bad extension does not
            # prevent pydruid2 from working.
            print("Error when registering module '{}'', error: {}".format(name), str(e))
    for key, defn in extensions.items():
        if defn.extn_points.client is not None:
            exec(clientCode.format(key, defn.extn_points.summary, key, key, key))
    return extensions
    