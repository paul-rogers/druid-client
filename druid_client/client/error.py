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

class ClientError(Exception):
    """
    Indicates an error with usage of the API.
    """
    
    def __init__(self, msg):
        self.message = msg

class DruidError(Exception):
    
    def __init__(self, msg):
        self.message = msg

class ConfigError(Exception):
    
    def __init__(self, msg):
        self.message = msg

class NotFoundError(Exception):
    """
    Raised for 404 response errors when the API is reasonably sure that the
    error indicates an missing resource.
    """
    
    def __init__(self, msg):
        self.message = msg
