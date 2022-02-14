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

import uuid
from ..client.error import ClientError

START_STATE = 0
RUN_STATE = 1
FAILING_STATE = 2
END_STATE = 3

class ExternalTask:

    def __init__(self, cluster, table):
        self.cluster = cluster
        #self.id = str(uuid.uuid4())
        self.id = None
        self.task = {
            'type': 'extern_task',
            'hostId': None,
            #'id': self.id,
            'id': None,
            'groupId': self.id,
            'dataSource': table,
            'priority': 50,
            'context': {}
        }
        self.status = None
        self.state = START_STATE
        self.error = None

    def overlord(self):
        return self.cluster.overlord()

    def register(self):
        if self.state != START_STATE:
            raise ClientError("External task already started")
        self.status = self.overlord().extern_register(self.task)
        if type(self.status) is not dict:
            self.state = END_STATE
            return False
        self.id = self.status.get('task')
        if self.id is None:
            self.error = self.status.get('error')
            self.state = END_STATE
            return False
        self.state = RUN_STATE
        return True

    def assert_running(self):
        if self.state != RUN_STATE:
            raise ClientError("External task is not running")

    def ping(self):
        self.assert_running()
        self.status = self.overlord().extern_ping(self.id)
        if type(self.status) is not dict:
            self.state = FAILING_STATE
            return False
        if self.status.get('status') != 'OK':
            self.error = self.status.get('error')
            self.state = FAILING_STATE
            return False
        return True

    def action(self, action):
        self.assert_running()
        return self.overlord.extern_action(self.id, action)

    def succeeded(self):
        self.assert_running()
        self.status = self.overlord().extern_complete(self.id)
        self.state = END_STATE
        if type(self.status) is not dict:
            return False
        if self.status.get('status') != 'OK':
            self.error = self.status.get('error')
            return False
        return True

    def failed(self, msg="Unknown failure"):
        if self.state != RUN_STATE and self.state != FAILING_STATE:
            raise ClientError("External task is not running")
        self.overlord().extern_failed(self.id, msg)
        self.error = msg
        self.state = END_STATE
