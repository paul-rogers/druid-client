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

from ..client.service import Service
from ..client import consts

OVERLORD_BASE = '/druid/indexer/v1'
REQ_OL_LEADER = OVERLORD_BASE + '/leader'
REQ_IS_OL_LEADER = OVERLORD_BASE + '/isLeader'

# Tasks
REQ_TASKS = OVERLORD_BASE + '/tasks'
REQ_POST_TASK = OVERLORD_BASE + '/task'
REQ_GET_TASK = REQ_POST_TASK + '/{}'
REQ_TASK_STATUS = REQ_GET_TASK + '/status'
REQ_TASK_REPORTS = REQ_GET_TASK + '/reports'
REQ_END_TASK = REQ_GET_TASK
REQ_END_DS_TASKS = REQ_END_TASK + '/shutdownAllTasks'

# Supervisors
REQ_SUPERVISORS_IDS = OVERLORD_BASE + '/supervisor'
REQ_SUPERVISORS = OVERLORD_BASE + '/supervisor'
REQ_ALL_SUPERVISOR_HISTORY = REQ_SUPERVISORS + '/history'
REQ_SUPERVISOR = REQ_SUPERVISORS + '/{}'
REQ_SUPERVISOR_STATUS = REQ_SUPERVISOR + '/status'
REQ_SUPERVISOR_HISTORY = REQ_SUPERVISOR + '/history'

class Overlord(Service):
    """
    Represents a Druid Overlord.

    Deprecated APIs
    ---------------
    As Druid has evolved, the API functionality has changed. This client does not
    implement the now-obsolete APIs. Druid recommends issuing SQL queries against
    the system tables instead.

    * `/druid/indexer/v1/task/{taskId}/segments`

    The following are redundant:

    * `/druid/indexer/v1/completeTasks`
    * `/druid/indexer/v1/runningTasks`
    * `/druid/indexer/v1/waitingTasks`
    * `/druid/indexer/v1/pendingTasks`

    APIs Not Yet Implemented
    ------------------------

    * `/druid/indexer/v1/taskStatus`
    * `POST /druid/indexer/v1/supervisor`
    * `/druid/indexer/v1/supervisor/<supervisorId>/suspend`
    * `/druid/indexer/v1/supervisor/suspendAll`
    * `/druid/indexer/v1/supervisor/<supervisorId>/resume`
    * `/druid/indexer/v1/supervisor/resumeAll`
    * `/druid/indexer/v1/supervisor/<supervisorId>/reset`
    * `/druid/indexer/v1/supervisor/<supervisorId>/terminate`
    * `/druid/indexer/v1/supervisor/terminateAll`
    * `/druid/indexer/v1/supervisor/<supervisorId>/shutdown`
    * `/druid/indexer/v1/worker`
    * `/druid/indexer/v1/worker/history?interval={interval}&count={count}`
    * `/druid/indexer/v1/workers`
    * `/druid/indexer/v1/scaling`
    """

    def __init__(self, cluster_config, endpoint):
        Service.__init__(self, cluster_config, endpoint)
    
    def service(self):
        return consts.OVERLORD

    def lead(self) -> str:
        '''
        Returns the current leader Overlord of the cluster.

        See https://druid.apache.org/docs/latest/operations/api-reference.html#leadership-1
        '''
        return self.get(REQ_OL_LEADER).text
    
    def is_lead(self) -> bool:
        '''
        Returns True if this server is the current leader Overlord of the cluster,
        False otherwise.

        Caveat: the documented API call does not appear to work; returns a 404.
        Functionality is simulated for now.

        See https://druid.apache.org/docs/latest/operations/api-reference.html#leadership-1
        '''
        #r = self.node.get(self.node.url(REQ_IS_OL_LEADER))
        #return r.json()["leader"]

        return self.endpoint == self.cluster_config.map_endpoint(self.lead())

    #-------- Tasks --------
    
    def tasks(self, state=None, table=None, type=None, max=None, created_time_interval=None):
        '''
        Retrieve list of tasks.

        Parameters
        ----------
        state : str, default = None
        	Filter list of tasks by task state. Valid options are "running", 
            "complete", "waiting", and "pending". Constants are defined for
            each of these in the `consts` file.
        table : str, default = None
        	Return tasks filtered by Druid table (datasource).
        created_time_interval : str, Default = None
        	Return tasks created within the specified interval.
        max	: int, default = None
            Maximum number of "complete" tasks to return. Only applies when state is set to "complete".
        type : str, default = None
        	filter tasks by task type.

        Reference
        ---------
        `GET /druid/indexer/v1/tasks`

        See https://druid.apache.org/docs/latest/operations/api-reference.html#get-15
        '''
        params = {}
        if state is not None:
            params['state'] = state
        if table is not None:
            params['datasource'] = table
        if type is not None:
            params['type'] = type
        if max is not None:
            params['max'] = max
        if created_time_interval is not None:
            params['createdTimeInterval'] = created_time_interval
        return self.get_json(REQ_TASKS, params=params)

    def task(self, task_id):
        """
        Retrieve the "payload" of a task.

        Parameters
        ----------
        task_id : str
            The id of the task to retrieve

        Reference
        ---------
        `GET /druid/indexer/v1/task/{taskId}`

        See https://druid.apache.org/docs/latest/operations/api-reference.html#get-15
        """
        return self.get_json(REQ_GET_TASK, args=[task_id])

    def task_status(self, task_id):
        '''
        Retrieve the status of a task.

        Parameters
        ----------
        task_id : str
            The id of the task to retrieve

        Reference
        ---------
        `GET /druid/indexer/v1/task/{taskId}/status`

        See https://druid.apache.org/docs/latest/operations/api-reference.html#get-15
        '''
        return self.get_json(REQ_TASK_STATUS, args=[task_id])

    def task_reports(self, task_id):
        '''
        Retrieve a task completion report for a task.
        Only works for completed tasks.

        Parameters
        ----------
        task_id : str
            The id of the task to retrieve

        Reference
        ---------
        `GET /druid/indexer/v1/task/{taskId}/reports`

        See https://druid.apache.org/docs/latest/operations/api-reference.html#get-15
        '''
        return self.get_json(REQ_TASK_REPORTS, args=[task_id])

    def submit_task(self, payload):
        """
        Submit a task orsupervisor specs to the Overlord.
        
        Returns the taskId of the submitted task.

        Parameters
        ----------
        payload : object
            The task object. Serialized to JSON.

        Reference
        ---------
        `POST /druid/indexer/v1/task`

        See https://druid.apache.org/docs/latest/operations/api-reference.html#post-5
        """
        return self.post_json(REQ_POST_TASK, payload)

    def shut_down_task(self, task_id):
        """
        Shuts down a task.

        Parameters
        ----------
        task_id : str
            The id of the task to shut down
        
        Reference
        ---------
        `POST /druid/indexer/v1/task/{taskId}/shutdown`

        See https://druid.apache.org/docs/latest/operations/api-reference.html#post-5
        """
        return self.post_json(REQ_END_TASK, args=[task_id])

    def shut_down_tasks_for(self, table):
        """
        Shuts down all tasks for a table (data source).

        Parameters
        ----------
        table : str
            The name of the table (data source).
        
        Reference
        ---------
        `POST /druid/indexer/v1/datasources/{dataSource}/shutdownAllTasks`

        See https://druid.apache.org/docs/latest/operations/api-reference.html#post-5
        """
        return self.post_json(REQ_END_DS_TASKS, args=[table])

    #-------- Supervisors --------
    
    def supervisor_ids(self):
        """
        Returns a list of of the currently active supervisor ids.

        Reference
        ---------
        `GET /druid/indexer/v1/supervisor`

        See https://druid.apache.org/docs/latest/operations/api-reference.html#get-16
        """
        return self.get_json(REQ_SUPERVISORS_IDS)

    def supervisors(self, state=False):
        """
        Returns a list of objects of the currently active supervisors.

        Parameters
        ----------
        state : bool, default = False
            If True, returns a list of the currently active supervisors
            and their current state.

        Reference
        ---------
        * `GET /druid/indexer/v1/supervisor?full`
        * `GET /druid/indexer/v1/supervisor?state=true`

        See https://druid.apache.org/docs/latest/operations/api-reference.html#get-16
        """
        if state:
            params = {'state': 'true'}
        else:
            params = {'full': ''}
        return self.get_json(REQ_SUPERVISORS, params=params)

    def supervisor(self, id):
        """
        Returns the current spec for the supervisor with the provided ID.

        Parameters
        ----------
        id : str
            The supervisor ID.

        Reference
        ---------
        `GET /druid/indexer/v1/supervisor/<supervisorId>`

        See https://druid.apache.org/docs/latest/operations/api-reference.html#get-16
        """
        return self.get_json(REQ_SUPERVISOR, args=[id])

    def supervisor_status(self, id):
        """
        Returns the current status of the supervisor with the provided ID.

        Parameters
        ----------
        id : str
            The supervisor ID.

        Reference
        ---------
        `GET /druid/indexer/v1/supervisor/<supervisorId>/status`

        See https://druid.apache.org/docs/latest/operations/api-reference.html#get-16
        """
        return self.get_json(REQ_SUPERVISOR_STATUS, args=[id])

    def supervisor_history(self, id):
        """
        Returns an audit history of specs for all supervisors (current and past).

        Parameters
        ----------
        id : str
            The supervisor ID.

        Reference
        ---------
        `GET /druid/indexer/v1/supervisor/<supervisorId>`

        See https://druid.apache.org/docs/latest/operations/api-reference.html#get-16
        """
        return self.get_json(REQ_SUPERVISOR_HISTORY, args=[id])
