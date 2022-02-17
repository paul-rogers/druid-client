from druid_client.client.service import Service, check_error

TALARIA_BASE = '/talaria/v1'
REQ_TASKS = TALARIA_BASE + '/tasks'
DELEGATE_BASE = TALARIA_BASE + '/delegate/async'
REQ_POST_TASK = DELEGATE_BASE + '/task'
REQ_LEADER = REQ_POST_TASK + '/{}'
REQ_GET_STATUS = REQ_LEADER + '/status'
REQ_GET_DETAILS = REQ_LEADER + '/details'

class Talaria(Service):

    def __init__(self, cluster_config, endpoint):
        Service.__init__(self, cluster_config, endpoint)

    def leaders(self, state=None, table=None, type=None, max=None):
        '''
        Retrieve list of tasks.
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
        return self.get_json(REQ_TASKS, params=params)

    def status(self, task_id):
        return self.get_json(REQ_GET_STATUS, args=[task_id])

    def details(self, task_id):
        return self.get_json(REQ_GET_DETAILS, args=[task_id])

    def results(self, task_id, offset=None, length=None):
        params = {}
        if offset is not None:
            params['offset'] = offset
        if length is not None:
            params['length'] = length
        return self.get_json(REQ_LEADER, args=[task_id], params=params)

    def cancel(self, task_id, andRemove=False):
        params = {'remove': andRemove}
        url = self.build_url(REQ_POST_TASK)
        r = self.session.delete(url, params=params)
        check_error(r)
        return r.json()
