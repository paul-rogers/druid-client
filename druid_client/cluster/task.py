import time
from ..client import consts
from ..client.error import DruidError

class Task:

    def __init__(self, cluster, id, spec=None):
        self.cluster = cluster
        self.overlord = cluster.ovrerlord()
        self._id = id
        self._spec = spec
        self.status_code = consts.RUNNING_STATE
        self._error = None

    def id(self):
        return self._id
    
    def status(self):
        self._status = self.overlord.task_status(self._id)
        status_obj = self._status.get('status', None)
        if status_obj is not None:
            self.status_code = status_obj.get('statusCode', None)
        return self._status

    def state(self):
        return self.status_code
    
    def done(self):
        return self.state() == consts.SUCCESS_STATE or self.state() == consts.FAILED_STATE

    def finished(self):
        return self.state() == consts.SUCCESS_STATE

    def join(self, poll_secs=1):
        if not self.done():
            self.status()
            while not self.done():
                time.sleep(poll_secs)
                self.status()
        return self.finished()

    def wait_done(self):
        if self.join():
            return
        status_obj = self._status.get('status', None)
        if status_obj is not None:
            self._error = status_obj.get('errorMsg', None)
        if self._error is None:
            self._error = "Unknown error"
        raise DruidError("Ingest failed: " + self._error)
   