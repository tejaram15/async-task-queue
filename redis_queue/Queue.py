import uuid
import pickle
from tasks import add


class SimpleTask(object):
    def __init__(self, func, *args, **kwargs):
        self.id = str(uuid.uuid4())
        self.func = func
        self.args = args
        self.kwargs = kwargs

    def process_task(self):
        self.func(*self.args, **self.kwargs)


class SimpleQueue(object):
    def __init__(self, conn, name):
        self.conn = conn
        self.name = name

    def enqueue(self, func, *args, **kwargs):
        task = SimpleTask(func, *args, **kwargs)
        serialized_task = pickle.dumps(task, protocol=pickle.HIGHEST_PROTOCOL)
        self.conn.lpush(self.name, serialized_task)
        return task.id

    def dequeue(self):
        _, serialized_task = self.conn.brpop(self.name)
        task = pickle.loads(serialized_task)
        # TODO: Check if this is necessary
        # task.process_task()
        return task

    def get_length(self):
        return self.conn.llen(self.name)
