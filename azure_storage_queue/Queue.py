import os
import uuid
import pickle
from tasks import add
from azure.storage.queue import QueueService, QueueMessageFormat


class SimpleTask(object):
    def __init__(self, func, *args, **kwargs):
        self.id = str(uuid.uuid4())
        self.func = func
        self.args = args
        self.kwargs = kwargs

    def process_task(self):
        self.func(*self.args, **self.kwargs)


class AzureQueue(object):
    def __init__(self, queue_name):
        self.conn = QueueService(account_name=os.getenv(
            'AZURE_ACCOUNT_NAME'), account_key=os.getenv('AZURE_ACCOUNT_KEY'))
        self.queue_name = queue_name
        self.conn.create_queue(queue_name)
        self.conn.encode_function = QueueMessageFormat.binary_base64encode
        self.conn.decode_function = QueueMessageFormat.binary_base64decode

    def enqueue(self, func, *args, **kwargs):
        task = SimpleTask(func, *args, **kwargs)
        serialized_task = pickle.dumps(task, protocol=pickle.HIGHEST_PROTOCOL)
        self.conn.put_message(self.queue_name, serialized_task)
        return task.id

    def dequeue(self):
        messages = self.conn.get_messages(self.queue_name)
        if len(messages) == 1:
            serialized_task = messages[0]
            task = pickle.loads(serialized_task.content)
            self.conn.delete_message(
                self.queue_name, serialized_task.id, serialized_task.pop_receipt)
            return task

    def get_length(self):
        metadata = self.conn.get_queue_metadata(self.queue_name)
        return metadata.approximate_message_count
