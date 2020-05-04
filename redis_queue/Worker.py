import os
import redis
import signal
import logging
import multiprocessing
from tasks import add
from Queue import SimpleQueue

DEFAULT_CONCURRENCY = 2
TERMINATE = False


def terminate_handler(signum, frame):
    global TERMINATE
    TERMINATE = True


class Worker(object):
    def __init__(self, queue_name, concurrency=None):
        self.redis = redis.Redis()
        self.queue = SimpleQueue(self.redis, queue_name)
        self.concurrency = concurrency if concurrency is not None else DEFAULT_CONCURRENCY
        self.logger = multiprocessing.get_logger()

    def process_one_task(self):
        proc = os.getpid()
        if self.queue.get_length() > 0:
            try:
                task = self.queue.dequeue()
                task.process_task()
            except Exception as e:
                self.logger.error(e)
            self.logger.info(f'Process {proc} completed successfully')
        else:
            # print('No Tasks to process.')
            pass

    def process_multiple_tasks(self):
        global TERMINATE
        processes = []
        while True:
            for _ in range(self.concurrency):
                if self.queue.get_length() > 0:
                    process = multiprocessing.Process(
                        target=self.process_one_task)
                    processes.append(process)
                    process.start()
            for process in processes:
                process.join()
            if TERMINATE:
                print('Exiting Gracefully')
                break


if __name__ == '__main__':
    multiprocessing.log_to_stderr(logging.INFO)
    signal.signal(signal.SIGINT, terminate_handler)
    worker = Worker('queue1')
    worker.process_multiple_tasks()
