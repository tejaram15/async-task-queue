import redis
import random
from tasks import add
from Queue import SimpleQueue

NUM_OF_TASKS = 10

if __name__ == '__main__':
    re = redis.Redis()
    queue = SimpleQueue(re, 'queue1')
    for num in range(NUM_OF_TASKS):
        nums = random.sample(range(10, 30), 5)
        queue.enqueue(add, *nums)
    print(f'Enqueued {NUM_OF_TASKS} tasks.')
