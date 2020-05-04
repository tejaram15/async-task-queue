import random
from tasks import add
from Queue import AzureQueue

NUM_OF_TASKS = 10

if __name__ == '__main__':
    queue = AzureQueue('samplequeue')
    for num in range(NUM_OF_TASKS):
        nums = random.sample(range(10, 30), 5)
        queue.enqueue(add, *nums)
    print(f'Enqueued {NUM_OF_TASKS} tasks.')
