import random
import asyncio
from asyncio.queues import Queue
from multiprocessing import Pool

TERMINATOR = object()


class TaskPool(object):
    def __init__(self, loop, num_workers):
        self.loop = loop
        self.tasks = Queue(loop=self.loop)
        self.workers = []
        for _ in range(num_workers):
            worker = asyncio.ensure_future(self.worker(), loop=self.loop)
            self.workers.append(worker)

    async def worker(self):
        while True:
            future, task = await self.tasks.get()
            if task is TERMINATOR:
                break
            result = await asyncio.wait_for(task, None, loop=self.loop)
            future.set_result(result)

    def submit(self, task):
        future = asyncio.Future(loop=self.loop)
        self.tasks.put_nowait((future, task))
        return future

    async def join(self):
        for _ in self.workers:
            self.tasks.put_nowait((None, TERMINATOR))
        await asyncio.gather(*self.workers, loop=self.loop)


async def do_stuff(i):
    # await asyncio.sleep(random.randrange(1, 10))
    await asyncio.sleep(1)
    print(f"Finished => {i}")


async def myrun():
    pool = TaskPool(asyncio.get_event_loop(), 4)
    futures = [pool.submit(do_stuff(_)) for _ in range(10)]
    await pool.join()


def multirun(msg):
    print(msg)
    asyncio.run(myrun())

p = Pool(5)

p.map(multirun, ['Process 1', 'Process 2', 'Process 3', 'Process 4', 'Process 5'])

p.close()
p.join()





