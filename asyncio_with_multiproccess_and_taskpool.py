import asyncio
import signal
from asyncio.queues import Queue
from dataclasses import dataclass
from multiprocessing import Pool
import random

import asyncssh

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


@dataclass
class ConsoleHealthCheck:
    server_name: str

    async def _run_ssh_command(self, cmd, username, password) -> tuple:
        try:
            async with await asyncio.wait_for(asyncssh.connect(self.server_name,
                                              known_hosts=None,
                                              username=username,
                                              password=password), timeout=5) as conn:
                try:
                    output = await asyncio.wait_for(conn.run(cmd, check=True), timeout=60)
                except asyncio.futures.TimeoutError:
                    result = (False, f'Timed out while running the ssh command {cmd} on {self.server_name}')
                    return result
                result = (True, output.stdout.strip())
        except asyncio.futures.TimeoutError:
            result = (False, f'Timed out while establishing the ssh connection to  {self.server_name}')
        except (OSError, asyncssh.Error) as e:
            msg = f'SSH connection failed to host {self.server_name}: {(e.__str__())}'
            result = (False, msg)
            print(msg)
        return result

    @staticmethod
    async def _run_cmd(cmd):
        """
        Send @count ping to @hostname with the given @timeout
        """

        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
            preexec_fn=(lambda: signal.signal(signal.SIGINT, signal.SIG_IGN)),
        )
        try:
            stdout = (await asyncio.wait_for(proc.communicate(), timeout=10))[0].decode(errors="ignore").strip()
            return_code = proc.returncode
        except asyncio.futures.TimeoutError:
            proc.terminate()
            stdout = f'Timed out while running the command {cmd}'
            return_code = -1
        return return_code, stdout

    async def _ping_check(self) -> bool:
        cmd = ('ping', self.server_name, '-c', str(random.randrange(5, 20)))
        exit_code, stdout = await self._run_cmd(cmd)
        if exit_code != 0:
            print(f'command: "{cmd}" did not exit with exit code 0. stdout: {stdout}')
        return exit_code == 0

    async def run_health_check(self):
        # ping_status = await self._ping_check()
        ssh_status = await self._run_ssh_command('hostname', username='', password='')
        return {
            'ping_status': 'ping_status',
            'ssh_status': ssh_status
        }


async def run(servers):
    pool = TaskPool(asyncio.get_event_loop(), 5)
    futures = [pool.submit(ConsoleHealthCheck(server).run_health_check()) for server in servers]
    await pool.join()
    for _future in futures:
        print(_future.result())


def multirun(msg):
    print(msg)
    # servers = ['localhost', 'localhost', 'localhost123', 'localhost', 'localhost']
    servers = ['localhost', 'localhost123']
    # servers = ['google.com', 'google.com', 'google123213.com', 'google.com', 'google.com']
    # servers = ['google.com']
    asyncio.run(run(servers))


def main():
    p = Pool(2)
    p.map(multirun, ['Process 1', 'Process 2'])
    p.close()
    p.join()


main()

