import asyncio
import signal
from asyncio.queues import Queue
from dataclasses import dataclass
from threading import Thread
import asyncssh

TERMINATOR = object()

ASYNCIO_TASK_POOL = 5
SSH_CONNECTION_ESTABLISH_TIMEOUT = 5
SSH_COMMAND_RUN_TIMEOUT = 60
SUB_PROCESS_COMMAND_RUN_TIMEOUT = 60


class TaskPool(object):
    def __init__(self, loop, num_workers):
        self.loop = loop
        asyncio.set_event_loop(loop)
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
        asyncio.run_coroutine_threadsafe(self.tasks.put((future, task)), loop=self.loop)
        return future

    async def join(self):
        await asyncio.gather(*self.workers, loop=self.loop)

    def stop(self):
        for _ in self.workers:
            asyncio.run_coroutine_threadsafe(self.tasks.put((None, TERMINATOR)), loop=self.loop)


@dataclass
class ConsoleHealthCheck:
    server_name: str

    async def _run_ssh_command(self, cmd, username, password) -> tuple:
        try:
            async with await asyncio.wait_for(asyncssh.connect(self.server_name,
                                                               known_hosts=None,
                                                               username=username,
                                                               password=password),
                                              timeout=SSH_CONNECTION_ESTABLISH_TIMEOUT) as conn:
                try:
                    output = await asyncio.wait_for(conn.run(cmd, check=True), timeout=SSH_COMMAND_RUN_TIMEOUT)
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
            stdout = (await asyncio.wait_for(
                proc.communicate(), timeout=SUB_PROCESS_COMMAND_RUN_TIMEOUT))[0].decode(errors="ignore").strip()
            return_code = proc.returncode
        except asyncio.futures.TimeoutError:
            proc.terminate()
            stdout = f'Timed out while running the command {cmd}'
            return_code = -1
        return return_code, stdout

    async def _ping_check(self) -> bool:
        cmd = ('ping', self.server_name, '-c', '4')
        exit_code, stdout = await self._run_cmd(cmd)
        if exit_code != 0:
            print(f'command: "{cmd}" did not exit with exit code 0. stdout: {stdout}')
        return exit_code == 0

    async def run_health_check(self,):
        ping_status = await self._ping_check()
        ssh_status = await self._run_ssh_command('hostname', username='', password='')
        print({
            'ping_status': ping_status,
            'ssh_status': ssh_status})
        return {
            'ping_status': ping_status,
            'ssh_status': ssh_status}


def main():
    servers = ['www.google.com']*5
    event_loop = asyncio.get_event_loop()
    asyncio.get_child_watcher()
    pool = TaskPool(event_loop, ASYNCIO_TASK_POOL)
    thread = Thread(target=event_loop.run_until_complete, args=(pool.join(),))
    thread.start()
    for server in servers:
        pool.submit(ConsoleHealthCheck(server).run_health_check())


if __name__ == '__main__':
    main()

