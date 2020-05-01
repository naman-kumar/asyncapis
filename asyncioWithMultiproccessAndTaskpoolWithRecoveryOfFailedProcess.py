import concurrent
import os
import sqlite3
import asyncio
import signal
from asyncio.queues import Queue
from dataclasses import dataclass
import aiosqlite
import random
import pickle
import shutil
import time
from concurrent.futures import ProcessPoolExecutor

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
    db_path: str
    db_record_id: int
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

    async def _mark_finished(self):
        async with aiosqlite.connect(self.db_path) as db:
            try:
                await db.execute(f"update process set is_processed=True where ID={self.db_record_id}")
                await db.commit()
            except Exception as e:
                await db.rollback()
                raise sqlite3.OperationalError(f'{e.__str__()}, db_path={self.db_path}')

    async def _processing(self, sleep_time):
        await asyncio.sleep(sleep_time)
        return self.server_name

    async def run_health_check(self):
        # ping_status = await self._ping_check()
        # ssh_status = await self._run_ssh_command('hostname', username='', password='')
        sleep_time = random.randrange(1, 20)
        process_done = await self._processing(sleep_time)
        await self._mark_finished()
        # print(process_done)
        print({
            'ping_status': 'ping_status',
            'ssh_status': 'ssh_status',
            'process_done': process_done,
            'sleep_time': sleep_time
        })


async def asynio_run(data):
    pool = TaskPool(asyncio.get_event_loop(), 1000)
    futures = []
    for record in data:
        _id = record[0]
        message = record[1]
        db_path = record[2]
        futures.append(pool.submit(ConsoleHealthCheck(server_name=message,
                                                      db_path=db_path,
                                                      db_record_id=_id).run_health_check()))
    await pool.join()
    # for _future in futures:
    #     print(_future.result())


def run(db_path):
    print(f"Starting the process {os.getpid()}")
    print(f"Argument => {db_path}")
    data = []
    conn = sqlite3.connect(db_path)
    result = conn.execute('select ID, message from process where is_processed=0')
    for record in result:
        _id = record[0]
        message = pickle.loads(record[1])
        data.append((_id, message, db_path))
    asyncio.run(asynio_run(data))


def create_db_and_table(process_dir, process_no):
    if not os.path.isdir(process_dir):
        os.mkdir(process_dir)
    conn = sqlite3.connect(f'{process_dir}/process_{process_no}.db')
    sql = '''create table if not exists process(
        ID INTEGER PRIMARY KEY AUTOINCREMENT,
        message BLOB,
        is_processed BOOL DEFAULT FALSE );'''
    conn.execute(sql)


def save_to_db(db_path: str, messages: list) -> None:
    conn = sqlite3.connect(db_path)
    sql = "insert into process (message) VALUES (?);"
    parms = [(pickle.dumps(message, pickle.HIGHEST_PROTOCOL),)for message in messages]
    conn.executemany(sql, parms)
    conn.commit()
    conn.close()


def main(process_count: int, messages: list, fresh_restart=False):
    proccess_dir = f"{os.path.expanduser('~')}/.custom_process"
    if fresh_restart:
        shutil.rmtree(proccess_dir)
    sql_dbs = []
    messages_count = len(messages)
    mod = messages_count % process_count
    devided_msg_count = int((messages_count-mod)/process_count)
    start = 0
    end = devided_msg_count+mod
    for process_no in range(process_count):
        process_messages = messages[start:end]
        create_db_and_table(proccess_dir, process_no)
        sql_db = f'{proccess_dir}/process_{process_no}.db'
        sql_dbs.append(sql_db)
        save_to_db(sql_db, process_messages)
        start = end
        end = end + devided_msg_count
    while True:
        with ProcessPoolExecutor(max_workers=process_count) as executor:
            try:
                results = list(executor.map(run, [sql_db for sql_db in sql_dbs]))
                return results
            except (concurrent.futures.process.BrokenProcessPool, sqlite3.OperationalError) as e:
                print(e.__str__())
                print('Got exception')


process_count = 5
messages = [v for v in range(50000)]
old = time.time()
main(process_count, messages, fresh_restart=True)
print(f'Time taken {time.time() - old}')


