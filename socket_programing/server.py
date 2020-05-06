import asyncio, socket, random
from threading import Thread
import nest_asyncio

from async_test import TaskPool, ASYNCIO_TASK_POOL, ConsoleHealthCheck


# async def echo(client, request, new_event_loop):
#     asyncio.set_event_loop(new_event_loop)
#     await asyncio.sleep(random.randrange(5, 20))
#     print("After sleep")
#     client.sendall(request.encode('utf8'))
# 
# 
# async def mytestrun(a, loop):
#     await asyncio.ensure_future(a, loop=loop)
# 
# 
# async def handle_client(client):
#     try:
#         new_event_loop = asyncio.get_event_loop()
#         nest_asyncio.apply()
#         asyncio.get_child_watcher()
#         pool = TaskPool(new_event_loop, ASYNCIO_TASK_POOL)
#         thread = Thread(target=new_event_loop.run_until_complete, args=(pool.join(),))
#         thread.start()
#         while True:
#             print('Ready for input')
#             request = (await loop.sock_recv(client, 255)).decode('utf8')
#             print(f'request => {request}')
#             if (not request.strip()) or (request.lower().strip() == 'quit'):
#                 break
#             pool.submit(ConsoleHealthCheck('server').run_health_check(client, request))
#     finally:
#         print("Closing connection")
#         client.close()
# 
# 
# async def run_server():
#     while True:
#         client, _ = await loop.sock_accept(server)
#         loop.create_task(handle_client(client))
# 
# server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
# server.bind(('192.168.1.5', 5000))
# server.listen(2)
# server.setblocking(False)
# 
# loop = asyncio.get_event_loop()
# loop.run_until_complete(run_server())

# ######################################### Another way of implementation ############################################

async def handle_client(reader, writer):
    new_event_loop = asyncio.get_event_loop()
    nest_asyncio.apply()
    asyncio.get_child_watcher()
    pool = TaskPool(new_event_loop, ASYNCIO_TASK_POOL)
    thread = Thread(target=new_event_loop.run_until_complete, args=(pool.join(),))
    thread.start()
    while True:
        print('Ready for input')
        req = (await reader.read(255)).decode('utf8')
        if (not req.strip()) or req.lower().strip() == 'quit':
            break
        print(f'req => {req}')
        sleep_time = random.randrange(1, 10)
        print(f'sleep_time => {sleep_time}')
        pool.submit(ConsoleHealthCheck('server').run_health_check(writer, req))
    writer.close()
    pool.stop()
    thread.join()

loop = asyncio.get_event_loop()
loop.create_task(asyncio.start_server(handle_client, '192.168.1.4', 5000))
loop.run_forever()

