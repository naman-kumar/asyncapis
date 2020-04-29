import threading
result_available = threading.Event()


class MyQueue:
    _queue = []
    def put(self, x):
        self._queue.append(x)
        result_available.set()
    def get(self):
        while True:
            if self._queue:
                return self._queue.pop()
            else:
                result_available.clear()
                result_available.wait()

q = MyQueue()

class Test:
    @staticmethod
    def test(q):
        while True:
            v = q.get()
            if v == 'TERMINATE':
                return
            print(v)
    @staticmethod
    def stop():
        q.put('TERMINATE')


thread = threading.Thread(target=Test.test, args=(q,))

thread.start()

for v in range(10):
    q.put(v)
Test.stop()
thread.join()

