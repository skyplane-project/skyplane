from multiprocessing import Queue


class GatewayQueue:
    def __init__(self, maxsize=10000):
        self.q = Queue(maxsize)
        self.handles = []

    def register_handle(self, requester_handle):
        self.handles.append(requester_handle)

    def put(self, chunk_req):
        self.q.put(chunk_req)

    def put_nowait(self, chunk_req):
        self.q.put_nowait(chunk_req)

    def pop(self, requester_handle=None):
        self.q.get()

    def get_nowait(self, requester_handle=None):
        return self.q.get_nowait()

    def get_handles(self):
        return self.handles

    def size(self):
        return self.q.qsize()


class GatewayANDQueue(GatewayQueue):
    def __init__(self, maxsize=10000):
        self.q = {}
        self.maxsize = maxsize
        self.temp_q = Queue(maxsize)  # temporarily store value

    def register_handle(self, requester_handle):
        # create a queue per handle (operator)
        self.q[requester_handle] = GatewayQueue(self.maxsize)

    def get_handles(self):
        return list(self.q.keys())

    def get_handle_queue(self, requester_handle):
        return self.q[requester_handle]

    def put(self, chunk_req):
        # place chunk in all downstream operators queues
        for handle in self.q.keys():
            print("add queue", handle, self.q[handle].size())
            # self.q[handle].put(copy.deepcopy(chunk_req))
            self.q[handle].put(chunk_req)

    def put_nowait(self, chunk_req):
        raise ValueError("GatewayANDQueue cannot be the first queue in a pipeline")

    def pop(self, requester_handle):
        self.q[requester_handle].get()

    def get_nowait(self, requester_handle):
        return self.q[requester_handle].get_nowait()
