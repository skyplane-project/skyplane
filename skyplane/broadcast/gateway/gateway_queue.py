from multiprocessing import Queue


class GatewayQueue:
    def __init__(self, maxsize=0):
        self.q = Queue(maxsize)
        self.handles = []

    def register_handle(self, requester_handle):
        self.handles.append(requester_handle)

    def put(self, chunk_req):
        self.q.put(chunk_req)

    def pop(self, requester_handle=None):
        self.q.pop()

    def get_nowait(self, requester_handle=None):
        return self.q.get_nowait()

    def get_handles(self):
        return self.handles


class GatewayANDQueue(GatewayQueue):
    def __init__(self, maxsize=0):
        self.q = {}
        self.maxsize = maxsize

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
            # self.q[handle].put(copy.deepcopy(chunk_req))
            self.q[handle].put(chunk_req)

    def pop(self, requester_handle):
        self.q[requester_handle].pop()

    def get_nowait(self, requester_handle):
        return self.q[requester_handle].get_nowait()
