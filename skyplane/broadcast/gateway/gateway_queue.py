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

    def get_nowait(self, requester_handle):
        return self.q.get_nowait()

    def get_handles(self):
        return self.handles


class GatewayORQueue(GatewayQueue):
    def __init__(self, maxsize=0):
        super().__init__(maxsize)


class GatewayANDQueue(GatewayQueue):
    def __init__(self, maxsize=0):
        super().__init__(maxsize)
        self.q = {}
        self.maxsize = maxsize

    def register_handle(self, requester_handle):
        # create a queue per handle (operator)
        self.q[requester_handle] = Queue(self.maxsize)

    def get_handles(self):
        return list(self.q.keys())

    def put(self, chunk_req):
        # place chunk in all downstream operators queues
        for handle in self.q.keys():
            # self.q[handle].put(copy.deepcopy(chunk_req))
            self.q[handle].put(chunk_req)

    def pop(self, requester_handle):
        self.q[requester_handle].pop()

    def get_nowait(self, requester_handle):
        return self.q[requester_handle].get_nowait()


# class GatewayChunkStoreCompleteQueue:
#
#    """ Special queue that sends put events to the chunk store.
#    Cannot pop items from this queue
#    """
#
#    def __init__(self, chunk_store: ChunkStore, maxsize=0):
#        self.q = Queue(maxsize)
#        self.chunk_store = chunk_store
#
#    def put(self, chunk_req, requester_handle: str, metadata: dict = None):
#        # send message to chunk store
#        if metadata:
#            metadata["operator_handle"] = requester_handle
#        else:
#            metadata = {"operator_handle": requester_handle}
#
#        self.chunk_store.set_chunk_state(chunk_req, "upload_complete", metadata)
#
#    def pop(self, requester_handle):
#        raise ValueError("Cannot pop from a output queue for a terminal operator")
#
# class GatewayDoneQueue:
#    def __init__(self, chunk_store: ChunkStore, maxsize=0):
#        #self.q = Queue(maxsize)
#        self.manager = Manager()
#        self.chunk_counts = self.manager.dict()
#        self.handles = []
#        self.chunk_store = chunk_store
#
#    def register_handle(self, handle):
#        self.handles.append(handle)
#
#    def get_handles(self):
#        return self.handles
#
#    def put(self, chunk_req):
#        """
#        Track number of times chunk is completed.
#        If all handles have completed the chunk, then place is
#        in the done queue.
#        """
#        if chunk_req.chunk.chunk_id in self.chunk_counts:
#            self.chunk_counts[chunk_req.chunk.chunk_id] += 1
#        else:
#            self.chunk_counts[chunk_req.chunk.chunk_id] = 1
#
#        # place is completed queue
#        if self.chunk_counts[chunk_req.chunk.chunk_id] == len(self.handles):
#            print("Placing chunk in done queue and removing", chunk_req.chunk.chunk_id)
#            self.q.put(chunk_req)
#            self.chunk_store.set_chunk_status("completed")
#            del self.chunk_counts[chunk_req.chunk.chunk_id]
#        else:
#            print(
#                "not enough chunk completitions", chunk_req.chunk.chunk_id, self.chunk_counts[chunk_req.chunk.chunk_id], len(self.handles)
#            )
