import subprocess
from collections import defaultdict
from datetime import datetime
from multiprocessing import Queue
from os import PathLike
from pathlib import Path
from typing import Dict, Optional

from skyplane.broadcast.gateway.gateway_queue import GatewayQueue
from skyplane.chunk import ChunkRequest, ChunkState
from skyplane.utils import logger


class ChunkStore:
    def __init__(self, chunk_dir: PathLike):
        self.chunk_dir = Path(chunk_dir)
        self.chunk_dir.mkdir(parents=True, exist_ok=True)

        # delete existing chunks
        for chunk_file in self.chunk_dir.glob("*.chunk"):
            logger.warning(f"Deleting existing chunk file {chunk_file}")
            chunk_file.unlink()

        # multiprocess-safe concurrent structures
        # TODO: Remove this and use queues instead
        # self.manager = Manager()
        # self.chunk_requests: Dict[int, ChunkRequest] = self.manager.dict()  # type: ignore
        # self.chunk_status: Dict[int, ChunkState] = self.manager.dict()  # type: ignore
        # self.chunk_requests: Dict[int, ChunkRequest] = {}  # type: ignore

        # queues of incoming chunk requests for each partition from gateway API (passed to operator graph)
        self.chunk_requests: Dict[str, GatewayQueue] = {}

        # queue of chunk status updates coming from operators (passed to gateway API)
        self.chunk_status_queue: Queue[Dict] = Queue()

        self.chunk_completions = defaultdict(list)

    def add_partition(self, partition: str):
        if partition in self.chunk_requests:
            raise ValueError(f"Partition {partition} already exists")
        self.chunk_requests[partition] = GatewayQueue()

    def get_chunk_file_path(self, chunk_id: str) -> Path:
        return self.chunk_dir / f"{chunk_id:05d}.chunk"

    ###
    # ChunkState management
    ###
    def log_chunk_state(self, chunk_req: ChunkRequest, new_status: ChunkState, metadata: Optional[Dict] = None):
        rec = {
            "chunk_id": chunk_req.chunk.chunk_id,
            "partition": chunk_req.chunk.partition,
            "state": new_status.name,
            "time": str(datetime.utcnow().isoformat()),
        }

        if metadata is not None:
            rec.update(metadata)

        # add to status queue
        self.chunk_status_queue.put(rec)

    ###
    # Chunk management
    ###
    def add_chunk_request(self, chunk_request: ChunkRequest, state: ChunkState = ChunkState.registered):

        self.chunk_requests[chunk_request.chunk.partition].put(chunk_request)
        # TODO: consider adding to partition queues here?

        # update state
        self.log_chunk_state(chunk_request, state)

    # Memory space calculation
    def remaining_bytes(self):
        return int(subprocess.check_output(["df", "-k", "--output=avail", self.chunk_dir]).decode().strip().split()[-1]) * 1024
