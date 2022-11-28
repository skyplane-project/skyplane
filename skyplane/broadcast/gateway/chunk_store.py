import subprocess
from datetime import datetime
from multiprocessing import Queue
from os import PathLike
from pathlib import Path
from typing import Dict, Optional

from skyplane.utils import logger

from skyplane.broadcast.chunk import ChunkRequest, ChunkState
from skyplane.broadcast.gateway.gateway_queue import GatewayQueue


class ChunkStore:
    def __init__(self, chunk_dir: PathLike):
        self.chunk_dir = Path(chunk_dir)
        self.chunk_dir.mkdir(parents=True, exist_ok=True)

        # delete existing chunks
        for chunk_file in self.chunk_dir.glob("*.chunk"):
            logger.warning(f"Deleting existing chunk file {chunk_file}")
            chunk_file.unlink()

        # queues of incoming chunk requests for each partition from gateway API (passed to operator graph)
        self.chunk_requests: Dict[str, GatewayQueue] = {}

        # queue of chunk status updates coming from operators (passed to gateway API)
        self.chunk_status_queue: Queue[Dict] = Queue()

    def add_partition(self, partition_id: str):
        """Create a queue for this partition."""
        if partition_id in self.chunk_requests:
            raise ValueError(f"Partition {partition_id} already exists")
        self.chunk_requests[partition_id] = GatewayQueue()

    def add_chunk_request(self, chunk_request: ChunkRequest, state: ChunkState = ChunkState.registered):
        """Enqueue new chunk request from Gateway API"""
        if chunk_request.chunk.partition_id not in self.chunk_requests:
            raise ValueError(f"Partition {chunk_request.chunk.partition_id} does not exist - was the gateway program loaded?")
        self.chunk_requests[chunk_request.chunk.partition_id].put(chunk_request)
        self.log_chunk_state(chunk_request, state)

    def log_chunk_state(
        self,
        chunk_req: ChunkRequest,
        new_status: ChunkState,
        worker_id: Optional[int] = None,
        operator_handle: Optional[str] = None,
        metadata: Optional[Dict] = None,
    ):
        """Add dict containing chunk status change (coming from operators) to queue"""
        rec = {
            "chunk_id": chunk_req.chunk.chunk_id,
            "partition": chunk_req.chunk.partition_id,
            "state": new_status.name,
            "time": str(datetime.utcnow().isoformat()),
            "handle": operator_handle,
            "worker_id": worker_id,
        }
        if metadata is not None:
            rec.update(metadata)
        self.chunk_status_queue.put(rec)

    # Memory space calculation
    def remaining_bytes(self):
        return int(subprocess.check_output(["df", "-k", "--output=avail", self.chunk_dir]).decode().strip().split()[-1]) * 1024

    def get_chunk_file_path(self, chunk_id: str) -> Path:
        return self.chunk_dir / f"{chunk_id}.chunk"
