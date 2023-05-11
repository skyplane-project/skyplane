import subprocess
from datetime import datetime
from multiprocessing import Queue
from os import PathLike
from pathlib import Path
from typing import Dict, Optional

from skyplane.utils import logger

from skyplane.chunk import ChunkRequest, ChunkState
from skyplane.gateway.gateway_queue import GatewayQueue


class ChunkStore:
    def __init__(self, chunk_dir: PathLike):
        self.chunk_dir = Path(chunk_dir)
        self.chunk_dir.mkdir(parents=True, exist_ok=True)

        self.region_key_upload_id_mappings: Dict[str, str] = {}

        # delete existing chunks
        for chunk_file in self.chunk_dir.glob("*.chunk"):
            logger.warning(f"Deleting existing chunk file {chunk_file}")
            chunk_file.unlink()

        # queues of incoming chunk requests for each partition from gateway API (passed to operator graph)
        self.chunk_requests: Dict[str, GatewayQueue] = {}

        # queue of chunk status updates coming from operators (passed to gateway API)
        self.chunk_status_queue: Queue[Dict] = Queue()

    def set_upload_ids_map(self, maps: Dict[str, str]):
        self.region_key_upload_id_mappings.update(maps)

    def get_upload_ids_map(self):
        return self.region_key_upload_id_mappings

    def add_partition(self, partition_id: str):
        """Create a queue for this partition."""
        if partition_id in self.chunk_requests:
            raise ValueError(f"Partition {partition_id} already exists")
        self.chunk_requests[partition_id] = GatewayQueue()

    def add_partition(self, partition_id: str, queue: GatewayQueue):
        """Create a queue for this partition."""
        print("Adding partition", partition_id, queue)
        if partition_id in self.chunk_requests:
            raise ValueError(f"Partition {partition_id} already exists")
        self.chunk_requests[partition_id] = queue
        print(self.chunk_requests)

    def add_chunk_request(self, chunk_request: ChunkRequest, state: ChunkState = ChunkState.registered):
        """Enqueue new chunk request from Gateway API
        :param chunk_request: ChunkRequest object
        :param state: ChunkState enum (registered, in_progress, complete)

        :return: size of Gateway queue
        """
        if chunk_request.chunk.partition_id not in self.chunk_requests:
            raise ValueError(
                f"Partition {chunk_request.chunk.partition_id} does not exist in {self.chunk_requests} - was the gateway program loaded?"
            )
        try:
            self.chunk_requests[chunk_request.chunk.partition_id].put_nowait(chunk_request)
        except Exception as e:
            print("Error adding chunk", e)
            return self.chunk_requests[chunk_request.chunk.partition_id].size(), False

        self.log_chunk_state(chunk_request, state)
        return self.chunk_requests[chunk_request.chunk.partition_id].size(), True

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
        try:
            remaining_bytes = (
                int(subprocess.check_output(["df", "-k", "--output=avail", self.chunk_dir]).decode().strip().split()[-1]) * 1024
            )
            return remaining_bytes
        except Exception as e:
            raw_output = subprocess.check_output(["df", "-k", "--output=avail", self.chunk_dir]).decode().strip()
            print(f"{str(e)}: failed to parse {raw_output}")
            return 0

    def get_upload_id_map_path(self) -> Path:
        return self.chunk_dir / f"upload_id_map.json"

    def get_chunk_file_path(self, chunk_id: str) -> Path:
        return self.chunk_dir / f"{chunk_id}.chunk"
