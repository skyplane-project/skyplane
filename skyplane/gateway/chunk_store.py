import subprocess
from datetime import datetime
from multiprocessing import Manager, Queue
from os import PathLike
from pathlib import Path
from queue import Empty
from typing import Dict, List, Optional

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
        self.manager = Manager()
        self.chunk_requests: Dict[int, ChunkRequest] = self.manager.dict()  # type: ignore
        self.chunk_status: Dict[int, ChunkState] = self.manager.dict()  # type: ignore

        # state log
        self.chunk_status_queue: Queue[Dict] = Queue()

        # metric log
        self.sender_compressed_sizes: Dict[int, float] = self.manager.dict()  # type: ignore

    def get_chunk_file_path(self, chunk_id: int) -> Path:
        return self.chunk_dir / f"{chunk_id:05d}.chunk"

    ###
    # ChunkState management
    ###
    def get_chunk_state(self, chunk_id: int) -> Optional[ChunkState]:
        return self.chunk_status[chunk_id] if chunk_id in self.chunk_status else None

    def set_chunk_state(self, chunk_id: int, new_status: ChunkState, log_metadata: Optional[Dict] = None):
        self.chunk_status[chunk_id] = new_status
        rec = {"chunk_id": chunk_id, "state": new_status.name, "time": str(datetime.utcnow().isoformat())}
        if log_metadata is not None:
            rec.update(log_metadata)
        self.chunk_status_queue.put(rec)

    def drain_chunk_status_queue(self) -> List[Dict]:
        out_events = []
        while True:
            try:
                elem = self.chunk_status_queue.get_nowait()
                out_events.append(elem)
            except Empty:
                break
        return out_events

    def state_queue_download(self, chunk_id: int):
        state = self.get_chunk_state(chunk_id)
        if state in [ChunkState.registered, ChunkState.download_queued]:
            self.set_chunk_state(chunk_id, ChunkState.download_queued)
        else:
            raise ValueError(f"Invalid transition queue_download from {state} (id={chunk_id})")

    def state_start_download(self, chunk_id: int, receiver_id: Optional[str] = None):
        state = self.get_chunk_state(chunk_id)
        if state in [ChunkState.download_queued, ChunkState.download_in_progress]:
            self.set_chunk_state(chunk_id, ChunkState.download_in_progress, {"receiver_id": receiver_id})
        else:
            raise ValueError(f"Invalid transition start_download from {state}")

    def state_finish_download(self, chunk_id: int, receiver_id: Optional[str] = None):
        state = self.get_chunk_state(chunk_id)
        if state in [ChunkState.download_in_progress, ChunkState.downloaded]:
            self.set_chunk_state(chunk_id, ChunkState.downloaded, {"receiver_id": receiver_id})
        else:
            raise ValueError(f"Invalid transition finish_download from {state} (id={chunk_id})")

    def state_queue_upload(self, chunk_id: int):
        state = self.get_chunk_state(chunk_id)
        if state in [ChunkState.downloaded, ChunkState.upload_queued]:
            self.set_chunk_state(chunk_id, ChunkState.upload_queued)
        else:
            raise ValueError(f"Invalid transition upload_queued from {state} (id={chunk_id})")

    def state_start_upload(self, chunk_id: int, sender_id: Optional[str] = None):
        state = self.get_chunk_state(chunk_id)
        if state in [ChunkState.upload_queued, ChunkState.upload_in_progress]:
            self.set_chunk_state(chunk_id, ChunkState.upload_in_progress, {"sender_id": sender_id})
        else:
            raise ValueError(f"Invalid transition start_upload from {state} (id={chunk_id})")

    def state_finish_upload(self, chunk_id: int, sender_id: Optional[str] = None, compressed_size_bytes: Optional[int] = None):
        state = self.get_chunk_state(chunk_id)
        if state in [ChunkState.upload_in_progress, ChunkState.upload_complete]:
            self.set_chunk_state(chunk_id, ChunkState.upload_complete, {"sender_id": sender_id})
            if compressed_size_bytes is not None:
                self.sender_compressed_sizes[chunk_id] = compressed_size_bytes
        else:
            raise ValueError(f"Invalid transition finish_upload from {state} (id={chunk_id})")

    def state_fail(self, chunk_id: int):
        if self.get_chunk_state(chunk_id) != ChunkState.upload_complete:
            self.set_chunk_state(chunk_id, ChunkState.failed)
        else:
            raise ValueError(f"Invalid transition fail from {self.get_chunk_state(chunk_id)} (id={chunk_id})")

    ###
    # Chunk management
    ###
    def get_chunk_requests(self, status: Optional[ChunkState] = None) -> List[ChunkRequest]:
        if status is None:
            return list(self.chunk_requests.values())
        else:
            return [req for i, req in self.chunk_requests.items() if self.get_chunk_state(i) == status]

    def get_chunk_request(self, chunk_id: int) -> ChunkRequest:
        if chunk_id not in self.chunk_requests:
            raise ValueError(f"ChunkRequest {chunk_id} not found")
        return self.chunk_requests[chunk_id]

    def add_chunk_request(self, chunk_request: ChunkRequest, state=ChunkState.registered):
        self.set_chunk_state(chunk_request.chunk.chunk_id, state)
        self.chunk_requests[chunk_request.chunk.chunk_id] = chunk_request

    def update_chunk_checksum(self, chunk_id: int, checksum: Optional[bytes]):
        cr = self.chunk_requests[chunk_id]
        cr.chunk.checksum = checksum
        self.chunk_requests[chunk_id] = cr

    # Memory space calculation
    def remaining_bytes(self):
        return int(subprocess.check_output(["df", "-k", "--output=avail", self.chunk_dir]).decode().strip().split()[-1]) * 1024
