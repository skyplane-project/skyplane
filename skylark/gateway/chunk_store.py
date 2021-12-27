from dataclasses import dataclass
from enum import Enum
from multiprocessing import Manager
from pathlib import Path
from typing import Dict, List, Optional

from loguru import logger

from skylark.gateway.wire_protocol_header import WireProtocolHeader


@dataclass
class Chunk:
    key: str  # human readable path where object is stored
    chunk_id: int
    file_offset_bytes: int
    chunk_length_bytes: int
    chunk_hash_sha256: str

    def to_wire_header(self, end_of_stream: bool = False):
        return WireProtocolHeader(chunk_id=self.chunk_id, chunk_len=self.chunk_length_bytes, end_of_stream=end_of_stream)


@dataclass
class ChunkRequestHop:
    hop_cloud_provider: str
    hop_cloud_region: str
    chunk_location_type: str  # enum of {"src_object_store", "dst_object_store", "relay"}

    # if chunk_location_type == "src_object_store":
    src_object_store_provider: str = None
    src_object_store_bucket: str = None

    # if chunk_location_type == "dst_object_store":
    dst_object_store_provider: str = None
    dst_object_store_bucket: str = None


@dataclass
class ChunkRequest:
    chunk: Chunk
    path: List[ChunkRequestHop]
    # todo: flags for compression, encryption, etc.


class ChunkState(Enum):
    REGISTERED = "registered"
    DOWNLOAD_IN_PROGRESS = "download_in_progress"
    READY_TO_UPLOAD = "ready_to_upload"
    UPLOAD_IN_PROGRESS = "upload_in_progress"
    UPLOAD_COMPLETE = "upload_complete"
    FAILED = "failed"


class ChunkStore:
    def __init__(self, chunk_dir: str = "/dev/shm/skylark/chunks"):
        self.chunk_dir = Path(chunk_dir)
        self.chunk_dir.mkdir(parents=True, exist_ok=True)

        # delete existing chunks
        for chunk_file in self.chunk_dir.glob("*.chunk"):
            logger.warning(f"Deleting existing chunk file {chunk_file}")
            chunk_file.unlink()

        # multiprocess-safe concurrent structures
        self.manager = Manager()
        self.chunks: Dict[int, Chunk] = self.manager.dict()
        self.chunk_status: Dict[int, ChunkState] = self.manager.dict()
        self.pending_chunk_requests: List[ChunkRequest] = self.manager.list()
        self.completed_chunk_requests: List[ChunkRequest] = self.manager.list()

    def get_chunk_file_path(self, chunk_id: int) -> Path:
        return self.chunk_dir / f"{chunk_id}.chunk"

    ###
    # ChunkState management
    ###
    def get_chunk_status(self, chunk_id: int) -> Optional[ChunkState]:
        return self.chunk_status[chunk_id] if chunk_id in self.chunk_status else None

    def set_chunk_status(self, chunk_id: int, new_status: ChunkState):
        self.chunk_status[chunk_id] = new_status

    def start_download(self, chunk_id: int):
        if self.get_chunk_status(chunk_id) == ChunkState.REGISTERED:
            self.set_chunk_status(chunk_id, ChunkState.DOWNLOAD_IN_PROGRESS)
        else:
            raise ValueError(f"Invalid transition start_download from {self.get_chunk_status(chunk_id)}")

    def finish_download(self, chunk_id: int, runtime_s: Optional[float] = None):
        # todo log runtime to statistics store
        if self.get_chunk_status(chunk_id) == ChunkState.DOWNLOAD_IN_PROGRESS:
            self.set_chunk_status(chunk_id, ChunkState.READY_TO_UPLOAD)
        else:
            raise ValueError(f"Invalid transition finish_download from {self.get_chunk_status(chunk_id)}")

    def start_upload(self, chunk_id: int):
        if self.get_chunk_status(chunk_id) == ChunkState.READY_TO_UPLOAD:
            self.set_chunk_status(chunk_id, ChunkState.UPLOAD_IN_PROGRESS)
        else:
            raise ValueError(f"Invalid transition start_upload from {self.get_chunk_status(chunk_id)}")

    def finish_upload(self, chunk_id: int, runtime_s: Optional[float] = None):
        # todo log runtime to statistics store
        if self.get_chunk_status(chunk_id) == ChunkState.UPLOAD_IN_PROGRESS:
            self.set_chunk_status(chunk_id, ChunkState.UPLOAD_COMPLETE)
        else:
            raise ValueError(f"Invalid transition finish_upload from {self.get_chunk_status(chunk_id)}")

    def fail(self, chunk_id: int):
        if self.get_chunk_status(chunk_id) != ChunkState.UPLOAD_COMPLETE:
            self.set_chunk_status(chunk_id, ChunkState.FAILED)
        else:
            raise ValueError(f"Invalid transition fail from {self.get_chunk_status(chunk_id)}")

    ###
    # Chunk management
    ###
    def get_chunk(self, chunk_id: int) -> Optional[Chunk]:
        return self.chunks[chunk_id] if chunk_id in self.chunks else None

    def add_chunk(self, chunk: Chunk):
        self.chunks[chunk.chunk_id] = chunk
        self.set_chunk_status(chunk.chunk_id, "registered")
