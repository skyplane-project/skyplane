from dataclasses import asdict, dataclass
from enum import Enum, auto
from multiprocessing import Manager
from os import PathLike
from pathlib import Path
from typing import Dict, List, Optional

from loguru import logger

from skylark.gateway.wire_protocol_header import WireProtocolHeader


@dataclass
class Chunk:
    # todo merge with WireProtocolHeader
    key: str  # human readable path where object is stored
    chunk_id: int
    file_offset_bytes: int
    chunk_length_bytes: int
    chunk_hash_sha256: str

    def to_wire_header(self, end_of_stream: bool = False):
        return WireProtocolHeader(chunk_id=self.chunk_id, chunk_len=self.chunk_length_bytes, end_of_stream=end_of_stream)

    def as_dict(self):
        return asdict(self)

    @staticmethod
    def from_dict(d: Dict):
        return Chunk(**d)


@dataclass
class ChunkRequestHop:
    hop_cloud_region: str  # format is provider:region
    hop_ip_address: str
    chunk_location_type: str  # enum of {"src_object_store", "dst_object_store", "relay"}

    # if chunk_location_type == "src_object_store":
    src_object_store_region: str = None  # format is provider:region
    src_object_store_bucket: str = None

    # if chunk_location_type == "dst_object_store":
    dst_object_store_region: str = None  # format is provider:region
    dst_object_store_bucket: str = None

    def as_dict(self):
        return asdict(self)

    @staticmethod
    def from_dict(dict: Dict):
        return ChunkRequestHop(**dict)


@dataclass
class ChunkRequest:
    chunk: Chunk
    path: List[ChunkRequestHop]

    # todo: flags for compression, encryption, logging api, etc.

    def as_dict(self):
        out = {}
        out["chunk"] = self.chunk.as_dict()
        out["path"] = [hop.as_dict() for hop in self.path]
        return out

    @staticmethod
    def from_dict(in_dict: Dict):
        return ChunkRequest(chunk=Chunk.from_dict(in_dict["chunk"]), path=[ChunkRequestHop.from_dict(hop) for hop in in_dict["path"]])


class ChunkState(Enum):
    registered = auto()
    download_in_progress = auto()
    downloaded = auto()
    upload_in_progress = auto()
    upload_complete = auto()
    failed = auto()

    @staticmethod
    def from_str(s: str):
        return ChunkState[s.lower()]


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
        self.chunk_requests: Dict[int, Chunk] = self.manager.dict()
        self.chunk_status: Dict[int, ChunkState] = self.manager.dict()

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
        state = self.get_chunk_status(chunk_id)
        if state in [ChunkState.registered, ChunkState.download_in_progress]:
            self.set_chunk_status(chunk_id, ChunkState.download_in_progress)
        else:
            raise ValueError(f"Invalid transition start_download from {self.get_chunk_status(chunk_id)}")

    def finish_download(self, chunk_id: int, runtime_s: Optional[float] = None):
        # todo log runtime to statistics store
        state = self.get_chunk_status(chunk_id)
        if state in [ChunkState.download_in_progress, ChunkState.downloaded]:
            self.set_chunk_status(chunk_id, ChunkState.downloaded)
        else:
            raise ValueError(f"Invalid transition finish_download from {self.get_chunk_status(chunk_id)}")

    def start_upload(self, chunk_id: int):
        state = self.get_chunk_status(chunk_id)
        if state in [ChunkState.downloaded, ChunkState.upload_in_progress]:
            self.set_chunk_status(chunk_id, ChunkState.upload_in_progress)
        else:
            raise ValueError(f"Invalid transition start_upload from {self.get_chunk_status(chunk_id)}")

    def finish_upload(self, chunk_id: int, runtime_s: Optional[float] = None):
        # todo log runtime to statistics store
        state = self.get_chunk_status(chunk_id)
        if state in [ChunkState.upload_in_progress, ChunkState.upload_complete]:
            self.set_chunk_status(chunk_id, ChunkState.upload_complete)
        else:
            raise ValueError(f"Invalid transition finish_upload from {self.get_chunk_status(chunk_id)}")

    def fail(self, chunk_id: int):
        if self.get_chunk_status(chunk_id) != ChunkState.upload_complete:
            self.set_chunk_status(chunk_id, ChunkState.failed)
        else:
            raise ValueError(f"Invalid transition fail from {self.get_chunk_status(chunk_id)}")

    ###
    # Chunk management
    ###
    def get_chunk_requests(self, status: Optional[ChunkState] = None) -> List[ChunkRequest]:
        if status is None:
            return list(self.chunk_requests.values())
        else:
            return [req for i, req in self.chunk_requests.items() if self.get_chunk_status(i) == status]

    def get_chunk_request(self, chunk_id: int) -> Optional[ChunkRequest]:
        return self.chunk_requests[chunk_id] if chunk_id in self.chunk_requests else None

    def add_chunk_request(self, chunk_request: ChunkRequest, state=ChunkState.registered):
        logger.debug(f"Adding chunk request {chunk_request.chunk.chunk_id}")
        self.set_chunk_status(chunk_request.chunk.chunk_id, state)
        self.chunk_requests[chunk_request.chunk.chunk_id] = chunk_request

    def pop_chunk_request_path(self, chunk_id: int) -> Optional[ChunkRequestHop]:
        if chunk_id in self.chunk_requests:
            chunk_request = self.chunk_requests[chunk_id]
            if len(chunk_request.path) > 0:
                result = chunk_request.path.pop(0)
                self.chunk_requests[chunk_id] = chunk_request
                return result
        return None
