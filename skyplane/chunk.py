import socket
from dataclasses import asdict, dataclass
from enum import Enum, auto
from functools import total_ordering

from typing import Dict, Optional


@dataclass
class Chunk:
    """A Chunk is a contiguous piece of a file (a file may be one or more chunks)."""

    src_key: str  # human readable path where object is stored
    dest_key: str  # human readable path where object is stored
    chunk_id: str
    chunk_length_bytes: int
    mime_type: Optional[str] = None
    partition: Optional[str] = None

    # checksum
    md5_hash: Optional[bytes] = None  # 128 bits

    # multi-part upload/download info
    file_offset_bytes: Optional[int] = None
    part_number: Optional[int] = None
    upload_id: Optional[str] = None

    def to_wire_header(self, n_chunks_left_on_socket: int, wire_length: int, is_compressed: bool = False):
        return WireProtocolHeader(
            chunk_id=self.chunk_id, data_len=wire_length, is_compressed=is_compressed, n_chunks_left_on_socket=n_chunks_left_on_socket
        )

    def as_dict(self):
        return asdict(self)

    @staticmethod
    def from_dict(d: Dict):
        return Chunk(**d)


# TODO: remove ChunkRequest abstraction (only need chunks)
@dataclass
class ChunkRequest:
    """A ChunkRequest stores all local state in the Gateway pertaining to a ChunkRequest."""

    chunk: Chunk
    src_region: str
    dst_region: str
    src_type: str  # enum of {"object_store", "random", "read_local"}
    dst_type: str  # enum of {"object_store", "save_local"}
    src_random_size_mb: Optional[int] = None
    src_object_store_bucket: Optional[str] = None
    dst_object_store_bucket: Optional[str] = None

    def __post_init__(self):
        if self.src_type == "object_store":
            assert self.src_object_store_bucket is not None
        elif self.src_type == "random":
            assert self.src_random_size_mb is not None
        if self.dst_type == "object_store":
            assert self.dst_object_store_bucket is not None

    def as_dict(self):
        dict_out = asdict(self)
        dict_out["chunk"] = self.chunk.as_dict()
        return dict_out

    @staticmethod
    def from_dict(in_dict: Dict):
        in_dict["chunk"] = Chunk.from_dict(in_dict["chunk"])
        return ChunkRequest(**in_dict)


@total_ordering
class ChunkState(Enum):
    registered = auto()
    download_queued = auto()
    download_in_progress = auto()
    downloaded = auto()
    upload_queued = auto()
    upload_in_progress = auto()
    upload_complete = auto()
    failed = auto()

    @staticmethod
    def from_str(s: str):
        return ChunkState[s.lower()]

    def __lt__(self, other):
        return self.value < other.value


@dataclass
class WireProtocolHeader:
    """Lightweight wire protocol header for chunk transfers along socket."""

    chunk_id: str  # 128bit UUID
    data_len: int  # long
    is_compressed: bool  # char
    n_chunks_left_on_socket: int  # long

    @staticmethod
    def magic_hex():
        return 0x534B595F4C41524B  # "SKY_LARK"

    @staticmethod
    def protocol_version():
        # v1 = base protocol
        # v2 = compression
        # v3 = uuid chunk_id
        return 3

    @staticmethod
    def length_bytes():
        # magic (8) + protocol_version (4) + chunk_id (16) + data_len (8) + is_compressed (1) + n_chunks_left_on_socket (8)
        return 8 + 4 + 16 + 8 + 1 + 8

    @staticmethod
    def from_bytes(data: bytes):
        assert len(data) == WireProtocolHeader.length_bytes(), f"{len(data)} != {WireProtocolHeader.length_bytes()}"
        magic = int.from_bytes(data[:8], byteorder="big")
        if magic != WireProtocolHeader.magic_hex():
            raise ValueError(f"Invalid magic number, got {magic:x} but expected {WireProtocolHeader.magic_hex():x}")
        version = int.from_bytes(data[8:12], byteorder="big")
        if version != WireProtocolHeader.protocol_version():
            raise ValueError(f"Invalid protocol version, got {version} but expected {WireProtocolHeader.protocol_version()}")
        chunk_id = data[12:28].hex()
        chunk_len = int.from_bytes(data[28:36], byteorder="big")
        is_compressed = bool(int.from_bytes(data[36:37], byteorder="big"))
        n_chunks_left_on_socket = int.from_bytes(data[37:45], byteorder="big")
        return WireProtocolHeader(
            chunk_id=chunk_id, data_len=chunk_len, is_compressed=is_compressed, n_chunks_left_on_socket=n_chunks_left_on_socket
        )

    def to_bytes(self):
        out_bytes = b""
        out_bytes += self.magic_hex().to_bytes(8, byteorder="big")
        out_bytes += self.protocol_version().to_bytes(4, byteorder="big")
        chunk_id_bytes = bytes.fromhex(self.chunk_id)
        assert len(chunk_id_bytes) == 16
        out_bytes += chunk_id_bytes
        out_bytes += self.data_len.to_bytes(8, byteorder="big")
        out_bytes += self.is_compressed.to_bytes(1, byteorder="big")
        out_bytes += self.n_chunks_left_on_socket.to_bytes(8, byteorder="big")
        assert len(out_bytes) == WireProtocolHeader.length_bytes(), f"{len(out_bytes)} != {WireProtocolHeader.length_bytes()}"
        return out_bytes

    @staticmethod
    def from_socket(sock: socket.socket):
        num_bytes = WireProtocolHeader.length_bytes()
        header_bytes = b""
        while len(header_bytes) < num_bytes:
            header_bytes += sock.recv(num_bytes - len(header_bytes))
        assert len(header_bytes) == num_bytes, f"{len(header_bytes)} != {num_bytes}"
        return WireProtocolHeader.from_bytes(header_bytes)

    def to_socket(self, sock: socket.socket):
        assert sock.sendall(self.to_bytes()) is None
