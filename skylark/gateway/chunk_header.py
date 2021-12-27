from dataclasses import dataclass
import socket


@dataclass
class ChunkHeader:
    # sent over wire in order:
    #   magic
    #   chunk_id
    #   chunk_size_bytes
    #   chunk_offset_bytes
    #   end_of_stream
    #   chunk_hash_sha256
    chunk_id: int  # unsigned long
    chunk_size_bytes: int  # unsigned long
    chunk_offset_bytes: int  # unsigned long
    chunk_hash_sha256: str  # 64-byte checksum
    end_of_stream: bool = False  # false by default, but true if this is the last chunk

    @staticmethod
    def magic_hex():
        return 0x534B595F4C41524B  # "SKY_LARK"

    @staticmethod
    def length_bytes():
        # magic (8) + chunk_id (8) + chunk_size_bytes (8) + chunk_offset_bytes (8) + end_of_stream (1) + chunk_hash_sha256 (64)
        return 8 + 8 + 8 + 8 + 1 + 64

    @staticmethod
    def from_bytes(data: bytes):
        assert len(data) == ChunkHeader.length_bytes()
        magic = int.from_bytes(data[:8], byteorder="big")
        if magic != ChunkHeader.magic_hex():
            raise ValueError("Invalid magic number")
        chunk_id = int.from_bytes(data[8:16], byteorder="big")
        chunk_size_bytes = int.from_bytes(data[16:24], byteorder="big")
        chunk_offset_bytes = int.from_bytes(data[24:32], byteorder="big")
        chunk_end_of_stream = bool(data[32])
        chunk_hash_sha256 = data[33:].decode("utf-8")
        return ChunkHeader(
            chunk_id=chunk_id,
            chunk_size_bytes=chunk_size_bytes,
            chunk_offset_bytes=chunk_offset_bytes,
            chunk_hash_sha256=chunk_hash_sha256,
            end_of_stream=chunk_end_of_stream,
        )

    def to_bytes(self):
        out_bytes = b""
        out_bytes += self.magic_hex().to_bytes(8, byteorder="big")
        out_bytes += self.chunk_id.to_bytes(8, byteorder="big")
        out_bytes += self.chunk_size_bytes.to_bytes(8, byteorder="big")
        out_bytes += self.chunk_offset_bytes.to_bytes(8, byteorder="big")
        out_bytes += bytes([int(self.end_of_stream)])
        assert len(self.chunk_hash_sha256) == 64
        out_bytes += self.chunk_hash_sha256.encode("utf-8")
        assert len(out_bytes) == ChunkHeader.length_bytes()
        return out_bytes

    @staticmethod
    def from_socket(sock: socket.socket):
        header_bytes = sock.recv(ChunkHeader.length_bytes())
        return ChunkHeader.from_bytes(header_bytes)

    def to_socket(self, sock: socket.socket):
        assert sock.sendall(self.to_bytes()) == None
