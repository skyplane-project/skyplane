from dataclasses import dataclass
import socket


@dataclass
class WireProtocolHeader:
    """Lightweight wire protocol header for chunk transfers along socket."""

    chunk_id: int  # unsigned long
    chunk_len: int  # unsigned long
    end_of_stream: bool = False  # false by default, but true if this is the last chunk

    @staticmethod
    def magic_hex():
        return 0x534B595F4C41524B  # "SKY_LARK"

    @staticmethod
    def length_bytes():
        # magic (8) + chunk_id (8) + chunk_len (8) + end_of_stream (1)
        return 8 + 8 + 8 + 1

    @staticmethod
    def from_bytes(data: bytes):
        assert len(data) == WireProtocolHeader.length_bytes()
        magic = int.from_bytes(data[:8], byteorder="big")
        if magic != WireProtocolHeader.magic_hex():
            raise ValueError("Invalid magic number")
        chunk_id = int.from_bytes(data[8:16], byteorder="big")
        chunk_len = int.from_bytes(data[16:24], byteorder="big")
        chunk_end_of_stream = bool(data[24])
        return WireProtocolHeader(chunk_id=chunk_id, chunk_len=chunk_len, end_of_stream=chunk_end_of_stream)

    def to_bytes(self):
        out_bytes = b""
        out_bytes += self.magic_hex().to_bytes(8, byteorder="big")
        out_bytes += self.chunk_id.to_bytes(8, byteorder="big")
        out_bytes += self.chunk_len.to_bytes(8, byteorder="big")
        out_bytes += bytes([int(self.end_of_stream)])
        assert len(out_bytes) == WireProtocolHeader.length_bytes()
        return out_bytes

    @staticmethod
    def from_socket(sock: socket.socket):
        header_bytes = sock.recv(WireProtocolHeader.length_bytes())
        return WireProtocolHeader.from_bytes(header_bytes)

    def to_socket(self, sock: socket.socket):
        assert sock.sendall(self.to_bytes()) == None
