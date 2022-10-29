from dataclasses import dataclass


@dataclass(frozen=True)
class TransferConfig:
    multipart_enabled: bool = False
    multipart_threshold_mb: int = 128
    multipart_chunk_size_mb: int = 64
    multipart_max_chunks: int = 10000
