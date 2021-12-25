# from pathlib import Path

# from skylark.utils import PathLike


# class Chunk:
#     def __init__(self, chunk_id: str, chunk_path: PathLike):
#         self.chunk_id = chunk_id
#         self.chunk_path = Path(chunk_path)

#         def read(self) -> bytes:
#             with open(self.chunk_path, "rb") as f:
#                 return f.read()

#         def __str__(self):
#             return f"Chunk({self.chunk_id}, {self.chunk_path})"


# class ChunkStore:
#     def __init__(self, base_path: PathLike = "/dev/shm/skylark/chunks"):
#         self.path = Path(base_path)
#         self.path.mkdir(parents=True, exist_ok=True)
#         self.chunks = {}

#     def store(self, chunk_id: str, data: bytes):
#         chunk_path = self.path / chunk_id
#         with open(chunk_path, "wb") as f:
#             f.write(data)
#         self.chunks[chunk_id] = Chunk(chunk_id, chunk_path)
#         return self.chunks[chunk_id]

#     def get(self, chunk_id: str) -> Chunk:
#         return self.chunks.get(chunk_id)

#     def delete(self, chunk_id: str):
#         chunk = self.chunks.get(chunk_id)
#         if chunk:
#             chunk.chunk_path.unlink()
#             del self.chunks[chunk_id]

#     def update_chunks(self):
#         for chunk_id in os.listdir(self.path):
#             chunk_path = self.path / chunk_id
#             self.chunks[chunk_id] = Chunk(chunk_id, chunk_path)
