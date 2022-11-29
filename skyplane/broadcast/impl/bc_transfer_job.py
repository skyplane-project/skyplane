from dataclasses import dataclass, field

import urllib3
from typing import Generator, TYPE_CHECKING, Tuple

from skyplane import exceptions
from skyplane.api.impl.path import parse_path
from skyplane.obj_store.object_store_interface import ObjectStoreInterface, ObjectStoreObject
from skyplane.api.impl.chunker import batch_generator, tail_generator
from skyplane.api.impl.transfer_job import TransferJob
from skyplane.api.transfer_config import TransferConfig
from skyplane.chunk import ChunkRequest
import uuid

import json
import time
from collections import defaultdict
from dataclasses import dataclass, field

import urllib3
from typing import Generator, Tuple, TYPE_CHECKING

from skyplane import exceptions
from skyplane.api.impl.chunker import batch_generator, tail_generator
from skyplane.broadcast.impl.bc_chunker import BCChunker
from skyplane.api.impl.path import parse_path
from skyplane.api.transfer_config import TransferConfig
from skyplane.obj_store.object_store_interface import ObjectStoreInterface, ObjectStoreObject
from skyplane.utils import logger
from skyplane.utils.fn import do_parallel

if TYPE_CHECKING:
    from skyplane.broadcast.bc_dataplane import BroadcastDataplane


@dataclass
class BCTransferJob(TransferJob):
    # TODO: might just use multiple TransferJob
    src_path: str
    dst_path: str
    recursive: bool = False
    dst_paths: list = field(default_factory=list)
    requester_pays: bool = False
    uuid: str = field(init=False, default_factory=lambda: str(uuid.uuid4()))
    type: str = ""

    def __post_init__(self):
        provider_src, bucket_src, self.src_prefix = parse_path(self.src_path)
        self.src_iface = ObjectStoreInterface.create(f"{provider_src}:infer", bucket_src)
        self.src_bucket = bucket_src
        self.src_region = self.src_iface.region_tag()

        self.dst_regions, self.dst_ifaces, self.dst_prefixes = {}, {}, {}  # bucket_dst --> dst_region
        for dst_path in self.dst_paths:
            provider_dst, bucket_dst, dst_prefix = parse_path(dst_path)
            self.dst_ifaces[bucket_dst] = ObjectStoreInterface.create(f"{provider_dst}:infer", bucket_dst)
            self.dst_prefixes[bucket_dst] = dst_prefix
            self.dst_regions[bucket_dst] = self.dst_ifaces[bucket_dst].region_tag()

        # initialize bucket_dst, dst_region, dst_prefix, dst_iface to the first destination (BCTransferJob only needs one TransferJob)
        self.dst_region, self.bucket_dst, self.dst_prefix = parse_path(self.dst_path)
        self.dst_iface = self.dst_ifaces[self.bucket_dst]

        if self.requester_pays:
            self.src_iface.set_requester_bool(True)
            for dst_iface in self.dst_ifaces.values():
                dst_iface.set_requester_bool(True)

    def broadcast_dispatch(self, dataplane: "BroadcastDataplane", **kwargs) -> Generator[ChunkRequest, None, None]:
        raise NotImplementedError("Broadcast Dispatch not implemented")

    def _transfer_pair_generator(self) -> Generator[Tuple[ObjectStoreObject, ObjectStoreObject], None, None]:
        """Query source region and return list of objects to transfer. Return a random pair for any destination"""
        # NOTE: do additional checking across destination
        for dst_iface in self.dst_ifaces.values():
            if not dst_iface.bucket_exists():
                raise exceptions.MissingBucketException(f"Destination bucket {dst_iface.path()} does not exist or is not readable.")

        return super()._transfer_pair_generator()


@dataclass
class BCCopyJob(BCTransferJob):
    transfer_list: list = field(default_factory=list)  # transfer list for later verification
    multipart_transfer_list: list = field(default_factory=list)
    type: str = "copy"

    def __post_init__(self):
        self.http_pool = urllib3.PoolManager(retries=urllib3.Retry(total=3))
        return super().__post_init__()

    def broadcast_dispatch(
        self, dataplane: "BroadcastDataplane", transfer_config: TransferConfig, dispatch_batch_size: int = 64
    ) -> Generator[ChunkRequest, None, None]:
        """Dispatch transfer job to specified gateways."""
        chunker = BCChunker(self.src_iface, self.dst_iface, transfer_config, dataplane.topology.num_partitions)
        n_multiparts = 0
        gen_transfer_list = tail_generator(self._transfer_pair_generator(), self.transfer_list)
        chunks = chunker.chunk(gen_transfer_list)
        chunk_requests = chunker.to_bc_chunk_requests(chunks)
        batches = batch_generator(chunk_requests, dispatch_batch_size)
        src_gateways = dataplane.source_gateways()
        bytes_dispatched = [0] * len(src_gateways)
        start = time.time()
        for batch in batches:
            end = time.time()
            logger.fs.debug(f"Queried {len(batch)} chunks in {end - start:.2f} seconds")
            min_idx = bytes_dispatched.index(min(bytes_dispatched))
            server = src_gateways[min_idx]
            n_bytes = sum([cr.chunk.chunk_length_bytes for cr in batch])
            bytes_dispatched[min_idx] += n_bytes
            start = time.time()
            reply = self.http_pool.request(
                "POST",
                f"{server.gateway_api_url}/api/v1/chunk_requests",
                body=json.dumps([c.as_dict() for c in batch]).encode("utf-8"),
                headers={"Content-Type": "application/json"},
            )
            end = time.time()
            if reply.status != 200:
                raise Exception(f"Failed to dispatch chunk requests {server.instance_name()}: {reply.data.decode('utf-8')}")
            logger.fs.debug(
                f"Dispatched {len(batch)} chunk requests to {server.instance_name()} ({n_bytes} bytes) in {end - start:.2f} seconds"
            )
            yield from batch

            # copy new multipart transfers to the multipart transfer list
            updated_len = len(chunker.multipart_upload_requests)
            self.multipart_transfer_list.extend(chunker.multipart_upload_requests[n_multiparts:updated_len])
            n_multiparts = updated_len

    def finalize(self):
        groups = defaultdict(list)
        for req in self.multipart_transfer_list:
            if "region" not in req or "bucket" not in req:
                raise Exception(f"Invalid multipart upload request: {req}")
            groups[(req["region"], req["bucket"])].append(req)
        for key, group in groups.items():
            region, bucket = key
            batch_len = max(1, len(group) // 128)
            batches = [group[i : i + batch_len] for i in range(0, len(group), batch_len)]
            obj_store_interface = ObjectStoreInterface.create(region, bucket)

            def complete_fn(batch):
                for req in batch:
                    obj_store_interface.complete_multipart_upload(req["key"], req["upload_id"])

            do_parallel(complete_fn, batches, n=-1)

    def verify(self):
        dst_keys = {dst_o.key: src_o for src_o, dst_o in self.transfer_list}
        # NOTE: assume dst keys are the same across destinations?
        for dst_iface in self.dst_ifaces.values():
            for obj in dst_iface.list_objects(self.dst_prefixes[dst_iface.bucket()]):
                # check metadata (src.size == dst.size) && (src.modified <= dst.modified)
                src_obj = dst_keys.get(obj.key)
                if src_obj and src_obj.size == obj.size and src_obj.last_modified <= obj.last_modified:
                    del dst_keys[obj.key]
            if dst_keys:
                raise exceptions.TransferFailedException(
                    f"{len(dst_keys)} objects failed verification", [obj.key for obj in dst_keys.values()]
                )


@dataclass
class BCSyncJob(BCCopyJob):
    type: str = "sync"

    def __post_init__(self):
        return super().__post_init__()

    @classmethod
    def _post_filter_fn(cls, src_obj: ObjectStoreObject, dest_obj: ObjectStoreObject) -> bool:
        return not dest_obj.exists or (src_obj.last_modified > dest_obj.last_modified or src_obj.size != dest_obj.size)
