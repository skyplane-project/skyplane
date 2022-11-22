import json
import sys
import time
import uuid
from collections import defaultdict
from dataclasses import dataclass, field

import urllib3
from rich import print as rprint
from typing import Generator, Tuple, TYPE_CHECKING, List

from skyplane import exceptions
from skyplane.api.impl.chunker import Chunker, batch_generator, tail_generator
from skyplane.api.impl.path import parse_path
from skyplane.api.transfer_config import TransferConfig
from skyplane.chunk import ChunkRequest
from skyplane.obj_store.azure_blob_interface import AzureBlobObject
from skyplane.obj_store.gcs_interface import GCSObject
from skyplane.obj_store.object_store_interface import ObjectStoreInterface, ObjectStoreObject
from skyplane.api.impl.transfer_job import TransferJob
from skyplane.obj_store.s3_interface import S3Object
from skyplane.utils import logger
from skyplane.utils.fn import do_parallel

if TYPE_CHECKING:
    from skyplane.broadcast.bc_dataplane import BroadcastDataplane


@dataclass
class BCTransferJob(TransferJob):
    # TODO: might just use multiple TransferJob
    dst_paths: List[str] = []

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

        # the first destination region (BCTransferJob only needs one TransferJob)
        self.bucket_dst, self.dst_region, self.dst_prefix = parse_path(self.dst_path)
        self.dst_iface = self.dst_ifaces[self.bucket_dst]

        if self.requester_pays:
            self.src_iface.set_requester_bool(True)
            for dst_iface in self.dst_ifaces.values():
                dst_iface.set_requester_bool(True)

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
