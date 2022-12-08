from dataclasses import dataclass, field

import urllib3
from typing import Generator, Optional, TYPE_CHECKING, Tuple

from skyplane import exceptions
from skyplane.utils.path import parse_path
from skyplane.api.transfer_job import TransferJob
from skyplane.api.config import TransferConfig
from skyplane.chunk import ChunkRequest
import uuid

import json
import time
from collections import defaultdict
from dataclasses import dataclass, field

import urllib3
from typing import Generator, Tuple, TYPE_CHECKING

from skyplane.broadcast.impl.bc_chunker import BCChunker
from skyplane.obj_store.object_store_interface import ObjectStoreInterface, ObjectStoreObject
from skyplane.utils import logger
from skyplane.utils.fn import do_parallel
from skyplane.utils.timer import Timer

if TYPE_CHECKING:
    from skyplane.broadcast.bc_dataplane import BroadcastDataplane


@dataclass
class BCTransferJob:
    # TODO: might just use multiple TransferJob
    src_path: str
    dst_path: str  # NOTE: should be None, not used
    recursive: bool = False
    dst_paths: list = field(default_factory=list)
    requester_pays: bool = False
    uuid: str = field(init=False, default_factory=lambda: str(uuid.uuid4()))
    type: str = ""

    def __post_init__(self):
        print("Parse src: ", parse_path(self.src_path))
        provider_src, bucket_src, src_prefix = parse_path(self.src_path)
        self.src_prefix = src_prefix
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

    def bc_finalize(self, dst_region: str):
        raise NotImplementedError("Broadcast finalize not implemented")

    def bc_verify(self, dst_region: str):
        """Verifies the transfer completed, otherwise raises TransferFailedException."""
        raise NotImplementedError("Broadcast verify not implemented")

    @classmethod
    def _pre_filter_fn(cls, obj: ObjectStoreObject) -> bool:
        """Optionally filter source objects before they are transferred."""
        return True


@dataclass
class BCCopyJob(BCTransferJob):
    transfer_list: list = field(default_factory=list)  # transfer list for later verification
    multipart_transfer_list: list = field(default_factory=list)
    type: str = "copy"

    def __post_init__(self):
        self.http_pool = urllib3.PoolManager(retries=urllib3.Retry(total=3))
        return super().__post_init__()

    def gen_transfer_pairs(self, chunker: Optional[BCChunker] = None) -> Generator[Tuple[ObjectStoreObject, ObjectStoreObject], None, None]:
        """Generate transfer pairs for the transfer job."""
        if chunker is None:  # used for external access to transfer pair list
            chunker = BCChunker(self.src_iface, list(self.dst_ifaces.values()), TransferConfig())
        yield from chunker.transfer_pair_generator(self.src_prefix, self.dst_prefix, self.recursive, self._pre_filter_fn)

    def broadcast_dispatch(
        self,
        dataplane: "BroadcastDataplane",
        transfer_config: TransferConfig,
        dispatch_batch_size: int = 1000,
    ) -> Generator[ChunkRequest, None, None]:
        """Dispatch transfer job to specified gateways."""
        chunker = BCChunker(self.src_iface, list(self.dst_ifaces.values()), transfer_config, dataplane.topology.num_partitions)
        transfer_pair_generator = self.gen_transfer_pairs(chunker)
        gen_transfer_list = chunker.tail_generator(transfer_pair_generator, self.transfer_list)
        chunks = chunker.chunk(gen_transfer_list)
        chunk_requests = chunker.to_chunk_requests(chunks)
        batches = chunker.batch_generator(
            chunker.prefetch_generator(chunk_requests, buffer_size=dispatch_batch_size * 32), batch_size=dispatch_batch_size
        )

        src_gateways = dataplane.source_gateways()
        bytes_dispatched = [0] * len(src_gateways)
        n_multiparts = 0
        done_dispatch_map_to_dst = False

        # print("Chunker mapping for upload ids:", chunker.all_mappings_for_upload_ids)
        start = time.time()
        for batch in batches:
            end = time.time()
            logger.fs.debug(f"Queried {len(batch)} chunks in {end - start:.2f} seconds")
            start = time.time()

            # copy new multipart transfers to the multipart transfer list
            # print("Chunker mapping for upload ids:", chunker.all_mappings_for_upload_ids)
            def dispatch_id_maps_to_dst():
                # NOTE: update the upload ids in each dest gateways --> chunker.all_mappings_for_upload_ids
                dst_servers = dataplane.sink_gateways()
                for dst_server in dst_servers:
                    construct_mappings = {}
                    dst_region_tag = dst_server.region_tag
                    for mapping in chunker.all_mappings_for_upload_ids:
                        for key, value in mapping.items():
                            # print(f"mapping: {key}, {value}")
                            if key.startswith(dst_region_tag):
                                construct_mappings[key] = value

                    # print("Construct mappings: ")
                    # from pprint import pprint
                    # pprint(construct_mappings)

                    # print("Json encode: ", json.dumps(construct_mappings).encode("utf-8"))
                    start = time.time()
                    reply = self.http_pool.request(
                        "POST",
                        f"{dst_server.gateway_api_url}/api/v1/upload_id_maps",
                        body=json.dumps(construct_mappings).encode("utf-8"),
                        headers={"Content-Type": "application/json"},
                    )
                    end = time.time()
                    # TODO: assume that only destination nodes would write to the obj store
                    if reply.status != 200:
                        raise Exception(
                            f"Failed to update upload ids to the dst gateway {dst_server.instance_name()}, constructed mappings for {dst_region_tag}: {construct_mappings}"
                        )
                    logger.fs.debug(f"Upload ids to dst gateway {dst_server.instance_name()} in {end - start:.2f} seconds")

            if not done_dispatch_map_to_dst:
                dispatch_id_maps_to_dst()
                done_dispatch_map_to_dst = True

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

            updated_len = len(chunker.multipart_upload_requests)
            self.multipart_transfer_list.extend(chunker.multipart_upload_requests[n_multiparts:updated_len])
            n_multiparts = updated_len

    def bc_finalize(self, dst_region: str):
        groups = defaultdict(list)

        for req in self.multipart_transfer_list:
            if "dest_ifaces" not in req or "region_bucketkey_to_upload_id" not in req:
                raise Exception(f"Invalid broadcast multipart upload request: {req}")

            dest_iface_list = [d for d in req["dest_ifaces"] if d.region_tag() == dst_region]
            for dest_iface in dest_iface_list:
                region = dest_iface.region_tag()
                bucket = dest_iface.bucket()
                upload_id = req["region_bucketkey_to_upload_id"][region + ":" + bucket + ":" + req["key"]]
                one_req = dict(upload_id=upload_id, key=req["key"], parts=req["parts"], region=region, bucket=bucket)
                groups[(region, bucket)].append(one_req)

        for key, group in groups.items():
            region, bucket = key
            batch_len = max(1, len(group) // 128)
            batches = [group[i : i + batch_len] for i in range(0, len(group), batch_len)]
            obj_store_interface = ObjectStoreInterface.create(region, bucket)

            def complete_fn(batch):
                for req in batch:
                    print("Complete multipart key:", req["key"], " and multipart ids: ", req["upload_id"])
                    obj_store_interface.complete_multipart_upload(req["key"], req["upload_id"])

            do_parallel(complete_fn, batches, n=-1)

    def bc_verify(self, dst_region: str):
        # NOTE: assume dst keys are the same across destinations?
        dst_keys = {dst_o.key: src_o for src_o, dst_o in self.transfer_list}
        dest_iface_list = [d for d in self.dst_ifaces.values() if d.region_tag() == dst_region]

        for dest_iface in dest_iface_list:
            dest_iface.region_tag()
            for obj in dest_iface.list_objects(self.dst_prefixes[dest_iface.bucket()]):
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

    def estimate_cost(self):
        raise NotImplementedError()

    def gen_transfer_pairs(self, chunker: Optional[BCChunker] = None) -> Generator[Tuple[ObjectStoreObject, ObjectStoreObject], None, None]:
        """Generate transfer pairs for the transfer job."""
        raise NotImplementedError("Broadcast Sync Job get_transfer_pairs not implemented")

    def _enrich_dest_objs(
        self, transfer_pairs: Generator[Tuple[ObjectStoreObject, ObjectStoreObject], None, None], dest_prefix: str
    ) -> Generator[Tuple[ObjectStoreObject, ObjectStoreObject], None, None]:
        """
        For skyplane sync, we enrich dest obj metadata with our existing dest obj metadata from the dest bucket following a query.
        """
        raise NotImplementedError("Broadcast Sync Job_enrich_dest_objs not implemented")

    @classmethod
    def _post_filter_fn(cls, src_obj: ObjectStoreObject, dest_obj: ObjectStoreObject) -> bool:
        return not dest_obj.exists or (src_obj.last_modified > dest_obj.last_modified or src_obj.size != dest_obj.size)
