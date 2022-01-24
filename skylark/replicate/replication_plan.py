from dataclasses import dataclass
from typing import List, Optional

from skylark.compute.aws.aws_cloud_provider import AWSCloudProvider
from skylark.compute.azure.azure_cloud_provider import AzureCloudProvider
from skylark.compute.gcp.gcp_cloud_provider import GCPCloudProvider
from skylark.obj_store.s3_interface import S3Interface
from skylark.obj_store.gcs_interface import GCSInterface
from skylark.utils.utils import do_parallel


@dataclass
class ReplicationJob:
    source_region: str
    source_bucket: str
    dest_region: str
    dest_bucket: str
    objs: List[str]

    # Generates random chunks for testing on the gateways
    random_chunk_size_mb: Optional[int] = None

    def src_obj_sizes(self):
        if self.source_region.split(":")[0] == "aws":
            interface = S3Interface(self.source_region.split(":")[1], self.source_bucket)
        if self.source_region.split(":")[0] == "gcp":
            interface = GCSInterface(self.source_region.split(":")[1][:-2], self.source_bucket)
        else:
            raise NotImplementedError
        get_size = lambda o: interface.get_obj_size(o)
        return do_parallel(get_size, self.objs, n=16, progress_bar=True, desc="Query object sizes")


class ReplicationTopology:
    def __init__(self, paths: Optional[List[List[str]]] = None):
        """
        paths: List of paths, each path is a list of nodes.
        E.g. [["aws:us-east-1", "aws:us-west-1"], ["aws:us-east-1", "aws:us-east-2", "aws:us-west-1"]]
        """
        self.paths = []
        self.source = None
        self.dest = None
        if paths is not None:
            for path in paths:
                self.add_path(path)

    def add_path(self, path: List[str]):
        assert len(path) > 1

        # validate path entries are valid regions
        for p in path:
            if p.startswith("aws:"):
                assert p.split(":")[1] in AWSCloudProvider.region_list(), f"{p} is not a valid AWS region"
            elif p.startswith("azure:"):
                assert p.split(":")[1] in AzureCloudProvider.region_list(), f"{p} is not a valid Azure region"
            elif p.startswith("gcp:"):
                assert p.split(":")[1] in GCPCloudProvider.region_list(), f"{p} is not a valid GCP region"
            else:
                raise NotImplementedError(f"Unknown provider for region {p}")

        # set source and dest
        if self.source is None:
            self.source = path[0]
        else:
            assert self.source == path[0]
        if self.dest is None:
            self.dest = path[-1]
        else:
            assert self.dest == path[-1]

        path_id = len(self.paths)
        self.paths.append(path)
        return path_id
