from skyplane.api.api import cp, deprovision
from skyplane.api.auth.auth_config import AuthenticationConfig
from skyplane.cli.common import parse_path
import asyncio

from skyplane.obj_store.object_store_interface import ObjectStoreInterface

def new_client(auth):
    return Skyplane(auth)

class Skyplane:
    def __init__(self, auth: AuthenticationConfig):
        # TODO: Pass auth to cloud api's own functions
        self.auth = auth
    
    def copy(self, src="s3://us-east-1/foo", dst="s3://us-east-2/bar", num_vms=1, recursive=False):
        provider_src, bucket_src, path_src = parse_path(src)
        provider_dst, bucket_dst, path_dst = parse_path(dst)
        src_region_tag, dst_region_tag = f"{provider_src}:infer", f"{provider_dst}:infer"
        src_client = ObjectStoreInterface.create(src_region_tag, bucket_src, config=self.auth)
        dst_client = ObjectStoreInterface.create(dst_region_tag, bucket_dst, config=self.auth)
        src_bucket = provider_src + ":" + bucket_src
        dst_bucket = provider_dst + ":" + bucket_dst
        with Session(src_bucket, dst_bucket, self.auth, num_vms, solver=None) as session:
            session.auto_terminate()
            session.copy(path_src, src_client, path_dst, dst_client, recursive=recursive)
            session.run()

    def new_session(self, src_bucket="aws:us-east-1", dst_bucket="aws:us-east-2", num_vms=1, solver=None):
        # solver = [None, "ILP", "RON"]
        return Session(src_bucket, dst_bucket, num_vms, solver)


class Session:
    def __init__(self, src_bucket, dst_bucket, auth, num_vms, solver):
        if src_bucket.startswith("aws:"):
            src_bucket = src_bucket.replace("aws:", "s3://")
        elif src_bucket.startswith("gs:"):
            src_bucket = src_bucket.replace("gs:", "gcp://")
        if dst_bucket.startswith("aws:"):
            dst_bucket = dst_bucket.replace("aws:", "s3://")
        elif dst_bucket.startswith("gs:"):
            dst_bucket = dst_bucket.replace("gs:", "gcp://")
        self.src_bucket = src_bucket
        self.dst_bucket = dst_bucket
        self.num_vms = num_vms
        self.solver = solver
        self.auth = auth
        self._auto_terminate = False
        self.job_list = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_tb):
        if self._auto_terminate:
            deprovision()
    
    def auto_terminate(self):
        self._auto_terminate = True

    def copy(self, src_file="foo", src_client=None, dst_file="bar", dst_client=None, recursive=False):
        src = self.src_bucket + "/" + src_file
        dst = self.dst_bucket + "/" + dst_file
        job = Job(src, src_client, dst, dst_client, self.num_vms, recursive)
        self.job_list.append(job)
    
    async def run_async(self):
        # TODO: Add reuse_gateways
        jobs_to_run = self.job_list
        self.job_list = []
        result = [job.run() for job in jobs_to_run]
        await asyncio.gather(*result)
        
    def run(self):
        asyncio.run(self.run_async())

class Job:
    def __init__(self, src, src_client, dst, dst_client, num_vms, recursive):
        self.src = src
        self.src = src_client
        self.dst = dst
        self.dst = dst_client
        self.num_vms = num_vms
        self.recursive = recursive

    async def run(self):
        await cp(self.src, self.src_client, self.dst, self.dst_client, recursive=self.recursive, max_instances=self.num_vms)

    def estimate_cost(self):
        # TODO
        pass
