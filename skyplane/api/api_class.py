from skyplane.api.api import cp
from skyplane.api.api import deprovision as _deprovision
from skyplane.api.auth.auth_config import AuthenticationConfig
from skyplane.cli.common import parse_path
import asyncio

from skyplane.obj_store.object_store_interface import ObjectStoreInterface


def new_client(auth):
    return Skyplane(auth)


def deprovision():
    _deprovision()


class Skyplane:
    def __init__(self, auth: AuthenticationConfig):
        # TODO: Pass auth to cloud api's own functions
        self.auth = auth

    # src="s3://us-east-1/foo", dst="s3://us-east-2/bar"
    def copy(self, src, dst, num_vms=1, recursive=False):
        provider_src, bucket_src, path_src = parse_path(src)
        provider_dst, bucket_dst, path_dst = parse_path(dst)
        src_bucket = provider_src + ":" + bucket_src
        dst_bucket = provider_dst + ":" + bucket_dst
        with Session(src_bucket, dst_bucket, self.auth, num_vms, solver=None) as session:
            session.auto_terminate()
            session.copy(path_src, path_dst, recursive=recursive)
            session.run()
    # src_bucket="aws:us-east-1", dst_bucket="aws:us-east-2"
    def new_session(self, src_bucket, dst_bucket, num_vms=1, solver=None):
        # solver = [None, "ILP", "RON"]
        return Session(src_bucket, dst_bucket, self.auth, num_vms, solver)


class Session:
    def __init__(self, src_bucket, dst_bucket, auth, num_vms, solver):
        provider_src, bucket_src = src_bucket.split(":")
        provider_dst, bucket_dst = dst_bucket.split(":")
        src_region_tag, dst_region_tag = f"{provider_src}:infer", f"{provider_dst}:infer"
        self.src_client = ObjectStoreInterface.create(src_region_tag, bucket_src, config=auth)
        self.dst_client = ObjectStoreInterface.create(dst_region_tag, bucket_dst, config=auth)
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

    # src_file="foo", dst_file="bar"
    def copy(self, src_file, dst_file, recursive=False):
        src = self.src_bucket + "/" + src_file
        dst = self.dst_bucket + "/" + dst_file
        job = Job(src, self.src_client, dst, self.dst_client, self.num_vms, recursive)
        self.job_list.append(job)

    def run_async(self):
        jobs_to_run = self.job_list
        self.job_list = []
        async def run_async_help(self):
            # TODO: Add reuse_gateways
            # this is not reached since funct is async
            result = [job.run() for job in jobs_to_run]
            await asyncio.gather(*result)
        return run_async_help(self)

    def run(self, future=None):
        if not future:
            asyncio.run(self.run_async())
        else:
            asyncio.run(future)


class Job:
    def __init__(self, src, src_client, dst, dst_client, num_vms, recursive):
        self.src = src
        self.src_client = src_client
        self.dst = dst
        self.dst_client = dst_client
        self.num_vms = num_vms
        self.recursive = recursive

    async def run(self):
        await cp(self.src, self.src_client, self.dst, self.dst_client, recursive=self.recursive, max_instances=self.num_vms)

    def estimate_cost(self):
        # TODO
        raise NotImplementedError
