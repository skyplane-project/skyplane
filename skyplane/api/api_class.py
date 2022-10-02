from skyplane.api.api import cp, deprovision
from skyplane.cli.common import parse_path

class Skyplane:
    def __init__(self, auth):
        # TODO: How to pass in auth to cloud api's own functions?
        # auth = Skyplane.Auth(aws=AWSAuthenticationConfig, azure=, )
        pass
    
    def copy(src="s3://us-east-1/foo", dst="s3://us-east-2/bar", num_vms=1, recursive=False):
        provider_src, bucket_src, path_src = parse_path(src)
        provider_dst, bucket_dst, path_dst = parse_path(dst)
        src_bucket = provider_src + ":" + bucket_src
        dst_bucket = provider_dst + ":" + bucket_dst
        with Session(src_bucket, dst_bucket, num_vms, solver=None) as session:
            session.replicate_job(path_src, path_dst, recursive=recursive)
            session.run()

    def new_session(src_bucket="aws:us-east-1", dst_bucket="aws:us-east-2", num_vms=1, solver=None):
        # solver = [None, "ILP", "RON"]
        return Session(src_bucket, dst_bucket, num_vms, solver)


class Session:
    def __init__(self, src_bucket, dst_bucket, num_vms, solver):
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
        self.auto_terminate = False
        self.job_list = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_tb):
        if self.auto_terminate:
            deprovision()
    
    def auto_terminate(self):
        self.auto_terminate = True

    def replicate_job(self, src_file="foo", dst_file="bar", recursive=False):
        src = self.src_bucket + "/" + src_file
        dst = self.dst_bucket + "/" + dst_file
        job = Job(src, dst, self.num_vms, recursive)
        self.job_list.append(job)

    def run(self):
        # TODO: Add reuse_gateways
        for job in self.job_list:
            job.run()
    

class Job:
    def __init__(self, src, dst, num_vms, recursive):
        self.src = src
        self.dst = dst
        self.num_vms = num_vms
        self.recursive = recursive

    def run(self):
        # TODO: Need a job manager to run asynchronously
        cp(self.src, self.dst, recursive=self.recursive, max_instances=self.num_vms)

    def estimate_cost():
        # TODO
        pass
