from skyplane.api.api import cp, deprovision

class Skyplane:
    def __init__(self, auth):
        # TODO: How to pass in auth to cloud api's own functions?
        # auth = Skyplane.Auth(aws=AWSAuthenticationConfig, azure=, )
    
    def copy(src="s3://us-east-1/foo", dst="s3://us-east-2/bar", vms=8, recursive=False):
        # with Session(src_region, dst_region, vms, solver=None) as s:
        #     s.replicate_job(src_file, dst_file, recursive=recursive)
        #     s.run()
        pass

    def new_session(src_region="aws:us-east-1", dst_region="aws:us-east-2", vms=8, solver=None):
        # solver = [None, "ILP", "RON"]
        return Session(src_region, dst_region, vms, solver)


class Session:
    def __init__(self, src_region, dst_region, vms, solver):
        self.src_region = src_region
        self.dst_region = dst_region
        self.vms = vms
        self.solver = solver
        self.job_list = []

    def __enter__(self, auto_terminate=False):
        # TODO: This might not be correct
        self.auto_terminate = auto_terminate

    def __exit__(self, exc_type, exc_value, exc_tb):
        if self.auto_terminate:
            deprovision()

    def replicate_job(src_file="foo", dst_file="bar", recursive=False):
        # TODO: Combine src_region and src_file together. Same for dst
        job = Job(src, dst, vms, recursive)
        self.job_list.append(job)

    def run():
        for job in self.job_list:
            job.run()
    

class Job:
    def __init__(self, src, dst, vms, recursive):
        self.src = src
        self.dst = dst
        self.vms = vms
        self.recursive = recursive

    def run():
        # TODO: Need a job manager to run asynchronously
        cp(src, dst, recursive=recursive, max_instances=vms)

    def estimate_cost():
        # TODO
        pass
