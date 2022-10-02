from skyplane.api.api import cp, deprovision

class Skyplane:
    def __init__(self, auth):
        # TODO: How to pass in auth to cloud api's own functions?
        # auth = Skyplane.Auth(aws=AWSAuthenticationConfig, azure=, )
    
    def copy(src="s3://us-east-1/foo", dst="s3://us-east-2/bar", num_vms=1, recursive=False):
        # with Session(src_region, dst_region, num_vms, solver=None) as s:
        #     s.replicate_job(src_file, dst_file, recursive=recursive)
        #     s.run()
        pass

    def new_session(src_region="aws:us-east-1", dst_region="aws:us-east-2", num_vms=1, solver=None):
        # solver = [None, "ILP", "RON"]
        return Session(src_region, dst_region, num_vms, solver)


class Session:
    def __init__(self, src_region, dst_region, num_vms, solver):
        self.src_region = src_region
        self.dst_region = dst_region
        self.num_vms = num_vms
        self.solver = solver
        self.auto_terminate = False
        self.job_list = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_tb):
        if self.auto_terminate:
            deprovision()
    
    def auto_terminate():
        self.auto_terminate = True

    def replicate_job(src_file="foo", dst_file="bar", recursive=False):
        # TODO: Combine src_region and src_file together. Same for dst
        job = Job(src, dst, num_vms, recursive)
        self.job_list.append(job)

    def run():
        for job in self.job_list:
            job.run()
    

class Job:
    def __init__(self, src, dst, num_vms, recursive):
        self.src = src
        self.dst = dst
        self.num_vms = num_vms
        self.recursive = recursive

    def run():
        # TODO: Need a job manager to run asynchronously
        cp(src, dst, recursive=recursive, max_instances=num_vms)

    def estimate_cost():
        # TODO
        pass
