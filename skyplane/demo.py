from skyplane.api.api_class import *
import time

# preprovision vms for demo
# keep the logging

# Simple Copy
client1 = new_client(None)
client1.copy(src="s3://jason-us-east-1/imagenet-train-000000.tar", 
            dst="s3://jason-us-west-2/imagenet-train-000000.tar", recursive=False)

# Session Copy
client2 = new_client(None)
session = client2.new_session(
    src_bucket="aws:jason-us-east-1", 
    dst_bucket="aws:jason-us-west-2", 
    num_vms=1, 
    solver=None
)

with session as s:
    session.auto_terminate()
    s.copy("fake_imagenet", "fake_imagenet_test", recursive=True)
    # s.copy("fake_imagenet", "fake_imagenet_rand", recursive=True)
    # This will not start the transfer
    future1 = s.run_async()
    s.wait_for_completion(future1)
    s.copy("fake_imagenet", "fake_imagenet_2", recursive=True)
    # This will not start the transfer
    start_time = time.time()
    future2 = s.run_async()
    print("future 2", future2.running())
    print("spent", time.time()-start_time)
    # This starts the transfer
    # s.run(future2)
    # s.run(future1)