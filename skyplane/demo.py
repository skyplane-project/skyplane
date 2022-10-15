from skyplane.api.api_class import *

# Simple Copy
# client1 = new_client(None)
# client1.copy(src="s3://jason-us-east-1/fake_imagenet/", dst="s3://jason-us-west-2/fake_imagenet_1/", recursive=True)

# Session Copy
client2 = new_client(None)
session = client2.new_session(
    src_bucket="aws:jason-us-east-1", 
    dst_bucket="aws:jason-us-west-2", 
    num_vms=1, 
    solver=None
)

with session as s:
    # session.auto_terminate()
    s.copy("fake_imagenet", "fake_imagenet_test", recursive=True)
    s.copy("fake_imagenet", "fake_imagenet_rand", recursive=True)
    # This will not start the transfer
    future1 = s.run_async()
    s.copy("fake_imagenet", "fake_imagenet_2", recursive=True)
    # This will not start the transfer
    future2 = s.run_async()
    # This starts the transfer
    s.run(future2)
    s.run(future1)