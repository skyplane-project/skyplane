from skyplane.api.api import SkyplaneClient
from skyplane.api.auth_config import AWSConfig

client = SkyplaneClient(aws_config=AWSConfig())
dp = client.direct_dataplane("aws", "us-east-1", "aws", "us-west-2", n_vms=1)

with dp.auto_deprovision():
    dp.provision()