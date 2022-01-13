# Skylark: A Unified Data Layer for the Multi-Cloud

<img src="https://gist.githubusercontent.com/parasj/d67e6e161ea1329d4509c69bc3325dcb/raw/232009efdeb8620d2acb91aec111dedf98fdae18/skylark.jpg" width="350px">
Skylark is lifting cloud object stores to the Sky.

## Instructions to build and run demo
Skylark is composed of two components: A ReplicatorClient that runs locally on your machine that is responsible for provisioning instances and coordinating replication jobs and a GatewayDaemon that runs on each provisioned instance to actually copy data through the overlay.

This package represents both components as a single binary. Docker builds a single container with the GatewayDaemon and pushes it to the Github Container Registry (ghcr.io). After provisioning an instance, a GatewayDaemon is started by launching that container. Therefore, it's simple and fast to launch a new Gateway.

### Requirements
* Python 3.8 or greater
* Docker
    * **Ensure you have authenticated your Github account with Docker**: https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-container-registry#authenticating-to-the-container-registry
    * TLDR:
        * (1) Install docker with `curl -fsSL https://get.docker.com -o get-docker.sh && sh get-docker.sh`
        * (1) Create a Personal Access Token at https://github.com/settings/tokens/new with "write:packages" permissions
        * (2) Run `echo <PERSONAL_ACCESS_TOKEN> | sudo docker login ghcr.io -u <GITHUB_USERNAME> --password-stdin`

### Building and deploying the gateway
To run a sample replication, first build a new version of the GatewayDaemon Docker image and push it to ghcr.io (ensure you are authenticated as above):

```
$ pip install -e ".[all]"
$ source scripts/pack_docker.sh
```
<details>
<summary>pack_docker result</summary>
<br>

```
$ pip install -e .
$ source scripts/pack_docker.sh
Building docker image
[+] Building 0.0s (2/2) FINISHED
 => [internal] load build definition from Dockerfile                                                                                               0.0s
 => => transferring dockerfile: 2B                                                                                                                 0.0s
 => [internal] load .dockerignore                                                                                                                  0.0s
 => => transferring context: 2B                                                                                                                    0.0s
failed to solve with frontend dockerfile.v0: failed to read dockerfile: open /var/lib/docker/tmp/buildkit-mount683951637/Dockerfile: no such file or directory
Uploading docker image to ghcr.io/parasj/skylark:local-PotRzrFT
The push refers to repository [ghcr.io/parasj/skylark]
20d2ed8618ca: Layer already exists
1c4146875228: Layer already exists
1f4f7ac2f199: Layer already exists
d1e36ec88afa: Layer already exists
824bf068fd3d: Layer already exists
local-PotRzrFT: digest: sha256:f412e376290d5a7bad28aca57ce9ffcf579e8dd7db3f4d6fb68ceae829d0a6b2 size: 1371
Deleted build cache objects:
tltkismwtov5n8zokghil1py9
u0e2ymhmv64oriiq66ibepn63

Total reclaimed space: 0B
SKYLARK_DOCKER_IMAGE=ghcr.io/parasj/skylark:local-PotRzrFT
```

</details>

The script will export the new image (ghcr.io/parasj/skylark:local-PotRzrFT) to an environment variable (`SKYLARK_DOCKER_IMAGE`). Ensure you use `source` so the environment variable is published to your shell.

### Running a replication job
We then run the ReplicatorClient with that new Docker image (stored in `$SKYLARK_DOCKER_IMAGE`):
```
$ skylark replicate-random aws:ap-northeast-1 aws:eu-central-1 --inter-region aws:us-east-2 \
   --chunk-size-mb 16 \
   --n-chunks 2048 \
   --num-gateways 1 \
   --num-outgoing-connections 32
```
<details>
<summary>`skylark replicate-random` result</summary>
<br>
 
```
$ skylark replicate-random aws:ap-northeast-1 aws:eu-central-1 --inter-region aws:us-east-2 --chunk-size-mb 16 --n-chunks 2048 --num-gateways 1 --num-outgoing-connections 32
2022-01-11 21:40:03.858 | DEBUG    | skylark.utils.utils:__exit__:24 - Cloud SSH key initialization: 5.63s
2022-01-11 21:40:09.653 | DEBUG    | skylark.utils.utils:__exit__:24 - Provisioning instances and waiting to boot: 0.00s
2022-01-11 21:40:09.654 | DEBUG    | skylark.compute.server:start_gateway:181 - Starting gateway aws:ap-northeast-1:i-0431a91c9fef9e10e: Installing docker
2022-01-11 21:40:09.654 | DEBUG    | skylark.compute.server:start_gateway:181 - Starting gateway aws:us-east-2:i-0351c4b3383c0e800: Installing docker
2022-01-11 21:40:09.660 | DEBUG    | skylark.compute.server:start_gateway:181 - Starting gateway aws:eu-central-1:i-065b5ef5536481277: Installing docker
2022-01-11 21:40:20.085 | DEBUG    | skylark.compute.server:start_gateway:197 - Starting gateway aws:eu-central-1:i-065b5ef5536481277: Starting monitoring
2022-01-11 21:40:20.355 | DEBUG    | skylark.compute.server:start_gateway:204 - Starting gateway aws:eu-central-1:i-065b5ef5536481277: Pulling docker image
2022-01-11 21:40:20.682 | DEBUG    | skylark.compute.server:start_gateway:197 - Starting gateway aws:ap-northeast-1:i-0431a91c9fef9e10e: Starting monitoring
2022-01-11 21:40:21.117 | DEBUG    | skylark.compute.server:start_gateway:204 - Starting gateway aws:ap-northeast-1:i-0431a91c9fef9e10e: Pulling docker image
2022-01-11 21:40:21.282 | DEBUG    | skylark.compute.server:start_gateway:197 - Starting gateway aws:us-east-2:i-0351c4b3383c0e800: Starting monitoring
2022-01-11 21:40:21.317 | DEBUG    | skylark.compute.server:start_gateway:204 - Starting gateway aws:us-east-2:i-0351c4b3383c0e800: Pulling docker image
2022-01-11 21:40:21.705 | DEBUG    | skylark.compute.server:start_gateway:209 - Starting gateway aws:us-east-2:i-0351c4b3383c0e800: Starting gateway container ghcr.io/parasj/skylark:local-bcf7dad82ea5d2fb7aba5729c03516af
2022-01-11 21:40:25.917 | DEBUG    | skylark.compute.server:start_gateway:209 - Starting gateway aws:eu-central-1:i-065b5ef5536481277: Starting gateway container ghcr.io/parasj/skylark:local-bcf7dad82ea5d2fb7aba5729c03516af
2022-01-11 21:40:27.717 | DEBUG    | skylark.compute.server:start_gateway:209 - Starting gateway aws:ap-northeast-1:i-0431a91c9fef9e10e: Starting gateway container ghcr.io/parasj/skylark:local-bcf7dad82ea5d2fb7aba5729c03516af
2022-01-11 21:40:31.844 | DEBUG    | skylark.utils.utils:__exit__:24 - Install gateway package on instances: 22.19s
2022-01-11 21:40:31.844 | INFO     | skylark.cli.cli:replicate_random:103 - Provisioned path aws:ap-northeast-1 -> aws:us-east-2 -> aws:eu-central-1
2022-01-11 21:40:31.844 | INFO     | skylark.cli.cli:replicate_random:105 - 	[aws:ap-northeast-1] http://18.183.44.196:8888/container/9b60878e4de3
2022-01-11 21:40:31.844 | INFO     | skylark.cli.cli:replicate_random:105 - 	[aws:us-east-2] http://3.15.39.236:8888/container/7e2f9bb71579
2022-01-11 21:40:31.844 | INFO     | skylark.cli.cli:replicate_random:105 - 	[aws:eu-central-1] http://18.193.115.24:8888/container/9e611399792b
2022-01-11 21:40:31.867 | DEBUG    | skylark.utils.utils:__exit__:24 - Building chunk requests: 0.02s
2022-01-11 21:40:31.867 | DEBUG    | skylark.replicate.replicator_client:run_replication_plan:239 - Sending 2048 chunk requests to 18.183.44.196
2022-01-11 21:40:35.576 | INFO     | skylark.cli.cli:replicate_random:118 - 32.00GByte replication job launched
2022-01-11 21:40:40.211 | DEBUG    | skylark.replicate.replicator_client:monitor_transfer:323 - 0/2048 chunks completed (0.00GB out of 32.00GB) at average throughput 0.00Gbit/s
2022-01-11 21:40:45.858 | DEBUG    | skylark.replicate.replicator_client:monitor_transfer:323 - 106/2048 chunks completed (1.66GB out of 32.00GB) at average throughput 1.36Gbit/s
2022-01-11 21:40:52.039 | DEBUG    | skylark.replicate.replicator_client:monitor_transfer:323 - 251/2048 chunks completed (3.92GB out of 32.00GB) at average throughput 2.00Gbit/s
2022-01-11 21:40:59.069 | DEBUG    | skylark.replicate.replicator_client:monitor_transfer:323 - 416/2048 chunks completed (6.50GB out of 32.00GB) at average throughput 2.30Gbit/s
2022-01-11 21:41:06.262 | DEBUG    | skylark.replicate.replicator_client:monitor_transfer:323 - 575/2048 chunks completed (8.98GB out of 32.00GB) at average throughput 2.43Gbit/s
2022-01-11 21:41:14.361 | DEBUG    | skylark.replicate.replicator_client:monitor_transfer:323 - 756/2048 chunks completed (11.81GB out of 32.00GB) at average throughput 2.50Gbit/s
2022-01-11 21:41:21.962 | DEBUG    | skylark.replicate.replicator_client:monitor_transfer:323 - 919/2048 chunks completed (14.36GB out of 32.00GB) at average throughput 2.53Gbit/s
2022-01-11 21:41:29.723 | DEBUG    | skylark.replicate.replicator_client:monitor_transfer:323 - 1095/2048 chunks completed (17.11GB out of 32.00GB) at average throughput 2.57Gbit/s
2022-01-11 21:41:38.233 | DEBUG    | skylark.replicate.replicator_client:monitor_transfer:323 - 1278/2048 chunks completed (19.97GB out of 32.00GB) at average throughput 2.59Gbit/s
2022-01-11 21:41:47.004 | DEBUG    | skylark.replicate.replicator_client:monitor_transfer:323 - 1473/2048 chunks completed (23.02GB out of 32.00GB) at average throughput 2.62Gbit/s
2022-01-11 21:41:56.556 | DEBUG    | skylark.replicate.replicator_client:monitor_transfer:323 - 1691/2048 chunks completed (26.42GB out of 32.00GB) at average throughput 2.64Gbit/s
2022-01-11 21:42:06.441 | DEBUG    | skylark.replicate.replicator_client:monitor_transfer:323 - 1901/2048 chunks completed (29.70GB out of 32.00GB) at average throughput 2.66Gbit/s
2022-01-11 21:42:23.727 | WARNING  | skylark.replicate.replicator_client:deprovision_gateway_instance:167 - Deprovisioning gateway skylark-aws-ce59f572adbc4cc99f8722c602941dde
2022-01-11 21:42:24.098 | WARNING  | skylark.replicate.replicator_client:deprovision_gateway_instance:167 - Deprovisioning gateway skylark-aws-093c86ccc4f74099b1b022d77de7aace
2022-01-11 21:42:24.293 | WARNING  | skylark.replicate.replicator_client:deprovision_gateway_instance:167 - Deprovisioning gateway skylark-aws-4c09ffd30a87436184bc9674ee79e410

{"total_runtime_s": 97.996321, "throughput_gbits": 2.6123429674467067, "monitor_status": "completed", "success": true}
```

</details>

When done, stop all instances started by Skylark by running `python skylark/benchmark/stop_all_instances.py`. If you use GCP, pass your GCP project ID in with the `--gcp-project` flag.
