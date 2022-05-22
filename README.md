# Skyplane: A Unified Data Layer for the Multi-Cloud


[![Docker](https://github.com/skyplane-project/skyplane/actions/workflows/docker-publish.yml/badge.svg?branch=main)](https://github.com/skyplane-project/skyplane/actions/workflows/docker-publish.yml)

<img src="https://gist.githubusercontent.com/parasj/d67e6e161ea1329d4509c69bc3325dcb/raw/232009efdeb8620d2acb91aec111dedf98fdae18/skylark.jpg" width="200px">

Skyplane is lifting cloud object stores to the Sky.

## Instructions to build and run demo
Skyplane is composed of two components: A ReplicatorClient that runs locally on your machine that is responsible for provisioning instances and coordinating replication jobs and a GatewayDaemon that runs on each provisioned instance to actually copy data through the overlay.

This package represents both components as a single binary. Docker builds a single container with the GatewayDaemon and pushes it to the Github Container Registry (ghcr.io). After provisioning an instance, a GatewayDaemon is started by launching that container. Therefore, it's simple and fast to launch a new Gateway.

### Requirements
* Python 3.7 or greater
* Docker
    * **Ensure you have authenticated your Github account with Docker**: https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-container-registry#authenticating-to-the-container-registry
    * TLDR:
        * (1) Install docker with `curl -fsSL https://get.docker.com -o get-docker.sh && sh get-docker.sh`
        * (2) Create a Personal Access Token at https://github.com/settings/tokens/new with "write:packages" permissions
        * (3) Run `echo <PERSONAL_ACCESS_TOKEN> | sudo docker login ghcr.io -u <GITHUB_USERNAME> --password-stdin`
* AWS:
	* (1) Install AWS CLI with `sudo apt install awscli`
	* (2) Configure AWS by running `aws configure` and input the necessary information. See https://docs.aws.amazon.com/powershell/latest/userguide/pstools-appendix-sign-up.html. 
	* (3) Ensure that you have a sufficient AWS vCPU limit in any regions you intend to use
	* (4) Install netcat with `sudo apt install netcat`

### Building and deploying the gateway

First, clone and enter the skyplane directory:
```
$ git clone https://github.com/skyplane-project/skyplane
$ cd skyplane
```

Then, configure cloud credentials as needed:

* AWS: `aws configure`
* GCS: `gcloud auth application-default login`
* Azure: `az login`

Finally, install and initalize Skyplane:
```
$ pip install -e ".[all]"
$ skyplane init
```

To run a sample transfer, first build a new version of the GatewayDaemon Docker image and push it to ghcr.io (ensure you are authenticated as above):
```
$ source scripts/pack_docker.sh
```
<details>
<summary>pack_docker result</summary>
<br>

```
$ pip install -e ".[all]"
$ source scripts/pack_docker.sh
Building docker image
[+] Building 0.0s (2/2) FINISHED
 => [internal] load build definition from Dockerfile                                                                                               0.0s
 => => transferring dockerfile: 2B                                                                                                                 0.0s
 => [internal] load .dockerignore                                                                                                                  0.0s
 => => transferring context: 2B                                                                                                                    0.0s
failed to solve with frontend dockerfile.v0: failed to read dockerfile: open /var/lib/docker/tmp/buildkit-mount683951637/Dockerfile: no such file or directory
Uploading docker image to ghcr.io/skyplane-project/skyplane:local-PotRzrFT
The push refers to repository [ghcr.io/skyplane-project/skyplane]
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
SKYPLANE_DOCKER_IMAGE=ghcr.io/skyplane-project/skyplane:local-PotRzrFT
```

</details>

The script will export the new image (ghcr.io/skyplane-project/skyplane:local-PotRzrFT in this example) to an environment variable (`SKYPLANE_DOCKER_IMAGE`). Ensure you use `source` so the environment variable is published to your shell.

### Running a basic cloud to cloud transfer job
We then run the ReplicatorClient with that new Docker image (stored in `$SKYPLANE_DOCKER_IMAGE`):
```
$ skyplane cp [s3/gs/azure]://[source-bucket-name]/[source-key-prefix] [s3/gs/azure]://[dest-bucket-name]/[dest-key-prefix]
```
<details>
<summary>skyplane cp result</summary>
<br>
 
```
$ skyplane cp s3://skyplane-example-us-east-1/ s3://skyplane-example-us-west-1/
11:34:48 [DEBUG] Cloud SSH key initialization: 3.23s
11:35:20 [DEBUG] Provisioning instances and waiting to boot: 31.87s
11:35:24 [DEBUG] Install docker: 2.79s
11:35:24 [DEBUG] Install docker: 2.50s
11:35:37 [DEBUG] Starting gateway aws:us-west-1:i-09dda9567bcf9ecad, host: 52.53.229.126: Docker pull: 13.10s
11:35:37 [DEBUG] Starting gateway aws:us-west-1:i-09dda9567bcf9ecad, host: 52.53.229.126: Starting gateway container
11:35:39 [DEBUG] Starting gateway aws:us-west-1:i-09dda9567bcf9ecad, host: 52.53.229.126: Gateway started fabfc1cd5aefa24c0cb5d5572501b19ff33e483cadfcccddc9bd0d90368c5329
11:36:05 [DEBUG] Starting gateway aws:us-east-1:i-08a9b4f70ee2caca3, host: 54.158.252.172: Docker pull: 39.93s
11:36:05 [DEBUG] Starting gateway aws:us-east-1:i-08a9b4f70ee2caca3, host: 54.158.252.172: Starting gateway container
11:36:14 [DEBUG] Starting gateway aws:us-east-1:i-08a9b4f70ee2caca3, host: 54.158.252.172: Gateway started 18ebc8a3a04b632375a71ae88e18286d402364e467b30b00ad3168391a914eaf
11:36:15 [DEBUG] Install gateway package on instances: 55.05s
11:36:15 [INFO]  Provisioned ReplicationTopologyGateway(region='aws:us-east-1', instance=0): http://54.158.252.172:8888/container/18ebc8a3a04b
11:36:15 [INFO]  Provisioned ReplicationTopologyGateway(region='aws:us-west-1', instance=0): http://52.53.229.126:8888/container/fabfc1cd5aef
11:36:15 [INFO]  Batch 0 size: 4387690 with 3 chunks
11:36:15 [DEBUG] Building chunk requests: 0.00s
11:36:15 [DEBUG] Sending 3 chunk requests to 54.158.252.172
11:36:15 [DEBUG] Dispatch chunk requests: 0.27s
11:36:15 [INFO]  0.00GByte replication job launched
0/3 chunks done (0.00 / 0.00GB, 0.00Gbit/s, ETA=unknown)                                                                                                    
Replication: average 0.02Gbit/s: 100%|███████████████████████████████████████████████████████████████████████████████| 33.5M/33.5M [00:02<00:00, 17.0Mbit/s]
11:36:17 [INFO]  Copying gateway logs from aws:us-east-1:i-08a9b4f70ee2caca3
11:36:17 [INFO]  Copying gateway logs from aws:us-west-1:i-09dda9567bcf9ecad
11:36:21 [DEBUG] Wrote profile to /tmp/skyplane/transfer_2022-03-29T11:36:17.755039/traceevent_5cb6dfbf-fac6-4ce6-a61b-1b813e83723d.json, visualize using `about://tracing` in Chrome
11:36:22 [WARN]  Deprovisioning 2 instances
11:36:23 [WARN]  Deprovisioned aws:us-west-1:i-09dda9567bcf9ecad
11:36:24 [WARN]  Deprovisioned aws:us-east-1:i-08a9b4f70ee2caca3

{"total_runtime_s": 1.692787, "throughput_gbits": 0.019311843710588934, "monitor_status": "completed", "success": true}
```

</details>

When done, stop all instances started by Skyplane by running:

```skyklark deprovision```

<details>
<summary>skyplane deprovision result</summary>
<br>

```
$ skyplane deprovision
No GCP project ID given, so will only deprovision AWS instances
Deprovisioning 3 instances
Deprovisioning (aws:ap-northeast-1): 100%|██████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████| 3/3 [00:01<00:00,  2.33it/s]
```

</details>
