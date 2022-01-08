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
$ pip install -e .
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
$ python skylark/test/test_replicator_client.py \
    --gateway-docker-image $SKYLARK_DOCKER_IMAGE \
    --skip-upload \
    --n-chunks 2048 \
    --chunk-size-mb 4 \
    --num-gateways 1 \
    --src-region aws:us-west-1 \
    --dest-region aws:us-east-1
```
<details>
<summary>test_replicator_client.py result</summary>
<br>
 
```
$ python skylark/test/test_replicator_client.py \
    --gateway-docker-image $SKYLARK_DOCKER_IMAGE \
    --skip-upload \
    --n-chunks 2048 \
    --chunk-size-mb 4 \
    --num-gateways 1 \
    --src-region aws:us-west-1 \
    --dest-region aws:us-east-1

=================================================
  ______  _             _                 _
 / _____)| |           | |               | |
( (____  | |  _  _   _ | |  _____   ____ | |  _
 \____ \ | |_/ )| | | || | (____ | / ___)| |_/ )
 _____) )|  _ ( | |_| || | / ___ || |    |  _ (
(______/ |_| \_) \__  | \_)\_____||_|    |_| \_)
                (____/
=================================================

2022-01-07 00:27:02.485 | INFO     | __main__:main:88 - Creating replication client
2022-01-07 00:27:08.630 | DEBUG    | skylark.utils.utils:__exit__:24 - Cloud SSH key initialization: 6.14s
2022-01-07 00:27:08.630 | INFO     | __main__:main:99 - Provisioning gateway instances
2022-01-07 00:27:11.425 | DEBUG    | skylark.utils.utils:__exit__:24 - Refresh AWS instances: 2.79s
2022-01-07 00:27:11.425 | DEBUG    | skylark.utils.utils:__exit__:24 - Provision gateways: 0.00s
Starting up gateways:   0%|                                                                                                                                                        | 0/2 [00:00<?, ?it/s]
Wait for aws:us-west-1:i-058d5f644284b0ba9 to be ready: 0it [00:00, ?it/s]
Starting up gateways (aws:us-west-1): 100%|████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████| 2/2 [00:04<00:00,  2.34s/it]
2022-01-07 00:27:16.103 | DEBUG    | skylark.utils.utils:__exit__:24 - Configure gateways: 4.68s
2022-01-07 00:27:16.103 | INFO     | __main__:main:107 - Provisioned path aws:us-west-1 -> aws:us-east-1
2022-01-07 00:27:16.103 | INFO     | __main__:main:109 - 	[aws:us-west-1] http://3.101.126.215:8080/api/v1
2022-01-07 00:27:16.103 | INFO     | __main__:main:109 - 	[aws:us-east-1] http://52.207.248.254:8080/api/v1
Solving chunk path: 100%|████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████| 2048/2048 [00:00<00:00, 132442.18it/s]
2022-01-07 00:27:16.122 | DEBUG    | skylark.replicate.replicator_client:run_replication_plan:239 - Sending 2048 chunk requests to 3.101.126.215                                | 0/2048 [00:00<?, ?it/s]
2022-01-07 00:27:20.750 | INFO     | __main__:main:124 - 8.2fGByte replication job launched
Replication progress:  17%|███████████████████████▏                                                                                                                | 11.2G/65.5G [00:33<02:24, 377Mbit/s]
```

</details>

When done, stop all instances started by Skylark by running `python skylark/benchmark/stop_all_instances.py`. If you use GCP, pass your GCP project ID in with the `--gcp-project` flag.