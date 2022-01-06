# Skylark: A Unified Data Layer for the Multi-Cloud

<img src="https://gist.githubusercontent.com/parasj/d67e6e161ea1329d4509c69bc3325dcb/raw/232009efdeb8620d2acb91aec111dedf98fdae18/skylark.jpg" width="350px">
Skylark is lifting cloud object stores to the Sky.

## Instructions to run demo
Skylark is composed of two components: A ReplicatorClient that runs locally on your machine that is responsible for provisioning instances and coordinating replication jobs and a GatewayDaemon that runs on each provisioned instance to actually copy data through the overlay.

This package represents both components as a single binary. Docker builds a single container with the GatewayDaemon and pushes it to the Github Container Registry (ghcr.io). After provisioning an instance, a GatewayDaemon is started by launching that container. Therefore, it's simple and fast to launch a new Gateway.

To run a sample replication, first build a new version of the GatewayDaemon Docker image and push it to ghcr.io:

```$ . scripts/pack_docker.sh```
<details>
<summary>pack_docker result</summary>
<br>

```
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

By running with `.` (source), the script will export the new image (ghcr.io/parasj/skylark:local-PotRzrFT) to an environment variable (`SKYLARK_DOCKER_IMAGE`).

We then run the ReplicatorClient with that new Docker image:
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
</details>
