# Building from source

## Setting up a developer environment

Skyplane is composed of the client (runs locally on a user's laptop) and gateway VMs (runs in respective clouds). Normally, the gateways use a pre-built nightly Docker image containing the latest build of the Skyplane gateway code (`public.ecr.aws/s6m1p0n8/skyplane:edge`). However, if you modify the gateway source (under `skyplane/gateway`), you will need to rebuild the gateway Docker image and push it to a container registry.

**Ensure you have [authenticated your Github account with Docker](https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-container-registry#authenticating-to-the-container-registry)**:

* Install docker
```bash
$ curl -fsSL https://get.docker.com -o get-docker.sh && sh get-docker.sh
```
* [Create a Personal Access Token](https://github.com/settings/tokens/new) with "write:packages" permissions
* Register the token with Docker:
```bash
$ echo <PERSONAL_ACCESS_TOKEN> | sudo docker login ghcr.io -u <GITHUB_USERNAME> --password-stdin
```

## Building and testing Skyplane

### Building Docker image for gateway
**TLDR:** We've packaged the code to build and push the gateway Docker image into a single script:
```
source scripts/pack_docker.sh
```

`source` will run the script and capture the exported `SKYPLANE_DOCKER_IMAGE` environment variable.

#### Manual steps to build and push the Skyplane gateway Docker image

After making a change to the Skyplane source, we need to rebuild the gateway Docker image:

```bash
$ DOCKER_BUILDKIT=1 docker build -t skyplane .
```

We now need to push the Docker image to a container registry. Replace `ghcr.io/skyplane-project/skyplane` with your container registry if you are developing against a fork. We autogenerate a short random hash for the image tag.

```bash
$ export SKYPLANE_DOCKER_IMAGE="ghcr.io/skyplane-project/skyplane:local-$(openssl rand -hex 16)"
$ sudo docker tag skyplane $SKYPLANE_DOCKER_IMAGE
$ sudo docker push $SKYPLANE_DOCKER_IMAGE
```

### Building the Skyplane client
We use [Poetry](https://python-poetry.org/) to manage package dependencies during development. For convenience, we provide a Poetry wrapper via `setup.py`. To build the client, install the Skyplane package in development mode. The package points to your current checked-out version of the code, and any edits to the Skyplane client will immediately apply to the `skyplane` CLI command.
```bash
$ pip install -e .
```

### Testing a transfer
We can run the `skyplane` CLI to test a transfer. The CLI will read your `SKYPLANE_DOCKER_IMAGE` environment variable and use that Docker image when launching gateways.

```bash
$ skyplane init
$ skyplane cp s3://... s3://...
```

## Development Tips 
If testing transfers repeatedly, we recommend using the `--reuse-gateways` to reduce setup time. 

Skyplane has debugging tools to `ssh` into gateway instances, view detailed transfer logs, and query chunk states during transfers. See [Debugging Tools](debugging.md) for more. 
