# Building from source

## Prerequisite: install development dependencies
For Skyplane development, we use a variety of tools to check for code quality and to build documentation. To install these tools, run the following command:
```bash
pip install -r requirements-dev.txt
```

> <sub> **_NOTE:_** This requires Python version 3.9 or above. </sub>

## Setting up a developer environment

Skyplane is composed of the client (runs locally on a user's laptop) and gateway VMs (runs in respective clouds). Normally, the gateways use a pre-built nightly Docker image containing the latest build of the Skyplane gateway code (`public.ecr.aws/s6m1p0n8/skyplane:edge`). However, if you modify the gateway source (under `skyplane/gateway`), you will need to rebuild the gateway Docker image and push it to a container registry.

**Ensure you have [authenticated your Github account with Docker](https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-container-registry#authenticating-to-the-container-registry)**:

* Install docker
```bash
curl -fsSL https://get.docker.com -o get-docker.sh && sh get-docker.sh
```
* [Create a Personal Access Token](https://github.com/settings/tokens/new) with "write:packages" permissions
* Register the token with Docker:
```bash
echo <PERSONAL_ACCESS_TOKEN> | sudo docker login ghcr.io -u <GITHUB_USERNAME> --password-stdin
```

## Building and testing Skyplane

### Building and pushing a Skyplane Docker image
To package the code into a Docker image and push it to a container registry on your account, run the following command (substitute YOUR_GITHUB_USERNAME_HERE for your Github username):

```bash
export SKYPLANE_DOCKER_IMAGE=$(bash scripts/pack_docker.sh <YOUR_GITHUB_USERNAME_HERE>)
```

This will build the Skyplane Docker image for the gateway and push it ghcr.io under your user account. When running a Skyplane transfer, any provisioned gateways will pull this image from the `SKYPLANE_DOCKER_IMAGE` environment variable to ensure a reproducible environment.

#### First time setup: make sure ghcr image is "public"

By default, new packages on ghcr are private. To make the package public so gateways can download the image, [convert the package to public](https://docs.github.com/en/packages/learn-github-packages/configuring-a-packages-access-control-and-visibility#configuring-visibility-of-container-images-for-your-personal-account) (you only need to do this once):
* Navigate to your newly created package on Github at [https://github.com/users/<YOUR_GITHUB_USERNAME_HERE>/packages/container/package/skyplane](https://github.com/users/<YOUR_GITHUB_USERNAME_HERE>/packages/container/package/skyplane). Make sure to substitute your GitHub username for <YOUR_GITHUB_USERNAME_HERE>.
* Click on the "Package Settings button at the top right of the Skyplane package page with the gear icon.

![Package settings](https://user-images.githubusercontent.com/453850/182975365-61d5e6f8-9d95-4445-8bdf-171f53f55c68.png)

* Click the "Make public" button.

![Make public](https://user-images.githubusercontent.com/453850/182975358-e2b66f9b-963b-432d-9b3c-d03b22e5ea1a.png)

### Building the Skyplane client
We use [Poetry](https://python-poetry.org/) to manage package dependencies during development. For convenience, we provide a Poetry wrapper via `setup.py`. To build the client, install the Skyplane package in development mode. The package points to your current checked-out version of the code, and any edits to the Skyplane client will immediately apply to the `skyplane` CLI command.
```bash
pip install -e ".[aws,azure,gcp]"
```

### Testing a transfer
We can run the `skyplane` CLI to test a transfer. The CLI will read your `SKYPLANE_DOCKER_IMAGE` environment variable and use that Docker image when launching gateways.

```bash
skyplane init
skyplane cp s3://... s3://...
```

## Development Tips 
If testing transfers repeatedly, we recommend using the `--reuse-gateways` to reduce setup time. 

Skyplane has debugging tools to `ssh` into gateway instances, view detailed transfer logs, and query chunk states during transfers. See [Debugging Tools](debugging.md) for more. 
