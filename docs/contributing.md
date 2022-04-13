# Contributing to Skyplane

Welcome to Skyplane! Everyone is welcome to contribute to Skyplane. We are always looking for new features and improvements and we value everyone's input. There are many ways to contribute to Skyplane:

* Answering questions on Skyplane's [discussions page](https://github.com/skyplane-project/skyplane/discussions)
* Improving Skyplane's documentation
* Filing bug reports or reporting sharp edges via [Github issues](https://github.com/skyplane-project/skyplane/issues)
* Contributing to our [codebase](https://github.com/skyplane-project/skyplane)

We welcome pull requests, in particular for those issues marked with [good first issue](https://github.com/skyplane-project/skyplane/issues?q=is%3Aopen+is%3Aissue+label%3A%22good+first+issue%22).

For other proposals or larger features, we ask that you open a new GitHub [Issue](https://github.com/skyplane-project/skyplane/issues/new) or [Discussion](https://github.com/skyplane-project/skyplane/discussions/new).

## Setting up a developer environment

Skyplane is composed of the client (runs locally on a user's laptop) and gateway VMs (runs in respective clouds). Normally, the gateways use a pre-built nightly Docker image containing the latest build of the Skyplane gateway code (`ghcr.io/skyplane-project/skyplane:main`). However, if you modify the gateway source (under `skylark/gateway`), you will need to rebuild the gateway Docker image and push it to a container registry.

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

After making a change to the Skyplane source, we need to rebuild the gateway Docker image:

```bash
$ DOCKER_BUILDKIT=1 docker build -t skylark .
```

We now need to push the Docker image to a container registry. Replace `ghcr.io/skyplane-project/skyplane` with your container registry if you are developing against a fork. We autogenerate a short random hash for the image tag.

```bash
$ export SKYLARK_DOCKER_IMAGE="ghcr.io/skyplane-project/skyplane:local-$(openssl rand -hex 16)"
$ sudo docker tag skylark $SKYLARK_DOCKER_IMAGE
$ sudo docker push $SKYLARK_DOCKER_IMAGE
```

To build the client, install the Skyplane package in development mode. The package points to your current checked-out version of the code, and any edits to the Skyplane client will immediately apply to the `skylark` CLI command.
```bash
$ pip install -e .
```

We can run the `skylark` CLI to test a transfer. The CLI will read your `SKYLARK_DOCKER_IMAGE` environment variable and use that Docker image when launching gateways.

```bash
$ skylark init
$ skylark cp s3://... s3://...
```

## Submitting pull requests

Basic knowledge of git is assumed. To contribute to Skyplane:

1. Fork the Skyplane repository to create a copy of the project in your own account.
2. Set up a developer environment as described as above.
3. Create a development branch (`git checkout -b feature_name`)
4. Test your changes manually using `skylark cp` and with the unit test suite:
```bash
$ pytest -n auto skylark/test
```
5. Ensure your code is autoformatted and passes type checks:
```bash
$ pip install black pytype
$ black -l 140 .
$ pytype --config .pytype.cfg skylark
```
5. Commit your changes using a [descriptive commit message](https://cbea.ms/git-commit/).
6. Create a pull request on the main Skyplane repo from your fork. Consult [Github Help](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/about-pull-requests) for more details.