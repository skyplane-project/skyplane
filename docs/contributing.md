# Contributing to Skyplane
Welcome to Skyplane! Everyone is welcome to contribute to Skyplane. We are always looking for new features and improvements and we value everyone's input. 

## Ways to Contribute
There are many ways to contribute to Skyplane:
* Answering questions on Skyplane's [discussions page](https://github.com/skyplane-project/skyplane/discussions)
* Improving Skyplane's documentation
* Filing bug reports or reporting sharp edges via [Github issues](https://github.com/skyplane-project/skyplane/issues)
* Contributing to our [codebase](https://github.com/skyplane-project/skyplane). 

## Code Contributions
We welcome pull requests, in particular for those issues marked with [good first issue](https://github.com/skyplane-project/skyplane/issues?q=is%3Aopen+is%3Aissue+label%3A%22good+first+issue%22). 

For other proposals or larger features, we ask that you open a new GitHub [Issue](https://github.com/skyplane-project/skyplane/issues/new) or [Discussion](https://github.com/skyplane-project/skyplane/discussions/new). We especially encourage external contributions to support additional cloud providers and object store endpoints. 

To see more on how to setup a development environment, see the [Development Guide](development_guide.md).

### Submitting pull requests
Before you submit a pull request, make sure to complete the following steps: 
1. Fork the Skyplane repository to create a copy of the project in your own account.
2. Set up a developer environment as described in the [Development Guide](development_guide.md).
3. Create a development branch (`git checkout -b feature_name`)
4. Test your changes manually using `skyplane cp` and with the unit test suite:
```bash
$ pytest -n auto skyplane/test
```
5. Ensure your code is autoformatted and passes type checks:
```bash
$ pip install black pytype
$ black -l 140 .
$ pytype --config .pytype.cfg skyplane
```
5. Commit your changes using a [descriptive commit message](https://cbea.ms/git-commit/).
6. Create a pull request on the main Skyplane repo from your fork. Consult [Github Help](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/about-pull-requests) for more details.