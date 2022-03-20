# Welcome to Skylark's contributing guide

Thank you for investing your time in contributing to our project! Your contributions will have an impact forever on our project.

In this guide you will get an overview of the contribution workflow from opening an issue, creating a PR, reviewing, and merging the PR.

## TLDR
1. Use PRs to merge code into the Skylark repository. Add another project collaborator as a reviewer (or a maintainer will assign one).
2. Make sure to format your code using [Black](https://github.com/psf/black). Run `black -l 140 .` before merging your PR.
3. Check for major syntax errors or type signature bugs using [PyType](https://github.com/google/pytype). Run `pytype --config .pytype.cfg skylark` before merging your PR.

## New contributor guide

To get an overview of the project, read the [README](README.md). Here are some resources to help you get started with open source contributions:

- [Finding ways to contribute to open source on GitHub](https://docs.github.com/en/get-started/exploring-projects-on-github/finding-ways-to-contribute-to-open-source-on-github)
- [Set up Git](https://docs.github.com/en/get-started/quickstart/set-up-git)
- [GitHub flow](https://docs.github.com/en/get-started/quickstart/github-flow)
- [Collaborating with pull requests](https://docs.github.com/en/github/collaborating-with-pull-requests)
