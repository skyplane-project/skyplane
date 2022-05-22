from setuptools import setup


setup(
    name="skyplane-dev",
    version="0.1",
    packages=["skyplane"],
    python_requires=">=3.7",
    install_requires=[
        # cloud integrations
        "awscrt",
        "azure-identity",
        "azure-mgmt-compute",
        "azure-mgmt-network",
        "azure-mgmt-resource",
        "azure-mgmt-storage",
        "azure-mgmt-authorization",
        "azure-storage-blob>=12.0.0",
        "boto3",
        "google-api-python-client",
        "google-auth",
        "google-cloud-compute",
        "google-cloud-storage",
        "grpcio-status>=1.33.2",
        # client dependencies
        "click",
        "halo",
        "pandas",
        "questionary",
        "sshtunnel",
        "typer",
        # shared dependencies
        "cachetools",
        "ilock",
        "paramiko",
        "termcolor",
        "tqdm",
    ],
    extras_require={
        "solver": [
            "cvxopt",
            "cvxpy",
            "graphviz",
            "matplotlib",
            "numpy",
        ],
        "gateway": [
            "flask",
            "pyopenssl",
            "werkzeug",
        ],
        "experiments": [
            "matplotlib",
            "numpy",
            "ray",
        ],
        "test": ["black", "ipython", "jupyter_console", "pytest", "pytype"],
    },
    entry_points={"console_scripts": [
        "skyplane=skyplane.cli.cli:app",
        "skylark=skyplane.cli.cli:app",
    ]},
)
