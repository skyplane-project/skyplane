from setuptools import setup


setup(
    name="skylark",
    version="0.1",
    packages=["skylark"],
    python_requires=">=3.8",
    install_requires=[
        "awscrt",
        "boto3",
        "flask",
        "google-api-python-client",
        "google-cloud-compute",
        "loguru",
        "setproctitle",
        "tqdm",
        "werkzeug",
    ],
    extras_require={
        "all": [
            "cachetools",
            "click",
            "cvxopt",
            "cvxpy",
            "graphviz",
            "matplotlib",
            "numpy",
            "oslo.concurrency",
            "paramiko",
            "pandas",
            "questionary",
            "ray",
            "typer",
        ],
        "test": ["black", "ipython", "jupyter_console", "pytest", "pytype"],
    },
    entry_points={
        "console_scripts": [
            "skylark=skylark.cli.cli:app",
        ]
    },
)
