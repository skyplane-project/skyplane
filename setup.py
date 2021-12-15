from setuptools import setup

setup(
    name='skylark',
    version='0.1',
    packages=["skylark"],
    python_requires=">=3.8",
    install_requires=[
        "boto3",
        # "cylp",
        "cvxpy",
        "cvxopt",
        "click",
        "google-cloud-compute",
        "google-api-python-client",
        "graphviz",
        "loguru",
        "matplotlib",
        "numpy",
        "pandas",
        "paramiko",
        "tqdm",
    ],
    extras_require={"test": ["black", "pytest", "ipython", "jupyter_console"]}
)