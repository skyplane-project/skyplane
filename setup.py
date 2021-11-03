from setuptools import setup

setup(
    name='skylark',
    version='0.1',
    packages=["skylark"],
    python_requires=">=3.8",
    install_requires=[
        "click",
        "tqdm",
        "loguru",
        "boto3",
        "paramiko",
        "matplotlib",
        "numpy",
        "pandas",
    ],
    extras_require={"test": ["black", "pytest", "ipython", "jupyter_console"]}
)