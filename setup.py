from setuptools import setup

setup(
    name='skydata',
    version='0.1',
    packages=["skydata"],
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
    extras_require={"test": ["pytest", "ipython", "jupyter_console"]}
)