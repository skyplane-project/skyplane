#!/usr/bin/env python3

import setuptools

if __name__ == "__main__":
    setuptools.setup(
        packages=["skyplane"],
        entry_points={
            "console_scripts": [
                "skylark=skyplane.cli.cli:app",
                "skyplane=skyplane.cli.cli:app",
            ]
        },
    )
