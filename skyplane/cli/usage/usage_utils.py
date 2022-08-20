import sys

import skyplane
from . import usage_constants


def compute_version_info():
    """Compute the versions of Skyplane and Python.
    Returns:
        A tuple containing the version information.
    """
    skyplane_version = skyplane.__version__
    python_version = ".".join(map(str, sys.version_info[:3]))
    return skyplane_version, python_version


def get_schema_version():
    """Get the schema version"""
    return usage_constants.SCHEMA_VERSION
