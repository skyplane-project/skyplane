import os
from enum import Enum, auto
import sys

import typer
import json
from typing import Any, Optional
from dataclasses import dataclass, asdict
from rich import print as rprint
from pathlib import Path
from skyplane import cloud_config, config_path
from skyplane.utils import logger
from . import usage_constants
from . import usage_utils


class UsageStatsEnabledness(Enum):
    ENABLED_EXPLICITLY = auto()
    DISABLED_EXPLICITLY = auto()
    ENABLED_BY_DEFAULT = auto()


def _map_type(value, boolean):
    if value is None:
        return False
    if boolean:
        if value.lower() in ["true", "yes", "1"]:
            return True
    else:
        if value.lower() in ["false", "no", "0"]:
            return True
    return False


def usage_stats_enabledness():
    # environment vairable has higher priority
    usage_stats_enabled_env_var = os.getenv(usage_constants.USAGE_STATS_ENABLED_ENV_VAR)
    if _map_type(usage_stats_enabled_env_var, False):
        return UsageStatsEnabledness.DISABLED_EXPLICITLY
    elif _map_type(usage_stats_enabled_env_var, True):
        return UsageStatsEnabledness.ENABLED_EXPLICITLY
    elif usage_stats_enabled_env_var is not None:
        raise ValueError(
            f"Valid value for {usage_constants.USAGE_STATS_ENABLED_ENV_VAR} "
            f"env var is (false/no/0) or (true/yes/1), but got {usage_stats_enabled_env_var}"
        )
    # then check in the config file
    usage_stats_enabled_config_var = None
    # TODO: Check the correct error
    try:
        usage_stats_enabled_config_var = cloud_config.get_flag("usage_stats")
    except FileNotFoundError:
        pass
    except Exception as e:
        logger.debug(f"Failed to load usage stats config {e}")

    if not usage_stats_enabled_config_var:
        return UsageStatsEnabledness.DISABLED_EXPLICITLY
    elif usage_stats_enabled_config_var:
        return UsageStatsEnabledness.ENABLED_EXPLICITLY
    elif usage_stats_enabled_config_var is not None:
        raise ValueError(
            f"Valid value for 'usage_stats' in {config_path}" f" is (false/no/0) or (true/yes/1), but got {usage_stats_enabled_config_var}"
        )

    return UsageStatsEnabledness.ENABLED_BY_DEFAULT


def usage_stats_enabled():
    return usage_stats_enabledness() is not UsageStatsEnabledness.DISABLED_EXPLICITLY


def set_usage_stats_via_config(value):
    current_status = usage_stats_enabledness()
    if current_status is UsageStatsEnabledness.DISABLED_EXPLICITLY:
        if (isinstance(value, bool) and not value) or (isinstance(value, str) and _map_type(value, False)):
            rprint("Usage stats collection is already disabled.")
            rprint(usage_constants.USAGE_STATS_REENABLE_MESSAGE)
            return
    elif current_status is UsageStatsEnabledness.ENABLED_EXPLICITLY:
        if (isinstance(value, bool) and value) or (isinstance(value, str) and _map_type(value, True)):
            rprint("Usage stats collection is already enabled.")
            rprint(usage_constants.USAGE_STATS_REENABLED_MESSAGE)
            return

    if (isinstance(value, bool) and not value) or (isinstance(value, str) and _map_type(value, False)):
        answer = usage_stats_disabled_reconfirmation()
        if not answer:
            # Do nothing if the user confirms not to disable
            return
        else:
            rprint(usage_constants.USAGE_STATS_REENABLE_MESSAGE)

    try:
        cloud_config.set_flag("usage_stats", value)
    except Exception as e:
        raise Exception("Failed to enable/disable by writing to" f"{config_path}") from e

    if cloud_config.get_flag("usage_stats"):
        rprint(usage_constants.USAGE_STATS_REENABLED_MESSAGE)


# Redacted: Should set environment variable directly from command line
# def set_usage_stats_via_env_var(value):
#     if value != "0" and value != "1":
#         raise ValueError("Valid value for usage_stats_env_var is 0 or 1, but got " + str(value))
#     os.environ[usage_constants.USAGE_STATS_ENABLED_ENV_VAR] = value


def show_usage_stats_prompt():
    usage_stats_var = usage_stats_enabledness()
    if usage_stats_var is UsageStatsEnabledness.DISABLED_EXPLICITLY:
        rprint(usage_constants.USAGE_STATS_DISABLED_MESSAGE)
    elif usage_stats_var in [UsageStatsEnabledness.ENABLED_BY_DEFAULT, UsageStatsEnabledness.ENABLED_EXPLICITLY]:
        rprint(usage_constants.USAGE_STATS_ENABLED_MESSAGE)
    else:
        raise Exception("Prompt message unknown.")


def usage_stats_disabled_reconfirmation():
    """
    Ask for confirmation when the user decides to disable metrics collection.
    default: the presumed answer if the user just hits <Enter>.
        It must be "no" (the default) or "yes".
    timeout: the number of seconds to wait for user to respond.
    """
    prompt = "Would you still like to opt out of sharing anonymous usage metrics?"
    rprint(usage_constants.USAGE_STATS_DISABLED_RECONFIRMATION_MESSAGE + "\n")
    return typer.confirm(prompt, default=False)


@dataclass
class UsageStatsToReport:
    """Usage stats to report"""

    #: The Skyplane version in use.
    skyplane_version: str
    #: The Python version in use.
    python_version: str
    #: The schema version of the report.
    schema_version: str
    #: The client id from SkyplaneConfig.
    client_id: str
    #: A random id of the transfer session.
    session_id: str
    #: The source region of the transfer session.
    source_region: str
    #: The destination region of the transfer session.
    destination_region: str
    #: The source cloud provider of the transfer session.
    source_cloud_provider: str
    #: The destination cloud provider of the transfer session.
    destination_cloud_provider: str
    #: The operating system in use.
    os: str
    #: When the transfer is started.
    session_start_timestamp_ms: int
    #: The collection of command arguments used in the transfer session.
    arguments_dict: dict


class UsageReportClient:
    """The client implementation for usage report.
    It is in charge of writing usage stats to the /tmp directory
    and report usage stats.
    """

    def write_usage_data(self, data: UsageStatsToReport, dir_path: Path) -> None:
        """Write the usage data to the directory.
        Params:
            data: Data to report
            dir_path: The path to the directory to write usage data.
        """
        dir_path = Path(dir_path)
        destination = dir_path / usage_constants.USAGE_STATS_FILE

        typer.secho(f"Storing usage information for transfer in {destination}", fg="yellow", err=True)
        with open(destination, "w+") as json_file:
            json_file.write(json.dumps(asdict(data)))

    # TODO: API not yet available
    # def report_usage_data(self, url: str, data: UsageStatsToReport) -> None:
    #     """Report the usage data to the usage server.
    #     Params:
    #         url: The URL to update resource usage.
    #         data: Data to report.
    #     Raises:
    #         requests.HTTPError if requests fails.
    #     """
    #     r = requests.request(
    #         "POST",
    #         url,
    #         headers={"Content-Type": "application/json"},
    #         json=asdict(data),
    #         timeout=10,
    #     )
    #     r.raise_for_status()
    #     return r
