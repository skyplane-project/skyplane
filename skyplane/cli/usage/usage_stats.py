from enum import Enum, auto
import os
import json
import sys
import time
from skyplane.utils import logger
from skyplane import cloud_config, config_path
from . import usage_constants


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
            f"Valid value for {usage_constants.USAGE_STATS_ENABLED_ENV_VAR} " f"env var is (false/no/0) or (true/yes/1), but got {usage_stats_enabled_env_var}"
        )
    # then check in the config file
    usage_stats_enabled_config_var = None
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
        raise ValueError(f"Valid value for 'usage_stats' in {config_path}" f" is (false/no/0) or (true/yes/1), but got {usage_stats_enabled_config_var}")

    return UsageStatsEnabledness.ENABLED_BY_DEFAULT


def usage_stats_enabled():
    return usage_stats_enabledness() is not UsageStatsEnabledness.DISABLED_EXPLICITLY


def set_usage_stats_via_config(value):
    if (isinstance(value, bool) and not value) or (isinstance(value, str) and _map_type(value, False)):
        answer = usage_stats_disabled_reconfirmation()
        if not answer:
            # Do nothing if the user confirms not to disable
            return

    try:
        cloud_config.set_flag("usage_stats", value)
    except Exception as e:
        raise Exception("Failed to enable/disable by writing to" f"{config_path}") from e


# Redacted: Should set environment variable directly from command line
# def set_usage_stats_via_env_var(value) -> None:
#     os.environ[usage_constants.USAGE_STATS_ENABLED_ENV_VAR] = "1" if value else "0"


def show_usage_stats_prompt():
    usage_stats_var = usage_stats_enabledness()
    if usage_stats_var is UsageStatsEnabledness.DISABLED_EXPLICITLY:
        print(usage_constants.USAGE_STATS_DISABLED_MESSAGE)
    elif usage_stats_var is UsageStatsEnabledness.ENABLED_BY_DEFAULT:
        print(usage_constants.USAGE_STATS_ENABLED_BY_DEFAULT_MESSAGE)
    elif usage_stats_var is UsageStatsEnabledness.ENABLED_EXPLICITLY:
        print(usage_constants.USAGE_STATS_ENABLED_MESSAGE)
    else:
        raise Exception("Prompt message unknown.")


def usage_stats_disabled_reconfirmation(default="no", timeout=10):
    """
    Ask for confirmation when the user decides to disable metrics collection.
    default: the presumed answer if the user just hits <Enter>.
        It must be "yes" (the default) or "no".
    timeout: the number of seconds to wait for user to respond.
    """
    valid = {"yes": True, "y": True, "no": False, "n": False}
    prompt = "Would you still like to opt out of sharing anonymous usage metrics? [y/N] \n"
    sys.stdout.write(usage_constants.USAGE_STATS_DISABLED_RECONFIRMATION_MESSAGE + "\n")
    sys.stdout.write(prompt)
    start_time = time.time()
    while True:
        if (time.time() - start_time) >= timeout:
            sys.stdout.write("Response timeout: " + str(timeout) + " seconds. Please try again.\n")
            # return False so it will not disable
            return False
        choice = input().lower()
        if default is not None and choice == "":
            return valid[default]
        elif choice in valid:
            return valid[choice]
        else:
            sys.stdout.write("Please respond with ['yes', 'no', 'y', 'n', " "].\n")
