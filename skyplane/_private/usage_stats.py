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


# def usage_stats_config_path():
#     return os.getenv(
#         "SKYPLANE_USAGE_STATS_CONFIG_PATH", os.path.expanduser("~/.skyplane/usage_stats_config.json")
#     )


def usage_stats_enabledness():
    # environment vairable has higher priority
    usage_stats_enabled_env_var = os.getenv(usage_constants.USAGE_STATS_ENABLED_ENV_VAR)
    if usage_stats_enabled_env_var == "0":
        return UsageStatsEnabledness.DISABLED_EXPLICITLY
    elif usage_stats_enabled_env_var == "1":
        return UsageStatsEnabledness.ENABLED_EXPLICITLY
    elif usage_stats_enabled_env_var is not None:
        raise ValueError(
            f"Valid value for {usage_constants.USAGE_STATS_ENABLED_ENV_VAR} " f"env var is 0 or 1, but got {usage_stats_enabled_env_var}"
        )
    # then check in the config file
    usage_stats_enabled_config_var = None
    try:
        usage_stats_enabled_config_var = cloud_config.get_flag("usage_stats")
    except FileNotFoundError:
        pass
    except Exception as e:
        logger.debug(f"Failed to load usage stats config {e}")

    if usage_stats_enabled_config_var == "0":
        return UsageStatsEnabledness.DISABLED_EXPLICITLY
    elif usage_stats_enabled_config_var == "1":
        return UsageStatsEnabledness.ENABLED_EXPLICITLY
    elif usage_stats_enabled_config_var is not None:
        raise ValueError(f"Valid value for 'usage_stats' in {config_path}" f" is 0 or 1, but got {usage_stats_enabled_config_var}")

    return UsageStatsEnabledness.ENABLED_BY_DEFAULT


def usage_stats_enabled():
    return usage_stats_enabledness() is not UsageStatsEnabledness.DISABLED_EXPLICITLY


def set_usage_stats_via_config(value):
    if value == "0":
        answer = usage_stats_disabled_reconfirmation()
        if not answer:
            # Do nothing if the user confirms not to disable
            return

    try:
        cloud_config.set_flag("usage_stats", value)
    except Exception as e:
        raise Exception("Failed to enable/disable by writing to" f"{config_path}") from e


def set_usage_stats_via_env_var(value) -> None:
    os.environ[usage_constants.USAGE_STATS_ENABLED_ENV_VAR] = "1" if value else "0"


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


def usage_stats_disabled_reconfirmation(default="yes", timeout=10):
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
