import yaml
import os
import re
import subprocess
import sys
import tempfile
import time
from enum import Enum
import inquirer
from inquirer import errors
from ibm_platform_services import IamIdentityV1
from ibm_cloud_sdk_core.authenticators import IAMAuthenticator
import ibm_cloud_sdk_core


CACHE = {}
ARG_STATUS = Enum("STATUS", "VALID INVALID MISSING")  # variable possible status.


class MSG_STATUS(Enum):
    ERROR = "[ERROR]"
    WARNING = "[WARNING]"
    SUCCESS = "[SUCCESS]"


NEW_INSTANCE = "Create a new"  # guaranteed substring in every 'create new instance' option prompted to user.


def get_option_from_list(msg, choices, default=None, choice_key="name", do_nothing=None, validate=True, carousel=True):
    """returns the user's choice out of given 'choices'.
    :param msg - msg to display the user before presenting choices
    :param choices - list of choices to present user with
    :param default - default choice out of the lists of choices presented to user
    :param choice_key - if 'choices' is a dict `choice_key` represents the key associated with the desired values
    :param do_nothing:str - an additional choice for 'choices' to be appended to the list
    :param carousel - when set to true allows user to rotate from last to first choice and vice-a-versa"""
    if len(choices) == 0 and do_nothing == None:
        error_msg = f"There no option for {msg}"
        print(error_msg)
        raise Exception(error_msg)

    if len(choices) == 1 and not do_nothing:
        return choices[0]

    if choice_key:
        choices_keys = [choice[choice_key] for choice in choices]
    else:
        choices_keys = [choice for choice in choices]

    if do_nothing:
        choices_keys.insert(0, do_nothing)

    questions = [
        inquirer.List(
            "answer",
            message=msg,
            choices=choices_keys,
            default=default,
            validate=validate,
            carousel=carousel,
        ),
    ]
    answers = inquirer.prompt(questions, raise_keyboard_interrupt=True)

    # now find the object by name in the list
    if answers["answer"] == do_nothing:
        return do_nothing
    else:
        if choice_key:
            return next((x for x in choices if x[choice_key] == answers["answer"]), None)
        else:
            return next((x for x in choices if x == answers["answer"]), None)


def inquire_user(
    msg, choices, default=None, choice_key="name", create_new_instance=None, handle_strings=False, validate=True, carousel=True
):

    """returns the user's choice out of given 'choices'.
    :param str create_new_instance: when initialized adds a 'create' option that allows the user
                          to create an instance rather than to opt for one of the options.
    :param bool handle_strings: when set to True handles input of list of strings instead of list of dicts.
    :param str choice_key: creates options list presented to user (choices_keys) using this key"""

    # options to be displayed to user
    choices_keys = [choice[choice_key] for choice in choices] if not handle_strings else choices

    if create_new_instance:
        choices_keys.insert(0, color_msg(create_new_instance, style=Style.ITALIC))

    if len(choices_keys) == 0:
        raise Exception(f"No options were found to satisfy the following request: {msg}")

    if len(choices_keys) == 1:
        if create_new_instance:
            print(
                color_msg(
                    f"\nNo existing instances were found in relation to the request: " f"'{msg}'. Create a new one to proceed. ",
                    color=Color.RED,
                )
            )
        else:
            print(
                color_msg(
                    f"single option was found in response to the request: '{msg}'." f" \n{choices[0]} was automatically chosen\n",
                    color=Color.LIGHTGREEN,
                )
            )
            return choices[0]

    questions = [
        inquirer.List(
            "answer",
            message=msg,
            choices=choices_keys,
            default=default,
            validate=validate,
            carousel=carousel,
        )
    ]
    answers = inquirer.prompt(questions, raise_keyboard_interrupt=True)

    if create_new_instance and create_new_instance in answers["answer"]:
        return create_new_instance
    elif handle_strings:  # returns the string the user chose
        return answers["answer"]
    else:  # returns the object belonging to user's choice
        return next((x for x in choices if x[choice_key] == answers["answer"]), None)


def find_obj(objects, msg, obj_id=None, obj_name=None, default=None, do_nothing=None):
    """returns object matching the specified keys' values: obj['id'] or obj['name']. Else, returns a user chosen object.
    obj_id and obj_name are the values for: 'id' and 'name' - common keys in the API response format of IBM SDK.
    """
    obj = None
    if obj_id:
        # validating that obj exists
        obj = next((obj for obj in objects if obj["id"] == obj_id), None)
        if not obj:
            raise Exception(f"Object with specified id {obj_id} not found")
    if obj_name:
        obj = next((obj for obj in objects if obj["name"] == obj_name), None)

    if not obj:  # no matching values were found, collecting user's choice from 'objects'.
        obj = get_option_from_list(msg, objects, default=default, do_nothing=do_nothing)
    return obj


def find_name_id(objects, msg, obj_id=None, obj_name=None, default=None, do_nothing=None):
    obj = find_obj(objects, msg, obj_id, obj_name, default, do_nothing)
    if do_nothing and obj == do_nothing:
        return None, None

    return obj["name"], obj["id"]


def validate_not_empty(answers, current):
    if not current:
        raise errors.ValidationError("", reason=f"Key name can't be empty")
    return True


def validate_exists(answers, current):
    if not current or not os.path.exists(os.path.abspath(os.path.expanduser(current))):
        raise errors.ValidationError("", reason=f"File {current} doesn't exist")
    return True


def get_region_by_endpoint(endpoint):
    res = re.search("//(.+?).iaas.cloud.ibm.com", endpoint)
    if res:
        return res.group(1)


def find_default(template_dict, objects, name=None, id=None, substring=False):
    """returns an object in 'objects' who's value for the field 'name' or 'id' equals that of item[name]/item[id] for an item in template_dict

    Args:
        template_dict (dict): dict from an existing configuration file that may contain desired values.
        objects (list of dicts): list of dicts that may contain a key with the desired value.
        name (str): name of the key of an object within 'template_dict' that contains the desired value, to be evaluated against obj['name'] of 'objects'.
        id (str): name of the key of an object within 'template_dict' that contains the desired value, to be evaluated against obj['id'] of 'objects'
        substring (bool): if set to true, the substring value of the desired val in template_dict is tested against the value of 'objects'
    """

    val = None
    for k, v in template_dict.items():
        if isinstance(v, dict):
            return find_default(v, objects, name=name, id=id)
        else:
            if name:
                key = "name"
                if k == name:
                    val = v
            elif id:
                key = "id"
                if k == id:
                    val = v

            if val:
                if not substring:
                    obj = next((obj for obj in objects if obj[key] == val), None)
                else:
                    obj = next((obj for obj in objects if val in obj[key]), None)
                if obj:
                    return obj["name"]


def free_dialog(msg, default=None, validate=True):
    """Returns user's answer to an open-ended question"""
    question = [inquirer.Text("answer", message=msg, default=default, validate=validate)]
    answer = inquirer.prompt(question, raise_keyboard_interrupt=True)
    return answer


def password_dialog(msg, default=None, validate=True):
    question = [inquirer.Password("answer", message=msg, default=default, validate=validate)]
    answer = inquirer.prompt(question, raise_keyboard_interrupt=True)
    return answer


def get_confirmation(msg, default=None):
    questions = [
        inquirer.Confirm(
            "answer",
            message=msg,
            default=default,
        ),
    ]
    answer = inquirer.prompt(questions, raise_keyboard_interrupt=True)

    return answer


def retry_on_except(retries, sleep_duration, error_msg="", default=None):
    """A decorator that calls the decorated function up to a number of times equals to 'retires' with a given
    'sleep_duration' in between.
     if the function failed the allotted number of retries, a default value will be returned,
     granted that one was provided, else the config tool will be terminated."""

    def retry_on_except_warpper(func):
        def func_wrapper(*args, **kwargs):
            msg = error_msg  # transferring the value via a mediator is necessary due to decorator's restrictions.
            for retry in range(retries):
                try:
                    result = func(*args, **kwargs)
                    return result

                except Exception as e:
                    msg += str(e)
                    if retry < retries - 1:  # avoid sleeping after last failure
                        time.sleep(sleep_duration)
            if default:
                print(color_msg(f"{msg}", color=Color.RED))
                return default
            else:
                print(color_msg(f"{msg}\nConfig tool was terminated", color=Color.RED))
                sys.exit(1)

        return func_wrapper

    return retry_on_except_warpper


def run_cmd(cmd):
    """runs a command via cli while constantly printing the output from the read pipe"""

    # small buffer size forces the process not to buffer the output, thus printing it instantly.
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, bufsize=1, shell=True)
    for line in iter(process.stdout.readline, b""):
        print(bytes(line).decode())
    process.stdout.close()
    process.wait()


def verify_paths(input_path, output_path, verify_config=False):
    """:returns a valid input and output path files, in accordance with provided paths.
    if a given path is invalid, and user is unable to rectify, a default path will be chosen in its stead."""

    def _is_valid_input_path(path):
        if not os.path.isfile(path):
            print(color_msg(f"\nError - Path: '{path}' doesn't point to a file. ", color=Color.RED))
            return False
        return True

    def _is_valid_output_path(path):
        """:returns path if it's either a valid absolute path, or a file name to be appended to current directory"""
        dir_file = path.rsplit("/", 1)
        prefix_directory = dir_file[0]
        if len(dir_file) == 1 or os.path.isdir(prefix_directory):
            return path
        else:
            print(color_msg(f"{prefix_directory} doesn't lead to an existing directory", color=Color.RED))

    def _prompt_user(path, default_config_file, verify_func, request, default_msg):
        while True:
            if not path:
                print(color_msg(default_msg, color=Color.LIGHTGREEN))
                return default_config_file

            if verify_func(path):
                return path
            else:
                path = free_dialog(request)["answer"]

    if not verify_config:
        input_path = _prompt_user(
            input_path,
            "",
            _is_valid_input_path,
            "Provide a path to your existing config file, or leave blank to configure from template",
            "Using default input file\n",
        )
    output_path = _prompt_user(
        output_path,
        tempfile.mkstemp(suffix=".yaml")[1],
        _is_valid_output_path,
        "Provide a custom path for your config file, or leave blank for default output location",
        "Using default output path\n",
    )
    return input_path, output_path


def verify_iam_api_key(answers, apikey, iam_endpoint=None):
    """Terminates the config tool if no IAM_API_KEY matching the provided value exists"""

    iam_identity_service = IamIdentityV1(authenticator=IAMAuthenticator(apikey, url=iam_endpoint))
    try:
        iam_identity_service.get_api_keys_details(iam_api_key=apikey)
    except ibm_cloud_sdk_core.api_exception.ApiException:
        raise errors.ValidationError("", reason=color_msg(f"No IAmApiKey matching the given value {apikey} was found.", Color.RED))
    return True


def color_msg(msg, color=None, style=None, background=None):
    """reformat a given string and returns it, matching the desired color,style and background in Ansi color code.
    parameters are Enums of the classes: Color, Style and Background."""

    init = "\033["
    end = "\033[m"
    font = ""
    if color:
        font += color.value
    if style:
        font = font + ";" if font else font
        font += style.value

    if background:
        font = font + ";" if font else font
        font += background.value

    return init + font + "m" + msg + end


class Color(Enum):
    BLACK = "30"
    RED = "31"
    GREEN = "32"
    BROWN = "33"
    BLUE = "34"
    PURPLE = "35"
    CYAN = "36"
    LIGHTGREY = "37"
    DARKGREY = "90"
    LIGHTRED = "91"
    LIGHTGREEN = "92"
    YELLOW = "93"
    LIGHTBLUE = "94"
    PINK = "95"
    LIGHTCYAN = "96"


class Style(Enum):
    RESET = "0"
    BOLD = "01"
    DISABLE = "02"
    ITALIC = "03"
    UNDERLINE = "04"
    REVERSE = "07"
    STRIKETHROUGH = "09"


class Background(Enum):
    BLACK = "40"
    RED = "41"
    GREEN = "42"
    ORANGE = "43"
    BLUE = "44"
    PURPLE = "45"
    CYAN = "46"
    LIGHTGREY = "47"


def load_base_config(default_config_suffix=""):
    """returns a default base configuration dict for the chosen backend"""
    dir_path = os.path.dirname(os.path.realpath(__file__))
    input_file = f"{dir_path}/skyplane/defaults{default_config_suffix}.yaml"

    base_config = None
    with open(input_file) as f:
        base_config = yaml.safe_load(f)

    return base_config
