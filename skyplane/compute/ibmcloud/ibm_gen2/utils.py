"""
Created on 21 Feb 2023

@author: gilvernik
"""

import os


def load_yaml_config(config_filename):
    import yaml

    try:
        with open(config_filename, "r") as config_file:
            data = yaml.safe_load(config_file)
    except FileNotFoundError:
        data = {}

    return data


def dump_yaml_config(config_filename, data):
    import yaml

    if not os.path.exists(os.path.dirname(config_filename)):
        os.makedirs(os.path.dirname(config_filename))

    with open(config_filename, "w") as config_file:
        yaml.dump(data, config_file, default_flow_style=False)


def delete_yaml_config(config_filename):
    if os.path.exists(config_filename):
        os.remove(config_filename)
