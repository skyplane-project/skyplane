import yaml
from skyplane.compute.ibmcloud.gen2.utils import color_msg, Color, verify_paths
from skyplane.compute.ibmcloud.gen2.skyplane import load_config, MODULES


def create_vpc(*args, **kwargs):
    """A programmatic avenue to create configuration files. To be used externally by user."""

    _, output_file = verify_paths(None, None)

    # now update base config with backend specific params
    base_config = load_config(*args, **kwargs)

    for module in MODULES:
        print(module)
        base_config = module(base_config).verify(base_config)

    with open(output_file, "w") as outfile:
        yaml.dump(base_config, outfile, default_flow_style=False)

    print("\n\n=================================================")
    print(color_msg(f"Cluster config file: {output_file}", color=Color.LIGHTGREEN))
    print("=================================================")

    return output_file
