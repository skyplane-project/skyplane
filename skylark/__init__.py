import os

from pathlib import Path
from skylark.compute.utils import query_which_cloud

from skylark.config import SkylarkConfig

# paths
skylark_root = Path(__file__).parent.parent
config_root = Path("~/.skylark").expanduser()
config_root.mkdir(exist_ok=True)

if "SKYLARK_CONFIG" in os.environ:
    config_path = Path(os.environ["SKYLARK_CONFIG"]).expanduser()
else:
    config_path = config_root / "config"

key_root = config_root / "keys"
tmp_log_dir = Path("/tmp/skylark")
tmp_log_dir.mkdir(exist_ok=True)

# header
def print_header():
    header = "\n"
    header += """=================================================
  ______  _             _                 _     
 / _____)| |           | |               | |    
( (____  | |  _  _   _ | |  _____   ____ | |  _ 
 \____ \ | |_/ )| | | || | (____ | / ___)| |_/ )
 _____) )|  _ ( | |_| || | / ___ || |    |  _ ( 
(______/ |_| \_) \__  | \_)\_____||_|    |_| \_)
                (____/                          
================================================="""
    header += "\n"
    print(header, flush=True)


# definitions
KB = 1024
MB = 1024 * 1024
GB = 1024 * 1024 * 1024
if config_path.exists():
    cloud_config = SkylarkConfig.load_config(config_path)
else:
    cloud_config = SkylarkConfig()
