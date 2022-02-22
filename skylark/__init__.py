from pathlib import Path

# paths
skylark_root = Path(__file__).parent.parent
key_root = skylark_root / "data" / "keys"
config_file = skylark_root / "data" / "config.json"
tmp_log_dir = Path("/tmp/skylark")

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
