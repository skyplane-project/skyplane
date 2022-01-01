import os
import sys
from pathlib import Path

skylark_root = Path(__file__).parent.parent
data_root = skylark_root / "data"
instance_log_root = skylark_root / "data" / "logs"
key_root = skylark_root / "data" / "keys"


def print_header():
    header = """=================================================
  ______  _             _                 _     
 / _____)| |           | |               | |    
( (____  | |  _  _   _ | |  _____   ____ | |  _ 
 \____ \ | |_/ )| | | || | (____ | / ___)| |_/ )
 _____) )|  _ ( | |_| || | / ___ || |    |  _ ( 
(______/ |_| \_) \__  | \_)\_____||_|    |_| \_)
                (____/                          
================================================="""
    print(header, flush=True)
