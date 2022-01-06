from pathlib import Path

skylark_root = Path(__file__).parent.parent
key_root = skylark_root / "data" / "keys"


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
