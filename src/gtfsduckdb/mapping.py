import re
import logging

def map_id(id:str, mapping:dict) -> str|None:

    for key, value in mapping.items():
        if re.match(key, id):
            return value
        else:
            logging.warning(f"{key} does not match {value}")
        
    return id