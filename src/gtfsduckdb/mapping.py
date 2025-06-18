import re

def map_id(id:str, mapping:dict) -> str|None:

    for key, value in mapping.items():
        if re.match(key, id):
            return value
        
    return id