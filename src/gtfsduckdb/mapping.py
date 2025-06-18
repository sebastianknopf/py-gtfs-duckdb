import re

def map_id(id:str, mapping:dict) -> str|None:

    rgx: str = id.replace('*', '.*')
    for key, value in mapping.items():
        if re.match(rgx, key):
            return value
        
    return None