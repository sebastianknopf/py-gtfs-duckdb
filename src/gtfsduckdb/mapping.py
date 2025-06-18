import re

def map_id(id:str, mapping:dict) -> str|None:

    for key, value in mapping.items():
        rgx: str = key.replace('*', '.*')
        if re.match(rgx, id):
            return value
        
    return None