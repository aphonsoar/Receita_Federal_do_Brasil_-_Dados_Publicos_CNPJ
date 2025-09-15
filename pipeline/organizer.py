import os
from config import Config

def classify_files():
    items = os.listdir(Config.EXTRACTED_FILES)
    groups = {
        "empresa": [],
        "estabelecimento": [],
        "socios": [],
        "simples": [],
        "cnae": [],
        "moti": [],
        "munic": [],
        "natju": [],
        "pais": [],
        "quals": [],
    }
    for item in items:
        for key in groups.keys():
            if key.upper() in item.upper():
                groups[key].append(item)
    return groups