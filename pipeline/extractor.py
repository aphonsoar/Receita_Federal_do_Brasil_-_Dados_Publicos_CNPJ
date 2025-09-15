import os
import zipfile
from config import Config

def extract_all(files):
    for f in files:
        full_path = os.path.join(Config.OUTPUT_FILES, f)
        print(f"Extraindo {f}...")
        with zipfile.ZipFile(full_path, 'r') as zip_ref:
            zip_ref.extractall(Config.EXTRACTED_FILES)