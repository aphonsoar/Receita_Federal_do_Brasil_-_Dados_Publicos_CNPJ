import os
import requests

def makedirs(path: str):
    if not os.path.exists(path):
        os.makedirs(path)

def check_diff(url: str, file_name: str) -> bool:
    """Verifica se precisa baixar novamente"""
    if not os.path.isfile(file_name):
        return True
    response = requests.head(url, timeout=30)
    new_size = int(response.headers.get('content-length', 0))
    old_size = os.path.getsize(file_name)
    if new_size != old_size:
        os.remove(file_name)
        return True
    return False