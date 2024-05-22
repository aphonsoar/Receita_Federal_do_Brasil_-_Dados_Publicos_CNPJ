from sys import stdout
from os import path, remove, cpu_count
from requests import head
from os import path, makedirs
import pandas as pd
import subprocess

from utils.logging import logger
from core.constants import CHUNK_SIZE

def makedir(
    folder_name: str, 
    is_verbose: bool = False
):
    if not path.exists(folder_name):
        makedirs(folder_name)
        
        if(is_verbose):
            print('Folder: \n' + repr(str(folder_name)))

    else:
        if(is_verbose):
            print(f'Folder {repr(str(folder_name))} already exists!')

def get_max_workers():
    # Get the number of CPU cores
    num_cores = cpu_count()

    # Ajusta o numero de workers baseado nos requisitos
    # Você deve dexar alguns cores livres para outras tarefas
    max_workers = num_cores - 1 if num_cores else None

    return max_workers

def delete_var(var):
    try:
        del var
    except:
        pass

def this_folder():
    # Get the path of the current file
    current_file_path = path.abspath(__file__)

    # Get the folder containing the current file
    return path.dirname(current_file_path)

# Create this bar_progress method which is invoked automatically from wget:
def bar_progress(current, total, width=80):
    progress_message = "Downloading: %d%% [%d / %d] bytes - " % (current / total * 100, current, total)

    # Don't use print() as it will print in new line every time.
    stdout.write("\r" + progress_message)
    stdout.flush()

def check_diff(url, file_name):
    '''
    Verifica se o arquivo no servidor existe no disco e se ele tem o mesmo
    tamanho no servidor.
    '''
    if not path.isfile(file_name):
        return True # ainda nao foi baixado

    response = head(url)
    new_size = int(response.headers.get('content-length', 0))
    old_size = path.getsize(file_name)
    if new_size != old_size:
        remove(file_name)
        return True # tamanho diferentes

    return False # arquivos sao iguais

def update_progress(index, total, message):
    percent = (index * 100) / total
    curr_perc_pos = f"{index:0{len(str(total))}}/{total}"
    progress = f'{message} {percent:.2f}% {curr_perc_pos}'
    
    stdout.write(f'\r{progress}')


def get_line_count(filepath):
    """
    Uses the `wc -l` command to get the line count of a file.

    Args:
        filepath (str): Path to the file.

    Returns:
        int: Number of lines in the file (or None on error).
    """
    try:
        # Execute the 'wc -l' command and capture the output
        result = subprocess.run(["wc", "-l", filepath], capture_output=True)
        result.check_returncode()  # Raise exception for non-zero return code
        
        # Extract the line count (first element) and convert to integer
        line_count = int(result.stdout.decode().strip().split()[0])
        return line_count
    except subprocess.CalledProcessError as e:
        print(f"Error running wc command: {e}")
    
    return None
