from sys import stdout
from os import path, remove, cpu_count
from requests import head
from os import path, makedirs
import pandas as pd
import subprocess
import re

from utils.logging import logger
from core.constants import CHUNK_SIZE

def tuple_list_to_dict(tuple_list: list):
    dict_ = dict()
    
    for key, value in tuple_list: 
        if key not in dict_:
            dict_[key] = {value}
        else:
            dict_[key] = dict_[key].union({value})
    
    return dict_ 


def process_filename(filename):
    
    # Split the filename at the last dot (".") to separate the base name and extension
    base_name, _ = filename.rsplit('.', 1)
    
    # Remove numbers from the base name using regular expressions
    return re.sub(r'\d+', '', base_name).lower()

def process_filenames(filenames):
    """
    This function takes a list of filenames and returns a new list with 
    extensions and numbers removed, and the names converted to lowercase.

    Args:
        filenames: A list of strings representing filenames.

    Returns:
        A new list of strings with processed filenames.
    """
    processed_names = []
    for filename in filenames:
        processed_names.append(process_filename(filename))
    
    return list(set(processed_names))

def makedir(
    folder_name: str, 
    is_verbose: bool = False
):
    if not path.exists(folder_name):
        makedirs(folder_name)
        
        if(is_verbose):
            logger.info('Folder: \n' + repr(str(folder_name)))

    else:
        if(is_verbose):
            logger.warn(f'Folder {repr(str(folder_name))} already exists!')

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

# Create this bar_progress method which is invoked automatically from wget:
def bar_progress(current, total, width=80):
    progress_message = "Downloading: %d%% [%d / %d] bytes - " % (current / total * 100, current, total)

    # Don't use print(): it will print in new line every time.
    stdout.write("\r{progress_message}")
    stdout.flush()


def update_progress(index, total, message):
    percent = (index * 100) / total
    curr_perc_pos = f"{index:0{len(str(total))}}/{total}"
    progress = f'{message} {percent:.2f}% {curr_perc_pos}'
    
    stdout.write(f'\r{progress}')
    stdout.flush()

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
        logger.error(f"Error running wc command: {e}")
    
    return None
