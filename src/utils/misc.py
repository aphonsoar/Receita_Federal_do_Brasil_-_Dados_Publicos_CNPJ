from sys import stdout
from os import path, remove, cpu_count
from zipfile import ZipFile
from requests import head
from os import makedirs
import subprocess
import re

from utils.logging import logger

def extract_zip_file(file_path, extracted_files_path):
    """
    Extracts a zip file to the specified directory.

    Args:
        file_path (str): The path to the zip file.
        extracted_files_path (str): The path to the directory where the files will be extracted.
    """
    with ZipFile(file_path, 'r') as zip_ref:
        zip_ref.extractall(extracted_files_path)
        
def list_zip_contents(zip_file_path):
    """
    Lists the filenames and other information about files within a zip archive.

    Args:
        zip_file_path (str): Path to the zip archive.

    Returns:
        list: A list of ZipInfo objects containing information about each file in the zip.
    """
    with ZipFile(zip_file_path, 'r') as zip_ref:
        return zip_ref.infolist()

def tuple_list_to_dict(tuple_list: list):
    """
    Converts a list of tuples into a dictionary.

    Args:
        tuple_list (list): A list of tuples.

    Returns:
        dict: A dictionary where the keys are the first elements of the tuples
              and the values are sets containing the second elements of the tuples.
    """
    dict_ = dict()
    
    for key, value in tuple_list: 
        if key not in dict_:
            dict_[key] = {value}
        else:
            dict_[key] = dict_[key].union({value})
    
    return dict_ 


def process_filename(filename):
    """
    Processes a filename by removing the extension and numbers, and converting it to lowercase.

    Args:
        filename (str): The filename to process.

    Returns:
        str: The processed filename.
    """
    # Split the filename at the last dot (".") to separate the base name and extension
    base_name, _ = filename.rsplit('.', 1)
    
    # Remove numbers from the base name using regular expressions
    return re.sub(r'\d+', '', base_name).lower()

def process_filenames(filenames):
    """
    Processes a list of filenames by removing extensions and numbers, and converting them to lowercase.

    Args:
        filenames (list): A list of strings representing filenames.

    Returns:
        list: A new list of strings with processed filenames.
    """
    processed_names = []
    for filename in filenames:
        processed_names.append(process_filename(filename))
    
    return list(set(processed_names))

def makedir(
    folder_name: str, 
    is_verbose: bool = False
):
    """
    Creates a new directory if it doesn't already exist.

    Args:
        folder_name (str): The name of the folder to create.
        is_verbose (bool, optional): Whether to log verbose information. Defaults to False.
    """
    if not path.exists(folder_name):
        makedirs(folder_name)
        
        if(is_verbose):
            logger.info('Folder: \n' + repr(str(folder_name)))

    else:
        if(is_verbose):
            logger.warn(f'Folder {repr(str(folder_name))} already exists!')

def get_max_workers():
    """
    Gets the maximum number of workers based on the number of CPU cores.

    Returns:
        int: The maximum number of workers.
    """
    # Get the number of CPU cores
    num_cores = cpu_count()

    # Adjust the number of workers based on the requirements
    # You should leave some cores free for other tasks
    max_workers = num_cores - 1 if num_cores else None

    return max_workers

def delete_var(var):
    """
    Deletes a variable from memory.

    Args:
        var: The variable to delete.
    """
    try:
        del var
    except:
        pass

def this_folder():
    """
    Gets the path of the current file.

    Returns:
        str: The path of the current file.
    """
    # Get the path of the current file
    current_file_path = path.abspath(__file__)

    # Get the folder containing the current file
    return path.dirname(current_file_path)

def check_diff(url, file_name):
    """
    Checks if the file on the server exists on disk and if it has the same size on the server.

    Args:
        url (str): The URL of the file on the server.
        file_name (str): The name of the file on disk.

    Returns:
        bool: True if the file has not been downloaded yet or if the sizes are different,
              False if the files are the same.
    """
    if not path.isfile(file_name):
        return True # not downloaded yet

    response = head(url)
    new_size = int(response.headers.get('content-length', 0))
    old_size = path.getsize(file_name)
    if new_size != old_size:
        remove(file_name)
        return True # different sizes

    return False # files are the same

# Create this bar_progress method which is invoked automatically from wget:
def bar_progress(current, total, width=80):
    """
    Displays a progress bar for a download.

    Args:
        current (int): The current number of bytes downloaded.
        total (int): The total number of bytes to download.
        width (int, optional): The width of the progress bar. Defaults to 80.
    """
    progress_message = "Downloading: %d%% [%d / %d] bytes - " % (current / total * 100, current, total)

    # Don't use print(): it will print in new line every time.
    stdout.write("\r{progress_message}")
    stdout.flush()


def update_progress(index, total, message):
    """
    Updates and displays a progress message.

    Args:
        index (int): The current index.
        total (int): The total number of items.
        message (str): The message to display.
    """
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
