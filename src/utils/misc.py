from sys import stdout
from os import path, remove, cpu_count, stat
from zipfile import ZipFile
from requests import head
from shutil import rmtree
from unicodedata import normalize
from os import makedirs
import subprocess
import re
from tqdm import tqdm
import wget

from setup.logging import logger

def repeat_token(token: str, n: int):
    """
    Repeat a token n times.

    Args:
        token (str): The token to repeat.
        n (int): The number of times to repeat the token.

    Returns:
        token (str): The token repeated n times.
        n (int): The number of times to repeat the token.
    """

    return ''.join([token] * n)

def invert_dict_list(dict_: dict):
    """
    Inverts a dictionary where the values are lists of keys.
    
    Args:
        dict_ (dict): The dictionary to be inverted.
        
    Returns:
        dict: The inverted dictionary where the keys are the values from the original dictionary
              and the values are the corresponding keys from the original dictionary.
    """
    inverted_dict = dict()
    for key, values_list in dict_.items():
        for value in values_list:
            if value not in inverted_dict:
                inverted_dict[value] = [key]
            else: 
                inverted_dict[value].append(key)
    
    return inverted_dict
    
def get_file_size(file_path):
    """
    This function retrieves the size of a file in bytes.

    Args:
        file_path (str): The path to the file.

    Returns:
        int: The size of the file in bytes, or None if the file doesn't exist or can't be accessed.

    Raises:
        OSError: If an error occurs while accessing the file.
    """
    try:
        # Use os.stat to get file information in a platform-independent way
        file_stats = stat(file_path)
        return file_stats.st_size
    
    except OSError as e:
        # Raise OSError for potential file access issues
        raise OSError(f"Error accessing file: {file_path}. Reason: {e}") from e

    except Exception as e:
    # Catch unexpected exceptions and re-raise with more context
        raise Exception(f"Unexpected error getting file size for {file_path}: {e}") from e


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

def convert_to_bytes(size_str):
  """
  This function converts a size string (e.g., "22K", "321M") into bytes.

  Args:
      size_str (str): The size string to convert.

  Returns:
      int: The size in bytes, or None if the format is invalid.
  """
  size_value = float(size_str[:-1])  # Extract numerical value
  size_unit = size_str[-1].upper()  # Get the unit (K, M, G)

  unit_multiplier = {
      'K': 1024,
      'M': 1024 * 1024,
      'G': 1024 * 1024 * 1024
  }

  if size_unit in unit_multiplier:
    return int(size_value * unit_multiplier[size_unit])
  else:
    return None  # Handle invalid units

def normalize_filename(filename):
    """
    This function normalizes a filename by removing the extension and numbers, 
    and converting it to lowercase.

    Args:
        filename (str): The filename to normalize.

    Returns:
        str: The normalized filename.
    """

    # Remove extension
    base_name = path.splitext(filename)[0]

    # Remove number (assuming numbers are at the end)
    base_name = re.sub(r'\d+$', '', base_name)

    # Normalize accentuation (assuming NFD normalization)
    base_name = normalize('NFD', base_name).casefold()

    return base_name

def normalize_filenames(filenames):
  """
  This function normalizes a list of filenames and creates a dictionary with key as normalized filename and value as original zip filename.

  Args:
      filenames (list): A list of filenames to normalize.

  Returns:
      dict: A dictionary with normalized filenames as keys and original filenames as values.
  """
  normalized_dict = {}
  for filename in filenames:
    base_name = normalize_filename(filename)

    # Create dictionary entry
    if base_name not in normalized_dict:
      normalized_dict[base_name] = [filename]
    else: 
      normalized_dict[base_name].append(filename)
    
  return normalized_dict

from datetime import timedelta


def get_date_range(timestamps):
  """
  This function finds the minimum and maximum date in a list of datetime timestamps.
  If there's only one element, it returns the same date and a timedelta of 0 days.

  Args:
      timestamps (list): A list of datetime timestamps.

  Returns:
      tuple: A tuple containing the minimum date and maximum date (or the same date 
              and a timedelta of 0 days if there's only one element).
  """
  if not timestamps:
    return None  # Handle empty list case

  if len(timestamps) == 1:
      return timestamps[0], timestamps[0] + timedelta(days=0)
  else:
      return min(timestamps), max(timestamps)

def remove_folder(folder: str):
    try:
        rmtree(folder)
    except Exception as e:
        logger.error(f"Error deleting folder {folder}: {e}")