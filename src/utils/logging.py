"""
File system utilities.

This module currently contains functions for managing files and directories,
such as clearing a specified number of items from a folder based on age.
Note: The module name 'logging.py' is somewhat misleading for its current content.
Consider renaming to e.g., 'file_system_utils.py' in a future refactor.
"""
from os import remove, scandir, path
from shutil import rmtree
from typing import List # For potential future use, not strictly needed for current signature

# Function to clear items, keeping the 'n' newest ones
def clear_latest_items(dir_path: str, n_to_keep: int) -> None:
    """
    Clears items (files or folders) in the specified directory, keeping only
    the `n_to_keep` most recent ones.

    Items are sorted by modification time (oldest first). Then, items from the
    beginning of this sorted list are removed until only `n_to_keep` items remain.
    If the total number of items is less than or equal to `n_to_keep`, no items
    are removed.

    Args:
        dir_path (str): The path to the directory containing the items.
        n_to_keep (int): The number of most recent items to keep. Older items will be deleted.

    Raises:
        FileNotFoundError: If the specified `dir_path` is not found.
        OSError: If an error occurs during directory scanning or file/directory removal
                 (other than FileNotFoundError for the main path).
    """
    if not path.exists(dir_path):
        raise FileNotFoundError(f"Path not found: {dir_path}")

    # Get all items sorted by modification time (oldest first)
    # scandir is generally more efficient than listdir + stat for getting metadata.
    try:
        # Using entry.stat().st_mtime ensures we sort by modification time.
        # scandir returns DirEntry objects.
        all_items = sorted(scandir(dir_path), key=lambda entry: entry.stat().st_mtime)
    except OSError as e:
        # Handle potential errors during scandir or stat calls (e.g., permission issues)
        raise OSError(f"Error scanning directory {dir_path}: {e}") from e

    num_items_to_delete = len(all_items) - n_to_keep
    
    if num_items_to_delete > 0:
        # Iterate through the items that need to be deleted (the oldest ones)
        for i in range(num_items_to_delete):
            item_to_delete = all_items[i]
            try:
                if item_to_delete.is_file() or item_to_delete.is_symlink():
                    remove(item_to_delete.path)
                    # logger.debug(f"Deleted file: {item_to_delete.path}") # Optional logging
                elif item_to_delete.is_dir():
                    rmtree(item_to_delete.path)
                    # logger.debug(f"Deleted directory: {item_to_delete.path}") # Optional logging
                # else: skip other types like block devices, pipes, etc.
            except OSError as e:
                # Log or handle error for individual item deletion, but try to continue
                # This could be a logger.error() call.
                print(f"Error deleting item {item_to_delete.path}: {e}")
    # else: no items to delete, or n_to_keep is greater than or equal to total items.
