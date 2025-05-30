"""
ZIP file manipulation utilities.

This module provides functions for creating, extracting, and listing contents
of ZIP archives using Python's `zipfile` module.
"""
from zipfile import ZipFile, ZipInfo # Import ZipInfo for type hinting
from typing import List # For type hinting

def create_sample_zip(filename: str, content: str) -> None:
    """
    Creates a sample ZIP file with the specified name and a single text file inside.

    The internal text file is named 'data.txt'. If the provided filename does not
    end with '.zip', the extension is appended automatically.

    Args:
        filename (str): The name of the ZIP file to create (e.g., "sample.zip").
        content (str): The string content to write into 'data.txt' within the ZIP file.
    """
    if not filename.endswith('.zip'):
        filename += '.zip'

    with ZipFile(filename, 'w') as zip_file:
        # Add a file named 'data.txt' with the provided content
        zip_file.writestr('data.txt', content)


def extract_zip_file(file_path: str, extracted_files_path: str) -> None:
    """
    Extracts all contents of a ZIP file to the specified directory.

    Args:
        file_path (str): The path to the ZIP file to be extracted.
        extracted_files_path (str): The directory where the contents of the
                                    ZIP file will be extracted. This directory
                                    will be created if it doesn't exist.

    Raises:
        zipfile.BadZipFile: If the file is not a ZIP file or is corrupted.
        FileNotFoundError: If `file_path` does not exist.
        # Other IOErrors are also possible.
    """
    with ZipFile(file_path, 'r') as zip_ref:
        zip_ref.extractall(extracted_files_path)


def list_zip_contents(zip_file_path: str) -> List[ZipInfo]:
    """
    Lists information about files within a ZIP archive.

    Args:
        zip_file_path (str): Path to the ZIP archive.

    Returns:
        List[ZipInfo]: A list of `zipfile.ZipInfo` objects, where each object
                       contains metadata about a file within the ZIP archive
                       (e.g., filename, size, modification date).

    Raises:
        zipfile.BadZipFile: If the file is not a ZIP file or is corrupted.
        FileNotFoundError: If `zip_file_path` does not exist.
    """
    with ZipFile(zip_file_path, 'r') as zip_ref:
        return zip_ref.infolist()