"""
Utility functions for schema-related operations, primarily focusing on grouping
file information and mapping file group names to table names.

These utilities help organize scraped file metadata into logical groups that
correspond to database tables, facilitating the ETL process.
"""
from typing import List, Dict
from utils.misc import normalize_filenames, get_date_range
from core.schemas import FileInfo, FileGroupInfo
from core.constants import TABLES_INFO_DICT
from setup.logging import logger

def file_group_name_to_table_name(file_group_name: str) -> str:
    """
    Maps a file group name to a table name.

    Args:
        file_group_name (str): The name of the file group.

    Returns:
        str: The corresponding table name.
    """
    matching_table = next(
        (table_name for table_name, table_info in TABLES_INFO_DICT.items() 
         if table_info['group'] == file_group_name), 
        None
    )
    if matching_table is None:
        raise ValueError(f"No table found for file group name: {file_group_name}")
    return matching_table

def create_file_groups(files_info: List[FileInfo]) -> List[FileGroupInfo]:
    """
    Creates a list of file groups based on the provided file information.

    Args:
        files_info (List[FileInfo]): A list of FileInfo objects, typically obtained
                                     from scraping the data source.
    
    Returns:
        List[FileGroupInfo]: A list of FileGroupInfo objects, where each object
                             represents a group of related files (e.g., parts of
                             the same table) along with consolidated metadata like
                             total size and date range.

    Note:
        If `file_group_name_to_table_name` fails to map a normalized group name
        to a table name (e.g., due to unexpected file naming conventions), an error
        is logged for that group, and it's excluded from the returned list.
    """
    # Build dictionaries from files_info for size and update_at lookup
    filename_to_update_at = {file_info.filename: file_info.updated_at for file_info in files_info}
    filename_to_size = {file_info.filename: file_info.file_size for file_info in files_info}
    
    # Normalize filenames and group them
    filenames = [file_info.filename for file_info in files_info]
    normalized_filenames = normalize_filenames(filenames)  # Returns a dict

    # Create file groups with accumulated size and date range
    file_groups = []
    for normalized_name, original_filenames in normalized_filenames.items():
        group_size_bytes = sum(filename_to_size[filename] for filename in original_filenames)
        group_date_range = get_date_range([filename_to_update_at[filename] for filename in original_filenames])

        try:
            table_name = file_group_name_to_table_name(normalized_name)
        except ValueError as e:
            logger.error(f"Error mapping file group {normalized_name}: {e}")
            continue

        file_groups.append(
            FileGroupInfo(
                name=normalized_name,
                elements=original_filenames,
                date_range=group_date_range,
                table_name=table_name,
                size_bytes=group_size_bytes
            )
        )

    return file_groups
