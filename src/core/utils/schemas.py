from typing import List, Dict

from utils.misc import normalize_filenames, get_date_range
from core.schemas import FileInfo, FileGroupInfo
from core.constants import TABLES_INFO_DICT

def file_group_name_to_table_name(file_group_name: str):
    """
    Maps file groups to table names.

    Args:
        file_groups_info: A list of FileGroupInfo objects.

    Returns:
        dict: A dictionary mapping file groups to table names.
    """
    return list(
        filter(
            lambda table_info_dict: table_info_dict[1]['group'] == file_group_name, 
            TABLES_INFO_DICT.items()
        )
    )[0][0]


def create_file_groups(files_info: List[FileInfo]) -> List[FileGroupInfo]:
    """
    Creates a list of file groups based on the provided file information.

    Args:
        files_info: A list of FileInfo objects.
    
    Returns:
        List[FileGroupInfo]: A list of FileGroupInfo objects.
    """

    zip_update_at={
        file_info.filename: file_info.updated_at for file_info in files_info
    }
    zip_file_size={
        file_info.filename: file_info.file_size for file_info in files_info
    }
    zip_filenames=[ file_info.filename for file_info in files_info ]
    normalized_dict=normalize_filenames(zip_filenames)

    groups = [
        {
            "name": normalized,
            "elements": originals,
            "size_bytes": sum([ zip_file_size[original] for original in originals ]),
            "date_range": get_date_range([ zip_update_at[original] for original in originals ]),
            "table_name": file_group_name_to_table_name(normalized)
        }
        for normalized, originals in normalized_dict.items()
    ]

    return [
        FileGroupInfo(
            name=group['name'], 
            elements=group['elements'], 
            date_range=group['date_range'],
            table_name=group['table_name'],
            size_bytes=group['size_bytes']
        )
        for group in groups
    ]

