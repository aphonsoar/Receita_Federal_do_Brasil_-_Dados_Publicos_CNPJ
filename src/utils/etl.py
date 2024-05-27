from setup.logging import logger
from core.constants import (
    TABLES_INFO_DICT, 
    DADOS_RF_URL, 
    LAYOUT_URL
)

tablename_list = [ table_name for table_name in TABLES_INFO_DICT.keys() ]
trimmed_tablename_list = [ table_name[:5] for table_name in TABLES_INFO_DICT.keys() ]
tablename_tuples = list(zip(tablename_list, trimmed_tablename_list))

def get_filename_to_tablename(item: str):
    """
    Returns the tablename corresponding to the given filename.

    Args:
        item (str): The filename for which to find the corresponding tablename.

    Returns:
        str: The tablename corresponding to the given filename.

    Raises:
        None

    """
    
    has_label_map = lambda label: item.lower().find(label[1].lower()) > -1
    this_tablename_tuple = list(filter(has_label_map, tablename_tuples))
    
    spans_multiple_tables = len(this_tablename_tuple) > 1
    if(spans_multiple_tables):
        logger.error(f"File {item} span multiple tables {this_tablename_tuple}")
    
    has_alias = len(this_tablename_tuple) != 0
    if(has_alias):
        this_tablename = this_tablename_tuple[0][0]
        
        has_alias = len(this_tablename_tuple) != 0    
        if(has_alias):
            
            this_tablename = this_tablename_tuple[0][0]
            
            return item

def get_zip_to_tablename(zip_file_dict):
    """
    Retrieves the filenames of the extracted files from the Receita Federal.

    Args:
        extracted_files_path (str): The path to the directory containing the extracted files.

    Returns:
        dict: A dictionary containing the filenames grouped by table name.
    """
    # Separar arquivos:
    zip_to_tablename = {
        zipped_file: []
        for zipped_file in zip_file_dict.keys()
    }

    # Filtrar arquivos
    for zipfile_filename, unzipped_files in zip_file_dict.items():
        for item in unzipped_files:
            has_label_map = lambda label: item.lower().find(label[1].lower()) > -1
            this_tablename_tuple = list(filter(has_label_map, tablename_tuples))
            
            has_alias = len(this_tablename_tuple) != 0
            if(has_alias):
                this_tablename = this_tablename_tuple[0][0]
                zip_to_tablename[zipfile_filename].append(this_tablename)

    return zip_to_tablename
