from wget import download 
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from os import path
from functools import reduce

from database.models import AuditDB
from database.dml import (
    populate_table,
    generate_tables_indices
)
from utils.misc import (    
    get_file_size, 
    get_max_workers
)
from utils.zip import extract_zip_file
from core.constants import TABLES_INFO_DICT
from setup.logging import logger 

# Tabelas
tablename_list = [ table_name for table_name in TABLES_INFO_DICT.keys() ]
trimmed_tablename_list = [ table_name[:5] for table_name in TABLES_INFO_DICT.keys() ]
tablename_tuples = list(zip(tablename_list, trimmed_tablename_list))

####################################################################################################
## LER E INSERIR DADOS #############################################################################
####################################################################################################

def download_this_zipfile(
    audit: AuditDB,
    url: str,
    zip_filename: str,
    download_path: str,
    has_progress_bar: bool
):
    """
    Downloads a zip file from the given URL to the specified output path.

    Args:
        url (str): The URL of the file to download.
        zip_filename (str): The name of the zip file to download.
        download_path (str): The path to save the downloaded file.
        has_progress_bar (bool): Whether to display a progress bar during the download.

    Raises:
        OSError: If an error occurs during the download process.
    """

    full_path = path.join(download_path, zip_filename)
    
    try:
        logger.info(f"Downloading file {zip_filename}.")
        file_url = url + '/' + zip_filename
        
        # Assuming download updates progress bar itself
        if(has_progress_bar):
            download(file_url, out=download_path)
        else:
            download(file_url, out=download_path, bar=None)

    except Exception as e:
        summary=f"Error downloading {url}"
        message=f"{summary}: {e}"
        logger.error(message)
        
        return None
    
    finally:
        logger.info(f"Finished file download of file {zip_filename}...")

        # Update audit metadata
        audit.audi_downloaded_at = datetime.now()
        audit.audi_file_size_bytes = get_file_size(full_path)

        return audit

def extract_this_zipfile(
    audit: AuditDB,
    zip_filename: str,
    download_path: str,
    extracted_path: str
):
    """
    Extracts a zip file to the specified directory.

    Args:
        zip_filename (str): The name of the zip file to extract.
        download_path (str): The path to the directory containing the zip file.
        extracted_path (str): The path to the directory where the file will be extracted.
    
    Raises:
        OSError: If an error occurs during the extraction process.
    """
    
    full_path = path.join(download_path, zip_filename)

    try:
        logger.info(f"Extracting file {zip_filename}...")
        # Assuming extraction updates progress bar itself
        extract_zip_file(full_path, extracted_path)
    
    except Exception as e:
        summary=f"Error extracting file {zip_filename}"
        message=f"{summary}: {e}"
        logger.error(message)
    
    finally:
        logger.info(f"Finished file extraction of file {zip_filename}.")
        
        audit.audi_processed_at = datetime.now()
        return audit


def download_and_extract_file(
    audit: AuditDB,
    url: str,                       
    zip_filename: str,
    download_path: str,
    extracted_path: str,
    has_progress_bar: bool,
):
    # Download file
    audit = download_this_zipfile(
        audit, url, zip_filename, download_path, has_progress_bar
    )

    # Extract file
    audit = extract_this_zipfile(
        audit, zip_filename, download_path, extracted_path
    )

    return audit

def download_and_extract_files(
    audit: AuditDB, 
    url: str, 
    download_path: str, 
    extracted_path: str, 
    has_progress_bar: bool
):
    """
    Downloads a file from the given URL to the specified output path and extracts it.

    Args:
        url (str): The URL of the file to download.
        download_path (str): The path to save the downloaded file.
        extracted_path (str): The path to the directory where the file will be extracted.
        has_progress_bar (bool): Whether to display a progress bar during the download.

    Raises:
        OSError: If an error occurs during the download or extraction process.
    """
    for zip_filename in audit.audi_filenames:
        download_and_extract_file(
            audit, url, zip_filename, download_path, extracted_path, has_progress_bar 
        )
        
        
def get_rf_filenames_parallel(
    url: str,
    audits: list,
    output_path: str, 
    extracted_path: str,
    max_workers = get_max_workers()
):
    """
    Downloads and extracts the files from the Receita Federal base URLs in parallel.

    Args:
        base_files (list): A list of base file names to be downloaded.
        output_path (str): The path to save the downloaded files.
        extracted_path (str): The path to the directory where the files will be extracted.
        max_workers (int, optional): The maximum number of concurrent downloads. Defaults to get_max_workers().
    """
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []

        for audit in audits:
            task = executor.submit(
                download_and_extract_files, 
                audit, url, output_path, extracted_path, False
            )

            futures.append(task)

        results = []
        for future in tqdm(as_completed(futures), total=len(audits)):
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                summary="An error occurred on parallelization"
                logger.error(f"{summary}: {e}")
        
        return results

def get_rf_filenames_serial(
    url: str,
    audits: list,
    output_path: str, 
    extracted_path: str, 
):
    """
    Downloads and extracts the files from the Receita Federal base URLs serially.

    Args:
        base_files (list): A list of base file names to be downloaded.
        output_path (str): The path to save the downloaded files.
        extracted_path (str): The path to the directory where the files will be extracted.
    """
    error_count = 0
    error_basefiles = []
    total_count = len(audits)
    audits_ = []
    audits = sorted(audits, key=lambda x: x.audi_file_size_bytes)

    for index, audit in enumerate(audits):
        try:    
            # Download and extract file
            audit_ = download_and_extract_files(
                audit, url, output_path, extracted_path, True
            )
            
            audits_.append(audit_)

        except OSError as e:
            summary = f"Erro ao baixar ou extrair arquivo da tabela {audit.audi_table_name}"
            message = f"{summary}: {e}"
            logger.error(message)
            error_count += 1
            error_basefiles.append(audit.audi_table_name)
        
        finally:
            progress_message=f"({index}/{total_count}) arquivos baixados"
            error_message=f"{error_count} erros: {error_basefiles}"
            logger.info(f"{progress_message}. {error_message}")


def download_and_extract_RF_data(
    zips_url: str,
    layout_url: str,
    audits: list, 
    output_path: str, 
    extracted_path: str,
    is_parallel=True,
):
    """
    Downloads files from the Receita Federal base URLs to the specified output path and extracts them.

    Args:
        base_files (list): A list of base file names to be downloaded.
        output_path (str): The path to save the downloaded files.
        extracted_path (str): The path to the directory where the files will be extracted.
        is_parallel (bool, optional): Whether to download and extract the files in parallel. Defaults to True.
        max_workers (int, optional): The maximum number of concurrent downloads. Defaults to get_max_workers().

    Raises:
        OSError: If an error occurs during the download or extraction process.
    """
    max_workers = get_max_workers()
    
    # Check if parallel processing is enabled
    is_parallel = max_workers > 1 and is_parallel
    
    # Download RF zip files
    if(is_parallel):
        audits = get_rf_filenames_parallel(zips_url, audits, output_path, extracted_path, max_workers)
    else:
        audits = get_rf_filenames_serial(zips_url, audits, output_path, extracted_path)

    # Download layout
    download(layout_url, out=output_path, bar=None)
    logger.info("Layout baixado com sucesso!")
    
    return audits

def get_RF_data(
    data_url, layout_url, audits, 
    from_folder, to_folder, is_parallel=True
):
    """
    Retrieves and extracts the data from the Receita Federal.

    Args:
        to_folder (str): The path to the directory where the downloaded files will be saved.
        from_folder (str): The path to the directory where the extracted files will be stored.
        is_parallel (bool, optional): Whether to download and extract the files in parallel. Defaults to True.
    """
    return download_and_extract_RF_data(
        data_url, layout_url, audits, 
        from_folder, to_folder, is_parallel
    )


def load_RF_data_on_database(database, from_folder, audit_metadata):
    """
    Populates the database with data from multiple tables.

    Args:
        database (Database): The database object.
        from_folder (str): The folder path where the files are located.
        files (dict): A dictionary containing the file names for each table.

    Returns:
        None
    """
    table_to_filenames = audit_metadata.tablename_to_zipfile_to_files
    zip_tablenames_set = set(table_to_filenames.keys())
    
    # Load data
    for index, (table_name, zipfile_content_dict) in enumerate(table_to_filenames.items()):
        table_files_list = list(zipfile_content_dict.values())
        
        table_filenames = sum(table_files_list, [])

        # Populate this table
        populate_table(database, table_name, from_folder, table_filenames)

        # Update audit metadata
        audit_metadata.audit_list[index].audi_inserted_at = datetime.now()

    zip_filenames = list(
        reduce(
            lambda x, y: list(x) + list(y), 
            map(lambda x: x.keys(), table_to_filenames.values())
        )
    )
    logger.info(f"Carga dos arquivos zip {zip_filenames} finalizado!")

    # Generate tables indices
    tables_with_indices = {'empresa', 'estabelecimento', 'socios', 'simples'}
    tables_renew_indices = list(zip_tablenames_set.intersection(tables_with_indices))

    has_new_tables = len(tables_renew_indices) != 0
    if(has_new_tables):
        generate_tables_indices(database.engine, tables_renew_indices)

    return audit_metadata


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
