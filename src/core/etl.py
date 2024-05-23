from wget import download 
from os import path, listdir
from timy import timer
from tqdm import tqdm
from shutil import rmtree
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from os import listdir, path

from utils.misc import extract_zip_file
from core.models import AuditDB
from utils.logging import logger
from utils.misc import (
    check_diff, 
    get_max_workers, 
    list_zip_contents, 
    process_filename
)
from core.scrapper import scrap_RF
from utils.database import populate_database, generate_database_indices
from core.constants import TABLES_INFO_DICT, DADOS_RF_URL, LAYOUT_URL
from utils.models import create_audit

####################################################################################################
## LER E INSERIR DADOS #############################################################################
####################################################################################################

@timer('Ler dados da Receita Federal')
def get_RF_filenames(extracted_files_path):
    """
    Retrieves the filenames of the extracted files from the Receita Federal.

    Args:
        extracted_files_path (str): The path to the directory containing the extracted files.

    Returns:
        dict: A dictionary containing the filenames grouped by table name.
    """
    # Files:
    items = [name for name in listdir(extracted_files_path) if name.endswith('')]

    # Separar arquivos:
    files = {
        table_name: [] for table_name in TABLES_INFO_DICT.keys()
    }

    tablename_list = [table_name for table_name in TABLES_INFO_DICT.keys()]
    trimmed_tablename_list = [table_name[:5] for table_name in TABLES_INFO_DICT.keys()]

    tablename_tuples = list(zip(tablename_list, trimmed_tablename_list))

    for item in items:
        has_label_map = lambda label: item.lower().find(label[1].lower()) > -1
        this_tablename_tuple = list(filter(has_label_map, tablename_tuples))

        if(len(this_tablename_tuple) != 0):
            this_tablename = this_tablename_tuple[0][0]
            files[this_tablename].append(item)

    return files


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
    file_name = path.basename(url)
    full_path = path.join(download_path, file_name)

    if not path.exists(full_path) or check_diff(url, full_path):
        try:
            # Assuming download updates progress bar itself
            if(has_progress_bar):
                download(url, out=download_path)
                
            else:
                download(url, out=download_path, bar=None)

            audit.audi_downloaded_at = datetime.now()
            
            # Assuming extraction updates progress bar itself
            extract_zip_file(full_path, extracted_path)
            audit.audi_processed_at = datetime.now()
            
            return audit

        except OSError as e:
            summary=f"Error downloading {url} or extracting file {file_name}"
            message=f"{summary}: {e}"
            logger.error(message)
            
            return None

def get_rf_filenames_parallel(
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
        futures = [
            executor.submit(
                download_and_extract_files, 
                audit,
                DADOS_RF_URL + audit.audi_filename, 
                output_path, 
                extracted_path,
                False
            )
            for audit in audits
        ]

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
    counter = 0
    error_count = 0
    error_basefiles = []
    total_count = len(audits)
    audits_ = []
    
    for index, audit in enumerate(audits):
        try:
            audit_ = download_and_extract_files(
                audit,
                DADOS_RF_URL + audit.audi_filename, 
                output_path, 
                extracted_path, 
                True
            )
            
            audits_.append(audit_)
            
            # Update progress bar after download (success or failure)
            counter = counter + 1
            logger.info('\n')

        except OSError as e:
            summary = f"Erro ao baixar ou extrair arquivo {audit.audi_filename}"
            message = f"{summary}: {e}"
            logger.error(message)
            error_count += 1
            error_basefiles.append(audit.audi_filename)
        
        logger.info(f"({index}/{total_count}) arquivos baixados. {error_count} erros: {error_basefiles}")

@timer('Baixar e extrair arquivos da Receita Federal')
def download_and_extract_RF_data(
    audits: list, 
    output_path: str, 
    extracted_path: str, 
    is_parallel: bool = True,
    max_workers: int = get_max_workers()
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

    if(is_parallel):
        audits = get_rf_filenames_parallel(audits, output_path, extracted_path, max_workers)
    else:
        audits = get_rf_filenames_serial(audits, output_path, extracted_path)

    # Download layout (assuming download remains unchanged)
    logger.info("Baixando layout")
    download(LAYOUT_URL, out=output_path, bar=None)
    
    return audits

@timer('Buscar dados da Receita Federal')
def get_RF_data(audits, from_folder, to_folder, is_parallel=True):
    """
    Retrieves and extracts the data from the Receita Federal.

    Args:
        to_folder (str): The path to the directory where the downloaded files will be saved.
        from_folder (str): The path to the directory where the extracted files will be stored.
        is_parallel (bool, optional): Whether to download and extract the files in parallel. Defaults to True.
    """
    # Extrair nomes dos arquivos
    audits = download_and_extract_RF_data(audits, from_folder, to_folder, is_parallel)
    
    zip_files = {
        zip_file: [
            zip_file_content.filename
            for zip_file_content in list_zip_contents(path.join(from_folder, zip_file))
        ]
        for zip_file in listdir(from_folder) if zip_file.rsplit('.', 1)[1] == 'zip'
    }
    
    # Deletar arquivos baixados
    rmtree(from_folder)
    
    return audits, zip_files

def load_database(database, from_folder, audits):
    """
    Loads the data from the extracted files into the database.

    Args:
        database: The database object to load the data into.
        from_folder (str): The path to the directory containing the extracted files.
    """
    # Lê e insere dados
    filenames = get_RF_filenames(from_folder)

    # Popula banco com dados da Receita
    populate_database(database, from_folder, filenames)

    # Cria índices na tabela em coluna 'cnpj_basico'
    generate_database_indices(database.engine)

