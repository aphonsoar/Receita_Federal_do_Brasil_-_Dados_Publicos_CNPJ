from wget import download 
from os import path, listdir
from timy import timer
from zipfile import ZipFile
from tqdm import tqdm
from shutil import rmtree
from concurrent.futures import ThreadPoolExecutor, as_completed

from utils.logging import logger
from utils.misc import check_diff, get_max_workers
from core.scrapper import scrap_RF
from utils.database import populate_database, generate_database_indices
from core.constants import TABLES_INFO_DICT, DADOS_RF_URL, LAYOUT_URL

####################################################################################################
## LER E INSERIR DADOS #############################################################################
####################################################################################################
@timer('Ler dados da Receita Federal')
def get_RF_filenames(extracted_files_path):
    # Files:
    items = [ name for name in listdir(extracted_files_path) if name.endswith('') ]
    
    # Separar arquivos:
    files = {
        table_name: [] for table_name in TABLES_INFO_DICT.keys()
    }

    tablename_list = [ table_name for table_name in TABLES_INFO_DICT.keys() ]
    trimmed_tablename_list = [ table_name[:5] for table_name in TABLES_INFO_DICT.keys() ]
    
    tablename_tuples = list(zip(tablename_list, trimmed_tablename_list))
    
    for item in items:
        has_label_map = lambda label: item.lower().find(label[1].lower()) > -1
        this_tablename_tuple = list(filter(has_label_map, tablename_tuples))

        if(len(this_tablename_tuple)!=0):
            this_tablename = this_tablename_tuple[0][0]
            files[this_tablename].append(item)

    return files

####################################################################################################
## DOWNLOAD ########################################################################################
####################################################################################################
def extract_zip_file(file_path, extracted_files_path):
    with ZipFile(file_path, 'r') as zip_ref:
        zip_ref.extractall(extracted_files_path)

def download_and_extract_files(
    url: str, 
    download_path: str, 
    extracted_path: str, 
    has_progress_bar: bool
):
    """
    Downloads a file from the given URL to the specified output path.

    Args:
        url (str): The URL of the file to download.
        output_path (str): The path to save the downloaded file.

    Raises:
        OSError: If an error occurs during the download process.
    """
    file_name = path.basename(url)
    
    full_path = path.join(download_path, file_name)

    if not path.exists(full_path) or check_diff(url, full_path):
        try:
            # Assuming download updates progress bar itself
            if(not has_progress_bar):
                download(url, out=download_path, bar=None)
            
            else:
                download(url, out=download_path)

            # Assuming extraction updates progress bar itself
            extract_zip_file(full_path, extracted_path)

        except OSError as e:
            raise OSError(f"Error downloading {url} or extracting file {file_name}: {e}") from e        
    
def get_rf_filenames_parallel(
    base_files: list,
    output_path: str, 
    extracted_path: str,
    max_workers = get_max_workers()
):
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(
                download_and_extract_files, 
                DADOS_RF_URL + base_file, 
                output_path, 
                extracted_path,
                False
            )
            for base_file in base_files
        ]

        for future in tqdm(as_completed(futures), total=len(base_files)):
            pass

def get_rf_filenames_serial(
    base_files: list,
    output_path: str, 
    extracted_path: str, 
):
    counter = 0
    error_count = 0
    error_basefiles = []
    total_count = len(base_files)
    for index, base_file in enumerate(base_files):
        try:
            download_and_extract_files(
                DADOS_RF_URL + base_file, 
                output_path, 
                extracted_path, 
                True
            )
            
            # Update progress bar after download (success or failure)
            counter = counter + 1
            logger.info('\n')

        except OSError as e:
            summary = f"Erro ao baixar ou extrair arquivo {base_file}"
            message = f"{summary}: {e}"
            logger.error(message)
            error_count += 1
            error_basefiles.append(base_file)
        
        logger.info(f"({index}/{total_count}) arquivos baixados. {error_count} erros: {error_basefiles}")

@timer('Baixar e extrair arquivos da Receita Federal')
def download_and_extract_RF_data(
    base_files: list, 
    output_path: str, 
    extracted_path: str, 
    is_parallel: bool = True,
    max_workers: int = get_max_workers()
):
    """
    Downloads files from the Receita Federal base URLs to the specified output path.

    Args:
        base_files (list): A list of base file names to be downloaded.
        output_path (str): The path to save the downloaded files.
        max_workers (int, optional): The maximum number of concurrent downloads. Defaults to get_max_workers().

    Raises:
        OSError: If an error occurs during the download process.
    """

    if(is_parallel):
        get_rf_filenames_parallel(base_files, output_path, extracted_path, max_workers)
    else:
        get_rf_filenames_serial(base_files, output_path, extracted_path)

    # Download layout (assuming download remains unchanged)
    logger.info("Baixando layout")
    download(LAYOUT_URL, out=output_path, bar=None)

####################################################################################################
## EXTRAÇÃO ########################################################################################
####################################################################################################
def extract_RF_data(
    base_files, 
    output_files_path, 
    extracted_files_path, 
    max_workers = get_max_workers()
):
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Use tqdm as a context manager to automatically handle progress bar updates
        with tqdm(total=len(base_files), desc='Descompactando arquivos') as pbar:
            futures = []
            for base_file in base_files:
                file_path = path.join(output_files_path, base_file)
                future = executor.submit(extract_zip_file, file_path, extracted_files_path)
                future.add_done_callback(lambda p: pbar.update())
                futures.append(future)

            # Wait for all tasks to complete
            for future in futures:
                future.result()

@timer('Buscar dados da Receita Federal')
def get_RF_data(to_folder, from_folder, is_parallel=True):
    # Raspar dados da página 
    base_files_info = scrap_RF()

    # Extrair nomes dos arquivos
    base_files = [base_file for updated_at, base_file in base_files_info]

    # Baixar arquivos e extrair arquivos
    download_and_extract_RF_data(base_files, to_folder, from_folder, is_parallel)

    # Deletar arquivos baixados
    rmtree(to_folder)

def load_database(database, from_folder):
    # Lê e insere dados
    filenames = get_RF_filenames(from_folder)

    # Popula banco com dados da Receita
    populate_database(database, from_folder, filenames)

    # Cria índices na tabela em coluna 'cnpj_basico'
    generate_database_indices(database.engine)

