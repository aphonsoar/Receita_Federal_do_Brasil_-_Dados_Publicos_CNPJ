from wget import download 
from os import path, listdir
from timy import timer
from zipfile import ZipFile
from tqdm import tqdm
from threading import Lock
from os import rmdir

lock = Lock()

from utils import check_diff
from scrapper import raspar_receita_federal
from utils import get_max_workers
from concurrent.futures import ThreadPoolExecutor, as_completed
from urls import DADOS_RF_URL, LAYOUT_URL

from database_utils import popular_banco, criar_indices_banco

##########################################################################
## LER E INSERIR DADOS ###################################################
##########################################################################
@timer('Ler dados da Receita Federal')
def ler_dados_receita_federal(extracted_files_path):
    # Files:
    items = [name for name in listdir(extracted_files_path) if name.endswith('')]

    # Separar arquivos:
    arquivos = {
        'empresa': [],
        'estabelecimento': [],
        'socios': [],
        'simples': [],
        'cnae': [],
        'moti': [],
        'munic': [],
        'natju': [],
        'pais': [],
        'quals': [],
    }
    for i in range(len(items)):
        if items[i].find('EMPRE') > -1:
            arquivos['empresa'].append(items[i])
        elif items[i].find('ESTABELE') > -1:
            arquivos['estabelecimento'].append(items[i])
        elif items[i].find('SOCIO') > -1:
            arquivos['socios'].append(items[i])
        elif items[i].find('SIMPLES') > -1:
            arquivos['simples'].append(items[i])
        elif items[i].find('CNAE') > -1:
            arquivos['cnae'].append(items[i])
        elif items[i].find('MOTI') > -1:
            arquivos['moti'].append(items[i])
        elif items[i].find('MUNIC') > -1:
            arquivos['munic'].append(items[i])
        elif items[i].find('NATJU') > -1:
            arquivos['natju'].append(items[i])
        elif items[i].find('PAIS') > -1:
            arquivos['pais'].append(items[i])
        elif items[i].find('QUALS') > -1:
            arquivos['quals'].append(items[i])
        else:
            pass

    return arquivos

##########################################################################
## DOWNLOAD ##############################################################
##########################################################################
def extrair_arquivo(file_path, extracted_files_path):
    with ZipFile(file_path, 'r') as zip_ref:
        zip_ref.extractall(extracted_files_path)

def baixar_e_extrair_arquivo(url, download_path, extracted_path):
    """Downloads a file from the given URL to the specified output path.

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
            download(url, out=download_path, bar=None)

            # Assuming extraction updates progress bar itself
            extrair_arquivo(full_path, extracted_path)

        except OSError as e:
            raise OSError(f"Error downloading {url} or extracting file {file_name}: {e}") from e        
    
@timer('Baixar e extrair arquivos da Receita Federal')
def baixar_e_extrair_dados_da_receita_federal(
    base_files, 
    output_path, extracted_path, 
    is_parallel = True,
    max_workers=get_max_workers()
):
    """Downloads files from the Receita Federal base URLs to the specified output path.

    Args:
        base_files (list): A list of base file names to be downloaded.
        output_path (str): The path to save the downloaded files.
        max_workers (int, optional): The maximum number of concurrent downloads. Defaults to get_max_workers().

    Raises:
        OSError: If an error occurs during the download process.
    """

    if(is_parallel):
        with tqdm(total=len(base_files)) as pbar:
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = [
                    (base_file, 
                        executor.submit(
                            baixar_e_extrair_arquivo, 
                            DADOS_RF_URL + base_file, 
                            output_path, 
                            extracted_path
                        )
                    )
                    for base_file in base_files
                ]

                for future in as_completed(futures):
                    pbar.update(1)
                    future.result()

    else:
        counter = 0
        total_count = len(base_files)
        for index, base_file in enumerate(base_files):
            baixar_e_extrair_arquivo(DADOS_RF_URL + base_file, output_path, extracted_path)

            # Update progress bar after download (success or failure)
            counter = counter + 1

            print(f"{index}/{total_count} arquivos baixados.")

    # Download layout (assuming download remains unchanged)
    print("Baixando layout:")
    download(LAYOUT_URL, out=output_path, bar=None)

##########################################################################
## EXTRAÇÃO ##############################################################
##########################################################################
def extrair_dados_receita_federal(
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
                future = executor.submit(extrair_arquivo, file_path, extracted_files_path)
                future.add_done_callback(lambda p: pbar.update())
                futures.append(future)

            # Wait for all tasks to complete
            for future in futures:
                future.result()

@timer('Buscar dados da Receita Federal')
def buscar_dados(output_files_path, extracted_files_path):
    # Raspar dados da página 
    base_files = raspar_receita_federal()

    # Baixar arquivos e extrair arquivos
    baixar_e_extrair_dados_da_receita_federal(base_files, output_files_path, extracted_files_path)
    
    # Deletar arquivos baixados
    rmdir(output_files_path)

def carregar_banco(engine, conn, extracted_files_path):

    # Ler e inserir dados
    arquivos = ler_dados_receita_federal(extracted_files_path)

    # Popular banco com dados da Receita
    popular_banco(engine, conn, extracted_files_path, arquivos)
    criar_indices_banco(conn)

