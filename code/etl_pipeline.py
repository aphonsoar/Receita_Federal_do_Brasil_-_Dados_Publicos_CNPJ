from wget import download 
from os import path, listdir
from timy import timer
from zipfile import ZipFile
from tqdm import tqdm

from utils import check_diff
from scrapper import raspar_receita_federal
from utils import get_max_workers
from concurrent.futures import ThreadPoolExecutor
from urls import DADOS_RF_URL, LAYOUT_URL

##########################################################################
## LER E INSERIR DADOS ###################################################
##########################################################################
@timer('Ler dados da Receita Federal')
def ler_dados_receita_federal(extracted_files_path):
    # Files:
    Items = [name for name in listdir(extracted_files_path) if name.endswith('')]

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
    for i in range(len(Items)):
        if Items[i].find('EMPRE') > -1:
            arquivos['empresa'].append(Items[i])
        elif Items[i].find('ESTABELE') > -1:
            arquivos['estabelecimento'].append(Items[i])
        elif Items[i].find('SOCIO') > -1:
            arquivos['socios'].append(Items[i])
        elif Items[i].find('SIMPLES') > -1:
            arquivos['simples'].append(Items[i])
        elif Items[i].find('CNAE') > -1:
            arquivos['cnae'].append(Items[i])
        elif Items[i].find('MOTI') > -1:
            arquivos['moti'].append(Items[i])
        elif Items[i].find('MUNIC') > -1:
            arquivos['munic'].append(Items[i])
        elif Items[i].find('NATJU') > -1:
            arquivos['natju'].append(Items[i])
        elif Items[i].find('PAIS') > -1:
            arquivos['pais'].append(Items[i])
        elif Items[i].find('QUALS') > -1:
            arquivos['quals'].append(Items[i])
        else:
            pass

    return arquivos

##########################################################################
## DOWNLOAD ##############################################################
##########################################################################
def baixar_arquivo(url, output_files_path):
    file_name = path.join(output_files_path, path.basename(url))
    
    if check_diff(url, file_name):
        download(url, out=output_files_path)

@timer('Baixar arquivos da Receita Federal')
def baixar_dados_receita_federal(base_files, output_files_path, max_workers=get_max_workers()):
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        with tqdm(total=len(base_files), desc='Baixando arquivos') as pbar:
            futures = []
            for base_file in base_files:
                future = executor.submit(baixar_arquivo, DADOS_RF_URL + base_file, output_files_path)
                future.add_done_callback(lambda p: pbar.update())
                futures.append(future)

            # Wait for all tasks to complete
            for future in futures:
                future.result()

    # Download layout:
    print('Baixando layout:')
    download(LAYOUT_URL, out=output_files_path)

##########################################################################
## EXTRACT ###############################################################
##########################################################################
@timer('Extrair arquivos da Receita Federal')
def extrair_raquivo(file_path, extracted_files_path):
    with ZipFile(file_path, 'r') as zip_ref:
        zip_ref.extractall(extracted_files_path)

def extrair_dados_receita_federal(base_files, output_files_path, extracted_files_path, max_workers=16):
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Use tqdm as a context manager to automatically handle progress bar updates
        with tqdm(total=len(base_files), desc='Descompactando arquivos') as pbar:
            futures = []
            for base_file in base_files:
                file_path = path.join(output_files_path, base_file)
                future = executor.submit(extrair_raquivo, file_path, extracted_files_path)
                future.add_done_callback(lambda p: pbar.update())
                futures.append(future)

            # Wait for all tasks to complete
            for future in futures:
                future.result()

@timer('Buscar dados da Receita Federal')
def buscar_dados_receita_federal(output_files_path, extracted_files_path):
    # Raspar dados da página 
    base_files = raspar_receita_federal()

    # Baixar arquivos
    baixar_dados_receita_federal(base_files, output_files_path)

    # Extrair arquivos
    extrair_dados_receita_federal(base_files, output_files_path, extracted_files_path)
