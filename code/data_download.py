# -*- coding: utf-8 -*-
"""
Created on Tue Jul 29 12:04:25 2025

@author: lealp
"""
import bs4 as bs
import wget
import os
import requests
import sys

import pandas as pd

import urllib
import zipfile

def check_diff(url, file_name) -> bool:
    '''
    Verifica se o arquivo no servidor existe no disco e se ele tem o mesmo
    tamanho no servidor.
    
    Retorna:
        Verdadeiro quando o arquivo precisa ser baixado novamente
    
        Falso quando o arquivo não deve ser baixado (redundante)
    '''
    if not os.path.isfile(file_name):
        return True # ainda nao foi baixado

    response = requests.head(url)
    new_size = int(response.headers.get('content-length', 0))
    old_size = os.path.getsize(file_name)
    if new_size != old_size:
        os.remove(file_name)
        return True # tamanho diferentes

    return False # arquivos sao iguais



# Create this bar_progress method which is invoked automatically from wget:
def bar_progress(current, total, width=80):
  progress_message = "Downloading: %d%% [%d / %d] bytes - " % (current / total * 100, current, total)
  # Don't use print() as it will print in new line every time.
  sys.stdout.write("\r" + progress_message)
  sys.stdout.flush()

#%% Donwloads

def download(output_directory_of_downloaded_files: str) -> list[str]:
    """
    
    Returns
    -------
    list[str]
        Lista de todos os nomes de todos os arquivos ZIPs que deverão ser baixados
        do site do governo federal.

    """
    
    dados_rf = os.getenv("URL_DA_RECEITA")
    
    raw_html = urllib.request.urlopen(dados_rf)
    raw_html = raw_html.read()
    
    # Formatar página e converter em string
    soup = bs.BeautifulSoup(raw_html, 'html.parser')
    all_anchor_tags = soup.find_all('a')
    
    # Obter arquivos
    print('Arquivos que serão baixados:')
    Files = []
    for anchorTag in all_anchor_tags:
        href = anchorTag.get('href')
        
        if (".zip" in href):
            print(str(len(Files)) + ' - ' + href)
            Files.append(href)
  
    for file in Files:
        if (dados_rf.endswith("/")):
            url = (dados_rf+ file).replace("\\","/")
        else:
            url = os.path.join(dados_rf, file).replace("\\","/")
        fullFileNameForDownload = os.path.join(output_directory_of_downloaded_files, file)
        if check_diff(url, fullFileNameForDownload):
            print(f"Baixando {file}")
            wget.download(url, out=output_directory_of_downloaded_files, bar=bar_progress)
            
    return Files


def unzipFiles(files: list[str],
               output_directory_of_downloaded_files: str,
               output_directory_of_extracted_files: str):
    i_l = 0
    for file in files:
        try:
            i_l += 1
            print(f'Descompactando arquivo {file}:')
            full_path = os.path.join(output_directory_of_downloaded_files, file)
            
            if (not os.path.exists(full_path) or not os.path.isfile(full_path)):
                with zipfile.ZipFile(full_path, 'r') as zip_ref:
                    zip_ref.extractall(output_directory_of_extracted_files)
        except:
            pass


class DataHolder:
    def __init__(self, 
                 arquivos_empresa: list[str],
                 arquivos_estabelecimento: list[str],
                 arquivos_socios: list[str],
                 arquivos_simples: list[str],
                 arquivos_cnae: list[str],
                 arquivos_moti: list[str],
                 arquivos_munic: list[str],
                 arquivos_natju: list[str],
                 arquivos_pais: list[str],
                 arquivos_quals: list[str]
                 ):
        
        self.arquivos_empresa = arquivos_empresa
        self.arquivos_estabelecimento = arquivos_estabelecimento
        self.arquivos_socios = arquivos_socios
        self.arquivos_simples = arquivos_simples
        self.arquivos_cnae = arquivos_cnae
        self.arquivos_moti = arquivos_moti
        self.arquivos_munic = arquivos_munic
        self.arquivos_natju = arquivos_natju
        self.arquivos_pais = arquivos_pais
        self.arquivos_quals = arquivos_quals

def segregarDadosPorTabelaDoBD(output_directory_of_extracted_files: str) -> DataHolder:
        
    # Files:
    Items = [name for name in os.listdir(output_directory_of_extracted_files) if name.endswith('')]
    
    # Separar arquivos:
    arquivos_empresa = []
    arquivos_estabelecimento = []
    arquivos_socios = []
    arquivos_simples = []
    arquivos_cnae = []
    arquivos_moti = []
    arquivos_munic = []
    arquivos_natju = []
    arquivos_pais = []
    arquivos_quals = []
    for i in range(len(Items)):
        if Items[i].find('EMPRE') > -1:
            arquivos_empresa.append(Items[i])
        elif Items[i].find('ESTABELE') > -1:
            arquivos_estabelecimento.append(Items[i])
        elif Items[i].find('SOCIO') > -1:
            arquivos_socios.append(Items[i])
        elif Items[i].find('SIMPLES') > -1:
            arquivos_simples.append(Items[i])
        elif Items[i].find('CNAE') > -1:
            arquivos_cnae.append(Items[i])
        elif Items[i].find('MOTI') > -1:
            arquivos_moti.append(Items[i])
        elif Items[i].find('MUNIC') > -1:
            arquivos_munic.append(Items[i])
        elif Items[i].find('NATJU') > -1:
            arquivos_natju.append(Items[i])
        elif Items[i].find('PAIS') > -1:
            arquivos_pais.append(Items[i])
        elif Items[i].find('QUALS') > -1:
            arquivos_quals.append(Items[i])
        else:
            pass
    
    dataHolder = DataHolder(arquivos_empresa,
                            arquivos_estabelecimento,
                            arquivos_socios,
                            arquivos_simples,
                            arquivos_cnae,
                            arquivos_moti,
                            arquivos_munic,
                            arquivos_natju,
                            arquivos_pais,
                            arquivos_quals)
    
    return dataHolder