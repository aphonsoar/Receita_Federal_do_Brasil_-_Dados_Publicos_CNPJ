# -*- coding: utf-8 -*-
"""
Created on Wed Jul 30 10:17:01 2025

@author: lealp
"""

import os
from environmental_variable_fetcher import loadEnvironmentalVariables


def loadOoutPutDirectories() -> (str, str):    
    
    def makedirs(path):
        '''
        cria path caso seja necessario
        '''
        if not os.path.exists(path):
            os.makedirs(path)
    
    loadEnvironmentalVariables()
    # Read details from ".env" file:
    output_directory_of_downloaded_files = None
    output_directory_of_extracted_files = None
    try:
        output_directory_of_downloaded_files = os.getenv('OUTPUT_DIRECTORY_FOR_DOWNLOAD_PATH')
        output_directory_of_extracted_files = os.getenv('OUTPUT_DIRECTORY_FOR_EXTRACTED_DATA_PATH')
        
        print('Diretórios definidos: \n' +
              'diretório para download: ' + str(output_directory_of_downloaded_files)  + '\n' +
              'diretório para extração dos arquivos zip: ' + str(output_directory_of_extracted_files))
        
        makedirs(output_directory_of_downloaded_files)
        makedirs(output_directory_of_extracted_files)
    
    except:
        pass
        print('Erro na definição dos diretórios, verifique o arquivo ".env" ou o local informado do seu arquivo de configuração.')
        
    return (output_directory_of_downloaded_files, 
            output_directory_of_extracted_files)