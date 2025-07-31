# -*- coding: utf-8 -*-
"""
Created on Tue Jul 29 14:46:28 2025

@author: lealp
"""

import pandas as pd
import os
from time import time
from data_download import DataHolder
from databaseContext import DatabaseContext

def processarPaises(dataHolder: DataHolder, 
                   databaseContext: DatabaseContext,
                   output_directory_of_extracted_files: str):
    """
    Processa dados dos países do mundo

    Parameters
    ----------
    dataHolder : DataHolder
        DESCRIPTION.
    databaseContext : DatabaseContext
        DESCRIPTION.
    output_directory_of_extracted_files : str
        DESCRIPTION.

    Returns
    -------
    None.

    """
        
    pais_insert_start = time()
    print("""
    ######################
    ## Arquivos de país:
    ######################
    """)
    
    
    for e in dataHolder.arquivos_pais:
        print('Trabalhando no arquivo de País: '+e+' [...]')
    
        dtypes = ({'codigo': int, 'descricao': str})
        extracted_file_path = os.path.join(output_directory_of_extracted_files, e)
        pais = pd.read_csv(filepath_or_buffer=extracted_file_path, 
                           sep=';', 
                           skiprows=0, 
                           header=None, 
                           names=dtypes.keys(),
                           dtype=dtypes, 
                           encoding='latin-1',
                           index_col=False)
   
        # Gravar dados no banco:
        # pais
        databaseContext.to_sql(pais, 
                               name='pais',  
                               if_exists='append', 
                               index=False)
        
        print('Arquivo ' + e + ' inserido com sucesso no banco de dados!')

    print('Arquivos de países finalizados!')
    pais_insert_end = time()
    pais_Tempo_insert = round((pais_insert_end - pais_insert_start))
    print('Tempo de execução do processo de país (em segundos): ' + str(pais_Tempo_insert))
