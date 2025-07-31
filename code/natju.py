# -*- coding: utf-8 -*-
"""
Created on Tue Jul 29 14:44:59 2025

@author: lealp
"""


import pandas as pd
import os
from time import time
from data_download import DataHolder
from databaseContext import DatabaseContext

def processarNATJU(dataHolder: DataHolder, 
                   databaseContext: DatabaseContext,
                   output_directory_of_extracted_files: str):
    """
    Processa os arquivos de natureza jurídica

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
    
        
    natju_insert_start = time()
    print("""
    #################################
    ## Arquivos de natureza jurídica:
    #################################
    """)
    
    
    for e in dataHolder.arquivos_natju:
        print('Trabalhando no arquivo: '+ e +' [...]')
    
        dtypes = {'codigo': int, 'descricao': str}
        extracted_file_path = os.path.join(output_directory_of_extracted_files, e)
        
        natju = pd.read_csv(filepath_or_buffer=extracted_file_path, 
                            sep=';', 
                            skiprows=0, 
                            header=None, 
                            dtype=dtypes, 
                            encoding='latin-1',
                            index_col=False,
                            names=dtypes.keys())
    
        # Gravar dados no banco:
        # natju
        databaseContext.to_sql(natju, 
                               name='natju', 
                               if_exists='append', 
                               index=False)
    
    
    print('Arquivos de natju finalizados!')
    natju_insert_end = time()
    natju_Tempo_insert = round((natju_insert_end - natju_insert_start))
    print('Tempo de execução do processo de natureza jurídica (em segundos): ' + str(natju_Tempo_insert))


