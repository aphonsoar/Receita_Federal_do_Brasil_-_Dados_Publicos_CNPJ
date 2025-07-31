# -*- coding: utf-8 -*-
"""
Created on Tue Jul 29 14:43:04 2025

@author: lealp
"""

import pandas as pd
import os
from time import time
from data_download import DataHolder
from databaseContext import DatabaseContext

def processarMunicipios(dataHolder: DataHolder, 
                        databaseContext: DatabaseContext,
                        output_directory_of_extracted_files: str):

    munic_insert_start = time()
    print("""
    ##########################
    ## Arquivos de municípios:
    ##########################
    """)


    for e in dataHolder.arquivos_munic:
        print('Trabalhando no arquivo: '+ e +' [...]')
        dtypes = {'codigo': 'int', 'descricao': str}
        
        extracted_file_path = os.path.join(output_directory_of_extracted_files, 
                                           e)
        
        munic = pd.read_csv(filepath_or_buffer=extracted_file_path, 
                            sep=';', 
                            skiprows=0, 
                            header=None, 
                            dtype=dtypes, 
                            encoding='latin-1',
                            index_col=False,
                            names=dtypes.keys())
       
        # Gravar dados no banco:
        # munic
        databaseContext.to_sql(munic, name='munic', index=False)
        print('Arquivo ' + e + ' inserido com sucesso no banco de dados!')

    try:
        del munic
    except:
        pass
    print('Arquivos de munic finalizados!')
    munic_insert_end = time()
    munic_Tempo_insert = round((munic_insert_end - munic_insert_start))
    print('Tempo de execução do processo de municípios (em segundos): ' + str(munic_Tempo_insert))