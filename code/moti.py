# -*- coding: utf-8 -*-
"""
Created on Tue Jul 29 14:40:33 2025

@author: lealp
"""


import pandas as pd
import os
from time import time
from data_download import DataHolder
from databaseContext import DatabaseContext

def processarMOTI(dataHolder: DataHolder, 
                  databaseContext: DatabaseContext,
                  output_directory_of_extracted_files: str):
    
    moti_insert_start = time()
    print("""
    #########################################
    ## Arquivos de motivos da situação atual:
    #########################################
    """)


    for e in dataHolder.arquivos_moti:
        print('Trabalhando no arquivo: '+e+' [...]')

        dtypes = {'codigo': int, 'descricao': str}
        
        extracted_file_path = os.path.join(output_directory_of_extracted_files, e)
        
        moti = pd.read_csv(filepath_or_buffer=extracted_file_path, 
                           sep=';', 
                           header=None, 
                           names=dtypes.keys(),
                           dtype=dtypes, 
                           encoding='latin-1',
                           index_col=False)

        databaseContext.to_sql(moti, name='moti', index=False)
        print('Arquivo ' + e + ' inserido com sucesso no banco de dados!')


    print('Arquivos de moti finalizados!')
    moti_insert_end = time()
    moti_Tempo_insert = round((moti_insert_end - moti_insert_start))
    print('Tempo de execução do processo de motivos da situação atual (em segundos): ' + str(moti_Tempo_insert))