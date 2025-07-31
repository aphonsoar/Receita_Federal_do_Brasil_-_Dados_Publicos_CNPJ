# -*- coding: utf-8 -*-
"""
Created on Tue Jul 29 14:48:00 2025

@author: lealp
"""


import pandas as pd
import os
from time import time
from data_download import DataHolder
from databaseContext import DatabaseContext

def processarQualificacoesDeSocios(dataHolder: DataHolder, 
                                   databaseContext: DatabaseContext,
                                   output_directory_of_extracted_files: str):
    
    quals_insert_start = time()
    print("""
    ######################################
    ## Arquivos de qualificação de sócios:
    ######################################
    """)
    
    
    for e in dataHolder.arquivos_quals:
        print('Trabalhando no arquivo de QUALIFICAÇÃO DE SÓCIO: '+e+' [...]')
    
        dtypes = {'codigo': int, 'descricao': str}
        extracted_file_path = os.path.join(output_directory_of_extracted_files, e)
        quals = pd.read_csv(filepath_or_buffer=extracted_file_path, 
                           sep=';', 
                           skiprows=0, 
                           header=None, 
                           names=dtypes.keys(),
                           dtype=dtypes, 
                           encoding='latin-1',
                           index_col=False)
    
        # Gravar dados no banco:
        # quals
        databaseContext.to_sql(quals, name='quals', if_exists='append', index=False)
        print('Arquivo ' + e + ' inserido com sucesso no banco de dados!')
    
    print('Arquivos de QUALIFICAÇÃO DE SÓCIO finalizados!')
    quals_insert_end = time()
    quals_Tempo_insert = round((quals_insert_end - quals_insert_start))
    print('Tempo de execução do processo de qualificação de sócios (em segundos): ' + str(quals_Tempo_insert))
