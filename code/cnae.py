# -*- coding: utf-8 -*-
"""
Created on Tue Jul 29 14:31:15 2025

@author: lealp
"""

# -*- coding: utf-8 -*-
"""
Created on Tue Jul 29 14:27:38 2025

@author: lealp
"""

import pandas as pd
import os
from time import time
from data_download import DataHolder
from databaseContext import DatabaseContext

def processarCNAEs(dataHolder: DataHolder, 
                   databaseContext: DatabaseContext,
                   output_directory_of_extracted_files: str):
    
    cnae_insert_start = time()
    print("""
    ######################
    ## Arquivos de cnae:
    ######################
    """)
    
    
    for e in dataHolder.arquivos_cnae:
        print('Trabalhando no arquivo: '+ e +' [...]')
       
    
        extracted_file_path = os.path.join(output_directory_of_extracted_files, e)
        cnae = pd.DataFrame(columns=[1,2])
        cnae = pd.read_csv(filepath_or_buffer=extracted_file_path, 
                           sep=';', 
                           skiprows=0, 
                           header=None, 
                           dtype='str', 
                           encoding='latin-1')
    
        # Tratamento do arquivo antes de inserir na base:
        cnae = cnae.reset_index()
        del cnae['index']
    
        # Renomear colunas
        cnae.columns = ['codigo', 'descricao']
    
        # Gravar dados no banco:
        # cnae
        databaseContext.to_sql(cnae, name='cnae', index=False)
        print('Arquivo ' + e + ' inserido com sucesso no banco de dados!')
    
    
    print('Arquivos de cnae finalizados!')
    cnae_insert_end = time()
    cnae_Tempo_insert = round((cnae_insert_end - cnae_insert_start))
    print('Tempo de execução do processo de cnae (em segundos): ' + str(cnae_Tempo_insert))