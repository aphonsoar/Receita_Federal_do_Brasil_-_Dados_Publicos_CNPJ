# -*- coding: utf-8 -*-
"""
Created on Tue Jul 29 13:10:33 2025

@author: lealp
"""

from dask import dataframe as dd
from databaseContext import DatabaseContext
import os
from time import time
from data_download import DataHolder


def processarSimples(dataHolder: DataHolder, 
                     databaseContext: DatabaseContext, 
                     outputDirectory: str,
                     blocksize = 16_000_000) -> None:
    
    simples_insert_start = time()
    print("""
    ################################
    ## Arquivos do SIMPLES NACIONAL:
    ################################
    """)


    for e in dataHolder.arquivos_simples:
        print('Trabalhando no arquivo: '+e+' [...]')
        
        dtypes = {'cnpj_basico': str, 
                  'opcao_pelo_simples': str, 
                  'data_opcao_simples': int, 
                  'data_exclusao_simples': int, 
                  'opcao_mei': str, 
                  'data_opcao_mei': int, 
                  'data_exclusao_mei': int}
        
        
        extracted_file_path = os.path.join(outputDirectory, e)

        simples_lenght = sum(1 for line in open(extracted_file_path, "r"))
        print('Linhas no arquivo do Simples '+ e +': '+str(simples_lenght))

        simples = dd.read_csv(urlpath=extracted_file_path,
                              sep=';',
                              names=dtypes.keys(),
                              header=None,
                              dtype=dtypes,
                              encoding='latin-1',
                              index_col=False,
                              blocksize=blocksize
        )
        
        # Gravar dados no banco:
        # simples
        databaseContext.to_sql(simples, name='simples', index=False)
       

    print('Arquivos do simples finalizados!')
    simples_insert_end = time()
    simples_Tempo_insert = round((simples_insert_end - simples_insert_start))
    print('Tempo de execução do processo do Simples Nacional (em segundos): ' + str(simples_Tempo_insert))

