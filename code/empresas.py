# -*- coding: utf-8 -*-
"""
Created on Tue Jul 29 14:27:38 2025

@author: lealp
"""

from dask import dataframe as dd
import os
from time import time
from data_download import DataHolder
from databaseContext import DatabaseContext

def processarEmpresas(dataHolder: DataHolder, 
                      databaseContext: DatabaseContext,
                      output_directory_of_extracted_files: str,
                      nomeDaTabela='empresa',
                      chunksize = 32_000_000):
    
    empresa_insert_start = time()
    print("""
    #######################
    ## Arquivos de EMPRESA:
    #######################
    """)


    for e in dataHolder.arquivos_empresa:
        print('Trabalhando no arquivo: '+ e + f' de {nomeDaTabela}')
        
        dtypes = {'cnpj_basico': str, 
                          'razao_social': str, 
                          'natureza_juridica': int, 
                          'qualificacao_responsavel': int, 
                          'capital_social': float, 
                          'porte_empresa': float, 
                          'ente_federativo_responsavel': str}
        
        extracted_file_path = os.path.join(output_directory_of_extracted_files, 
                                           e)

        empresa = dd.read_csv(urlpath=extracted_file_path,
                              sep=';',
                              names=dtypes.keys(),
                              header=None,
                              dtype=dtypes,
                              encoding='latin-1',
                              blocksize = chunksize,
                              index_col=False,
                              decimal=","
        )
        
        # Gravar dados no banco:
        # Empresa
        databaseContext.to_sql(empresa, name=nomeDaTabela, if_exists='append', index=False)
        print('Arquivo ' + e + ' inserido com sucesso no banco de dados!')


    print('Arquivos de empresa finalizados!')
    empresa_insert_end = time()
    empresa_Tempo_insert = round((empresa_insert_end - empresa_insert_start))
    print(f'Tempo de execução do processo de {nomeDaTabela} (em segundos): ' + str(empresa_Tempo_insert))
