# -*- coding: utf-8 -*-
"""
Created on Tue Jul 29 12:12:08 2025

@author: lealp
"""

import pandas as pd
import os
from time import time
from databaseContext import DatabaseContext
import warnings
from dask import dataframe as dd

def _processarEstabelecimentos(lista: pd.DataFrame, 
                               output_directory_of_extracted_files: str,
                               chunksize = 32_000_000) -> pd.DataFrame:
    """
    
    Parameters
    ----------
    lista : pd.DataFrame
        DESCRIPTION.
    output_directory_of_extracted_files : str
        DESCRIPTION.
        
    chunksize: int
        Define o tamanho de chunk que será utilizado para leitura dos dados 
        do estabelecimento
        
    cnae: str
        Define o CNAE do IBGE para securitizadoras.
         Vide: https://concla.ibge.gov.br/busca-online-cnae.html?view=subclasse&tipo=cnae&versao=10.1.0&subclasse=6492100&chave=Securitiza%C3%A7%C3%A3o

    Yields
    ------
    estabelecimento : pd.DataFrame
        DESCRIPTION.

    """

    print('Tem %i arquivos de estabelecimento!' % len(lista))
    
    dtypes = {'cnpj_basico': str, # 1
            'cnpj_ordem': str, # 2 
            'cnpj_dv': str,  # 3
            'identificador_matriz_filial': int, #4
            'nome_fantasia': str, # 5
            'situacao_cadastral': int, # 6
            'data_situacao_cadastral': str, # 7 
            'motivo_situacao_cadastral' : str, # 8
            'nome_cidade_exterior': str, # 9
            'pais': str, #10
            'data_inicio_atividade': str, #11 
            'cnae_fiscal_principal': str, # 12
            'cnae_fiscal_secundaria': str, # 13
            'tipo_logradouro': str, # 14
            'logradouro': str,  # 15
            'numero': str,  # 16
            'complemento': str, #17
            'bairro': str, #18
            'cep': str, #19
            'uf': str, #20
            'municipio': str, #21
            'ddd_1': str, #22
            'telefone_1': str, #23 
            'ddd_2': str, #24
            'telefone_2': str, #25 
            'ddd_fax': str, #26
            'fax': str,  #27
            'correio_eletronico': str, #28 
            'situacao_especial': str, #29
            'data_situacao_especial': str #30
            }
    
    
    for e in lista:
        print('Trabalhando no arquivo: '+e+' de estabelecimentos')
        
        extracted_file_path = os.path.join(output_directory_of_extracted_files, e)
                
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=pd.errors.ParserWarning)
            
            estabelecimento = dd.read_csv(urlpath=extracted_file_path,
                                  sep=';',
                                  names=dtypes.keys(),
                                  header=None,
                                  dtype=dtypes,
                                  encoding='latin-1',
                                  blocksize = chunksize,
                                  index_col=False
            )
            
            
            yield estabelecimento


def processarEstabelecimentos(arquivos: list[str], 
                             output_directory_of_extracted_files: str,
                             databaseContext: DatabaseContext) -> None:
    
    
        
    print("""
    ###############################
    ## Arquivos de ESTABELECIMENTO:
    ###############################
    """)
    
    
    start = time()

    print('Tem %i arquivos de estabelecimento!' % len(arquivos))


    enumeravel = _processarEstabelecimentos(arquivos, 
                                            output_directory_of_extracted_files)

    for estabelecimento in enumeravel:
        
        databaseContext.to_sql(estabelecimento, 
                               name='estabelecimento', 
                               if_exists='append', 
                               index=False)
        
    
    print('Arquivos de estabelecimentos finalizados!')
    end = time()
    elapsedTime = round((end - start))
    print('Tempo de execução do processo de estabelecimentos (em segundos): ' + str(elapsedTime))
    