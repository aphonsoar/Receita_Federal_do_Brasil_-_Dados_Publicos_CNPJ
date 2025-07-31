# -*- coding: utf-8 -*-
"""
Created on Tue Jul 29 12:19:27 2025

@author: lealp
"""
from sqlalchemy import create_engine
import psycopg2
import sys
import pandas as pd
from dask import dataframe as dd
from typing import Union
import os

from environmental_variable_fetcher import loadEnvironmentalVariables


class DatabaseContext:
    def __init__(self):
        
        loadEnvironmentalVariables()

        # Conectar no banco de dados:
        # Dados da conexão com o BD
        user=os.getenv('DB_USER')
        passw=os.getenv('DB_PASSWORD')
        host=os.getenv('DB_HOST')
        port=os.getenv('DB_PORT')
        database=os.getenv('DB_NAME')

        
        self.databaseConnectionString = ('postgresql://'+user+':'+passw+'@'+host+':'+port+'/'+database)
        
        self.engine = create_engine(self.databaseConnectionString)
        
        dsn = 'dbname='+database+' '+'user='+user+' '+'host='+host+' '+'port='+port+' '+'password='+passw
        
        self.conn = psycopg2.connect(dsn)
        self.cur = self.conn.cursor()
        
        
    def sanitizarBD(self):
        """
        Sanitiza o BD, limpando todos os dados de todas as tabelas do BD

        Returns
        -------
        None.

        """
        print("Sanitizando o BD")
        
        self.cur.execute('DROP TABLE IF EXISTS "estabelecimento";')
        
        self.cur.execute('DROP TABLE IF EXISTS "empresa";')
        
        # Drop table antes do insert
        self.cur.execute('DROP TABLE IF EXISTS "socios";')
        
        self.cur.execute('DROP TABLE IF EXISTS "simples";')
        
        self.cur.execute('DROP TABLE IF EXISTS "cnae";')
        
        self.cur.execute('DROP TABLE IF EXISTS "moti";')
        
        self.cur.execute('DROP TABLE IF EXISTS "munic";')
        
        # Drop table antes do insert
        self.cur.execute('DROP TABLE IF EXISTS "natju";')
        
        self.cur.execute('DROP TABLE IF EXISTS "pais";')
        
        self.cur.execute('DROP TABLE IF EXISTS "quals";')
        
        self.conn.commit()
        
    def __exit__(self):
        self.close()
        
    def close(self):
        self.conn.close()
        self.engine.dispose()

        
    def to_sql(self, 
               df: Union[dd.DataFrame, pd.DataFrame], 
               if_exists = 'append',
               chunksize = (2**15), 
               **kwargs):
        """
        Quebra em pedacos a tarefa de inserir registros no banco

        Parameters
        ----------
        df : dd.DataFrame | pd.DataFrame
            Arquivo que será escrito no BD.
            
        chunksize : TYPE, optional
            DESCRIPTION: define o tamanho que o arquivo será quebrado
            para respectiva escrita no BD
            Default: 2^15 (dois elevado à 15ª)
            
        **kwargs : TYPE
            DESCRIPTION.

        Returns
        -------
        TYPE
            DESCRIPTION.

        """
        
        if ("if_exists" in kwargs):
            del kwargs["if_exists"]
        
        if (isinstance(df, dd.DataFrame)):
            name = kwargs.get('name')
            progress = f'Escrevendo dados de: {name}.'
            df.to_sql(name=name,
                      uri = self.databaseConnectionString,
                      chunksize=chunksize, 
                      method="multi", 
                      if_exists = if_exists,
                      parallel=True)
            
            sys.stdout.write(f'\r{progress}')
            sys.stdout.write('\n')
            
        else:
            if (not df.empty):
                total = len(df)//chunksize
                total += 1
                name = kwargs.get('name')
            
                def chunker(dfx):
                    return (dfx[i:i + chunksize] for i in range(0, len(dfx), chunksize))
            
                for i, df in enumerate(chunker(df)):
                    df.to_sql(con=self.engine, if_exists = if_exists, **kwargs)
                    index = (i+1)
                    progress = f'Escrevendo dados de: {name}. Processo: {index} de {total}.'
                    sys.stdout.write(f'\r{progress}')
                sys.stdout.write('\n')
            else:
                print("Arquivo vazio. Nenhum dado para ser gravado no BD")
            
    def __del__(self):
        self.close()
    