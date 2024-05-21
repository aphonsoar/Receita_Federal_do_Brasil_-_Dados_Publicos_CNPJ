from timy import timer
from os import getenv, path
from typing import Tuple, Union
from psycopg2 import connect, sql, OperationalError, errors
import pandas as pd
from sqlalchemy import text

from utils.misc import delete_var, to_sql
from core.constants import FENCE, TABLES_INFO_DICT
from core.models import Database, TableInfo
import sqlalchemy as sa

##########################################################################
## LOAD AND TRANSFORM
##########################################################################
def populate_table_with_filename(
    database: Database, 
    table_info: TableInfo,
    to_folder: str,
    filename: str
): 
    enum_columns = list(range(0, len(table_info.columns)))
    artefato = pd.DataFrame(columns=enum_columns)
    dtypes = { column: 'object' for column in table_info.columns }
    extracted_file_path = path.join(to_folder, filename)

    artefato = pd.read_csv(
        filepath_or_buffer=extracted_file_path,
        sep=';', skiprows=0, header=None, 
        dtype=dtypes, encoding=table_info.encoding,
        low_memory=False
    )

    # Tratamento do arquivo antes de inserir na base:
    artefato = artefato.reset_index()
    del artefato['index']

    # Renomear colunas
    artefato.columns = table_info.columns
    artefato = table_info.transform_map(artefato)
    
    # Gravar dados no banco:
    to_sql(
        artefato, 
        filename=extracted_file_path,
        tablename=table_info.table_name, 
        con=database.engine, 
        if_exists='append', 
        index=False
    )
    
    print('Arquivos ' + filename + ' inserido com sucesso no banco de dados!')

    delete_var(artefato)

@timer('Popular tabela')
def populate_table_with_filenames(
    database: Database, 
    table_info: TableInfo, 
    from_folder: str,
    filenames: list
):
    title=f'## Arquivos de {table_info.label.upper()}:'
    header=f'{FENCE}\n{title}\n{FENCE}'
    print(header)
    
    # Drop table (if exists)
    with database.engine.connect() as conn:
        query = text(f"DROP TABLE IF EXISTS {table_info.table_name};")

        # Execute the compiled SQL string
        conn.execute(query)
    
    # Inserir dados
    for filename in filenames:
        print('Trabalhando no arquivo: ' + filename + ' [...]')
        try:
            populate_table_with_filename(database, table_info, from_folder, filename)

        except Exception as e:
            print(f'Falha em salvar arquivo {filename} em tabela {table_info.table_name}. Erro: {e}')

    print(f'Arquivos de {table_info.label} finalizados!')

@timer(ident='Popular banco')
def populate_database(database, from_folder, files):
    for table_name in TABLES_INFO_DICT:
        
        label = TABLES_INFO_DICT[table_name]['label']
        columns = TABLES_INFO_DICT[table_name]['columns']
        expression = TABLES_INFO_DICT[table_name]['expression']
        encoding = TABLES_INFO_DICT[table_name]['encoding']
        transform_map = TABLES_INFO_DICT[table_name].get('transform_map', lambda x: x)

        table_info = TableInfo(label, table_name, columns, encoding, transform_map)
        populate_table_with_filenames(database, table_info, from_folder, files[table_name])
        
        print("""
        #############################################
        ## Processo de carga dos arquivos finalizado!
        #############################################
        """)

@timer('Criar indices do banco')
def generate_database_indices(engine):
    # Criar índices na base de dados:
    print("""
    #######################################
    ## Criar índices na base de dados [...]
    #######################################
    """)

    fields_tables=[
        ('empresa_cnpj', 'empresa',),
        ('estabelecimento_cnpj', 'estabelecimento',),
        ('socios_cnpj', 'socios',),
        ('simples_cnpj', 'simples',)
    ]
    mask="create index {field} on {table}(cnpj_basico); commit;"
    
    with engine.connect() as conn:
        queries=[ mask.format(field=field_, table=table_) for field_, table_ in fields_tables ]
        query_str="\n".join(queries)
        query = text(query_str)

        # Execute the compiled SQL string
        try:
            conn.execute(query)
        except Exception:
            pass
    
    print("""
    ############################################################
    ## Índices criados nas tabelas, para a coluna `cnpj_basico`:
    - empresa
    - estabelecimento
    - socios
    - simples
    ############################################################
    """)
