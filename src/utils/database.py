from timy import timer
from os import path
import pandas as pd
from sqlalchemy import text

from utils.dataframe import to_sql
from utils.misc import delete_var, update_progress, get_line_count
from core.constants import (
    TABLES_INFO_DICT, 
    CHUNK_SIZE
)
from core.models import Database, TableInfo
from utils.logging import logger

##########################################################################
## LOAD AND TRANSFORM
##########################################################################
def populate_table_with_filename(
    database: Database, 
    table_info: TableInfo,
    to_folder: str,
    filename: str
): 
    len_cols=len(table_info.columns)
    data = {
        str(col): []
        for col in range(0, len_cols)
    }
    
    df = pd.DataFrame(data)
    dtypes = { column: str for column in table_info.columns }
    extracted_file_path = path.join(to_folder, filename)
    
    csv_read_props = {
        "filepath_or_buffer": extracted_file_path,
        "sep": ';', 
        "skiprows": 0,
        "chunksize": CHUNK_SIZE, 
        "header": None, 
        "dtype": dtypes,
        "encoding": table_info.encoding,
        "low_memory": False,
        "memory_map": True
    }
    
    row_count_estimation = get_line_count(extracted_file_path)
    
    for index, df_chunk in enumerate(pd.read_csv(**csv_read_props)):
        # Tratamento do arquivo antes de inserir na base:
        df_chunk = df_chunk.reset_index()
        del df_chunk['index']
        
        # Renomear colunas
        df_chunk.columns = table_info.columns
        df_chunk = table_info.transform_map(df_chunk)

        update_progress(index * CHUNK_SIZE, row_count_estimation, filename)
        
        # Gravar dados no banco:
        to_sql(
            df_chunk, 
            filename=extracted_file_path,
            tablename=table_info.table_name, 
            conn=database.engine, 
            if_exists='append', 
            index=False,
            verbose=False
        )
    
    logger.info('Arquivos ' + filename + ' inserido com sucesso no banco de dados!')

    delete_var(df)

@timer('Popular tabela')
def populate_table_with_filenames(
    database: Database, 
    table_info: TableInfo, 
    from_folder: str,
    filenames: list
):
    title=f'Arquivos de tabela {table_info.label.upper()}:'
    logger.info(title)
    
    # Drop table (if exists)
    with database.engine.connect() as conn:
        query_str=f"DROP TABLE IF EXISTS {table_info.table_name};"
        query = text(query_str)

        # Execute the compiled SQL string
        conn.execute(query)
    
    # Inserir dados
    for filename in filenames:
        logger.info('Trabalhando no arquivo: ' + filename + ' [...]')
        try:
            populate_table_with_filename(database, table_info, from_folder, filename)

        except Exception as e:
            summary=f'Falha em salvar arquivo {filename} em tabela {table_info.table_name}'
            logger.info(f'{summary}: {e}')
    
    logger.info(f'Arquivos de {table_info.label} finalizados!')


def populate_table(database: Database, table_name: str, from_folder: str, table_files: list):
    table_info = TABLES_INFO_DICT[table_name]
    
    label = table_info['label']
    columns = table_info['columns']
    encoding = table_info['encoding']
    transform_map = table_info.get('transform_map', lambda x: x)

    table_info = TableInfo(label, table_name, columns, encoding, transform_map)
    populate_table_with_filenames(database, table_info, from_folder, table_files)

@timer(ident='Popular banco')
def populate_database(database, from_folder, files):
    for table_name in TABLES_INFO_DICT:
        table_filenames = files[table_name]
        populate_table(database, table_name, from_folder, table_filenames)
                
    logger.info("Processo de carga dos arquivos CNPJ finalizado!")

@timer('Criar indices do banco')
def generate_database_indices(engine):
    # Criar índices na base de dados:
    logger.info("Criando índices na base de dados [...]")

    tables = ['empresa', 'estabelecimento', 'socios', 'simples']
    
    # Criar índices
    fields_tables = [(f'{table}_cnpj', table) for table in tables]
    mask="create index {field} on {table}(cnpj_basico); commit;"
    
    with engine.connect() as conn:
        queries = [ mask.format(field=field_, table=table_) for field_, table_ in fields_tables ]
        query_str="\n".join(queries)
        query = text(query_str)

        # Execute the compiled SQL string
        try:
            conn.execute(query)
        except Exception:
            pass
    
    logger.info("Índices criados nas tabelas, para a coluna `cnpj_basico`: {tables}")
    
 