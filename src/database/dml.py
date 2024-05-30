from os import path, environ
import pandas as pd
from sqlalchemy import text

from utils.dataframe import to_sql
from utils.misc import delete_var, update_progress, get_line_count
from core.constants import TABLES_INFO_DICT, CHUNK_SIZE

from core.schemas import TableInfo
from database.schemas import Database
from setup.logging import logger

##########################################################################
## LOAD AND TRANSFORM
##########################################################################
def populate_table_with_filename(
    database: Database, 
    table_info: TableInfo,
    to_folder: str,
    filename: str
): 
    """
    Populates a table in the database with data from a file.

    Args:
        database (Database): The database object.
        table_info (TableInfo): The table information object.
        to_folder (str): The folder path where the file is located.
        filename (str): The name of the file.

    Returns:
        None
    """
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
        
        # Gravar dados no banco
        to_sql(
            df_chunk, 
            filename=extracted_file_path,
            tablename=table_info.table_name, 
            conn=database.engine, 
            if_exists='append', 
            index=False,
            verbose=False
        )
    
    update_progress(row_count_estimation, row_count_estimation, filename)
    print()
    
    logger.info('Arquivo ' + filename + ' inserido com sucesso no banco de dados!')

    delete_var(df)

def populate_table_with_filenames(
    database: Database, 
    table_info: TableInfo, 
    from_folder: str,
    filenames: list
):
    """
    Populates a table in the database with data from multiple files.

    Args:
        database (Database): The database object.
        table_info (TableInfo): The table information object.
        from_folder (str): The folder path where the files are located.
        filenames (list): A list of file names.

    Returns:
        None
    """
    title=f'Arquivos de tabela {table_info.label.upper()}:'
    logger.info(title)
    
    # Drop table (if exists)
    with database.engine.connect() as conn:
        query=text(f"DROP TABLE IF EXISTS {table_info.table_name};")
        
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


def table_name_to_table_info(table_name: str) -> TableInfo:
    table_info_dict = TABLES_INFO_DICT[table_name]
    
    # Get table info
    label = table_info_dict['label']
    zip_group = table_info_dict['group']
    columns = table_info_dict['columns']
    encoding = table_info_dict['encoding']
    transform_map = table_info_dict.get('transform_map', lambda x: x)
    expression = table_info_dict['expression']

    # Create table info object
    return TableInfo(
        label, zip_group, table_name, columns, encoding, transform_map, expression
    )


def populate_table(
    database: Database, 
    table_name: str, 
    from_folder: str, 
    table_files: list
):
    """
    Populates a table in the database with data from multiple files.

    Args:
        database (Database): The database object.
        table_name (str): The name of the table.
        from_folder (str): The folder path where the files are located.
        table_files (list): A list of file names.

    Returns:
        None
    """
    table_info = table_name_to_table_info(table_name)
    populate_table_with_filenames(database, table_info, from_folder, table_files)


def generate_tables_indices(engine, tables):
    """
    Generates indices for the database tables.

    Args:
        engine: The database engine.

    Returns:
        None
    """
    # Criar índices na base de dados:
    logger.info("Criando índices na base de dados [...]")

    # Criar índices
    fields_tables = [(f'{table}_cnpj', table) for table in tables]
    mask="create index {field} on {table}(cnpj_basico);"
    
    try:
        with engine.connect() as conn:
            queries = [ 
                mask.format(field=field_, table=table_) 
                for field_, table_ in fields_tables 
            ]
            query=text("\n".join(queries) + "\n" + "commit")
            
            # Execute the compiled SQL string
            try:
                conn.execute(query)
            except Exception as e:
                logger.error(f"Erro ao criar índices: {e}")

        message = f"Índices criados nas tabelas, para a coluna `cnpj_basico`: {tables}"
        logger.info(message)
    
    except Exception as e:
        logger.error(f"Erro ao criar índices: {e}") 

