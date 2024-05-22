import pandas as pd

from core.constants import CHUNK_SIZE
from utils.misc import update_progress
from utils.logging import logger

def dataframe_chunker_gen(df: pd.DataFrame):
    for i in range(0, len(df), CHUNK_SIZE):
        yield df[i:i + CHUNK_SIZE]

def to_sql(dataframe: pd.DataFrame, **kwargs):
    '''
    Quebra em pedacos a tarefa de inserir registros no banco
    '''
    total = len(dataframe)
    
    # Query arguments
    tablename = kwargs.get('tablename')
    filename = kwargs.get('filename')
    if_exists = kwargs.get('if_exists')
    conn = kwargs.get('conn')
    index = kwargs.get('index')
    verbose = kwargs.get('verbose')

    # Query arguments
    query_args = {
        "name": tablename,
        "if_exists": if_exists,
        "conn": conn,
        "index": index
    }

    # Break the dataframe into chunks
    try:
        dataframe.to_sql(**query_args)
    
    except Exception as e:
        summary = f"Failed to insert content of file {filename} on table {tablename}."
        message = f"{summary}: {e}"
        logger.error(message)
