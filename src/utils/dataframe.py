import pandas as pd

from setup.logging import logger

def to_sql(dataframe: pd.DataFrame, **kwargs):
    '''
    Inserts the records from a DataFrame into a database table.

    Parameters:
        dataframe (pd.DataFrame): The DataFrame containing the records to be inserted.
        tablename (str): The name of the table in the database.
        filename (str): The name of the file being processed.
        if_exists (str): Action to take if the table already exists in the database.
        conn (sqlalchemy.engine.Connection): The database connection.
        index (bool): Whether to include the DataFrame index as a column in the table.
        verbose (bool): Whether to print verbose output.

    Returns:
        None

    Raises:
        Exception: If there is an error inserting the records into the table.
    '''
    
    # Query arguments
    tablename = kwargs.get('tablename')
    filename = kwargs.get('filename')
    if_exists = kwargs.get('if_exists')
    conn = kwargs.get('conn')
    index = kwargs.get('index')

    # Query arguments
    query_args = {
        "name": tablename,
        "if_exists": if_exists,
        "con": conn,
        "index": index
    }

    # Break the dataframe into chunks
    try:
        dataframe.to_sql(**query_args)
    
    except Exception as e:
        summary = f"Failed to insert content of file {filename} on table {tablename}."
        message = f"{summary}: {e}"
        logger.error(message)
