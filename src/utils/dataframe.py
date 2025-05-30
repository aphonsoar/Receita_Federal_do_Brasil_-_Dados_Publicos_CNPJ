"""
DataFrame utility functions, primarily for database interactions.

This module provides helper functions for working with Pandas DataFrames,
such as writing DataFrame contents to an SQL table.
"""
import pandas as pd

from setup.logging import logger

INSERT_CHUNK_SIZE = 1000 # Default chunk size for inserting data into SQL.

def to_sql(dataframe: pd.DataFrame, **kwargs) -> None:
    """
    Inserts records from a Pandas DataFrame into a database table using `to_sql`.

    This function wraps pandas' `to_sql` method, providing logging for errors.
    It uses a predefined `INSERT_CHUNK_SIZE` for chunked insertion.

    Args:
        dataframe (pd.DataFrame): The DataFrame containing records to be inserted.
        **kwargs: Keyword arguments that are passed directly to `pandas.DataFrame.to_sql()`.
                  Expected kwargs include:
            name (str): Name of the SQL table. (Required by pandas.to_sql)
            con (sqlalchemy.engine.Engine or sqlalchemy.engine.Connection):
                      SQLAlchemy engine or connection. (Required by pandas.to_sql)
            if_exists (str, optional): How to behave if the table already exists.
                                       Options: 'fail', 'replace', 'append'. Default: 'fail'.
            index (bool, optional): Whether to write DataFrame index as a column. Default: False.
            filename (str, optional): The name of the source file being processed,
                                      used for logging error messages.

    Returns:
        None

    Raises:
        Exception: Catches and logs exceptions from `dataframe.to_sql()`, but does not
                   re-raise them. The error is logged via the configured logger.
                   The underlying `to_sql` might raise various database-related exceptions.
    """
    
    # Extract necessary arguments for logging and operation
    # 'name' is the kwarg for table name in pandas.to_sql, not 'tablename'
    tablename = kwargs.get('name')
    filename = kwargs.get('filename') # For logging purposes

    # Construct arguments for pandas.DataFrame.to_sql
    # We ensure 'chunksize' is always set from our constant.
    # Other kwargs like 'name', 'con', 'if_exists', 'index' are passed as is.
    sql_kwargs = kwargs.copy() # Start with a copy of all provided kwargs
    sql_kwargs['chunksize'] = INSERT_CHUNK_SIZE # Set/overwrite chunksize

    # Break the dataframe into chunks and insert
    try:
        dataframe.to_sql(**sql_kwargs)
    
    except Exception as e:
        summary = f"Failed to insert content of file {filename} on table {tablename}."
        message = f"{summary}: {e}"
        logger.error(message)
