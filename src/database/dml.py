"""
Data Manipulation Language (DML) operations for the database.

This module includes functions for populating database tables from CSV files
and for generating database indices. It handles chunked reading of large CSVs,
data transformation based on table-specific configurations, and retry mechanisms
for database operations.
"""
from os import path, getcwd
import pandas as pd
from sqlalchemy import text
from sqlalchemy.exc import OperationalError
import numpy as np

from utils.misc import delete_var, update_progress, get_line_count
from core.constants import TABLES_INFO_DICT
from core.schemas import TableInfo
from database.schemas import Database
from setup.logging import logger

MAX_RETRIES=3

##########################################################################
## LOAD AND TRANSFORM
##########################################################################
# Chunk size for download and extraction 
READ_CHUNK_SIZE = 50000
def populate_table_with_filename(
    database: Database, table_info: TableInfo, source_folder: str, filename: str
): 
    """
    Populates a table in the database with data from a file.

    Args:
        database (Database): The database connection and session management object.
        table_info (TableInfo): An object containing metadata for the table,
                                including column names, data types, encoding,
                                and transformation functions.
        source_folder (str): The directory where the source CSV file is located.
        filename (str): The name of the CSV file to load.

    Returns:
        None

    Processing Steps:
        1. Reads the CSV file in chunks using pandas. `csv_read_props` defines
           parameters like separator (';'), chunk size, string data type enforcement,
           encoding, and error handling for bad lines.
        2. For each chunk:
            a. Resets index and removes the added 'index' column.
            b. Ensures all data is cast to string type initially.
            c. Renames columns according to `table_info.columns`.
            d. Applies any table-specific transformations via `table_info.transform_map`.
            e. Attempts to write the transformed chunk to the SQL table using `to_sql`.
            f. Implements a retry loop (`MAX_RETRIES`) for `to_sql` operation to handle
               transient database errors.
        3. Updates progress visually and logs success or failure.

    Raises:
        Exception: If inserting a chunk fails after `MAX_RETRIES`.
    """
    dtypes = { column: str for column in table_info.columns } # Enforce string type for all columns initially
    extracted_file_path = path.join(getcwd(), source_folder, filename)
    
    # Properties for reading the CSV file with pandas
    csv_read_props = {
        "filepath_or_buffer": extracted_file_path,
        "sep": ';',                     # CSV delimiter
        "skiprows": 0,                  # No rows to skip at the beginning
        "chunksize": READ_CHUNK_SIZE,   # Process file in manageable chunks
        "dtype": dtypes,                # Apply initial string data types
        "header": None,                 # CSV has no header row
        "encoding": table_info.encoding,# Use encoding specified in table_info
        "low_memory": False,            # Important for mixed type inference, though dtypes helps
        "memory_map": True,             # Use memory mapping for potentially faster reads
        "on_bad_lines": 'warn',         # Log warnings for malformed lines
        "keep_default_na": False,       # Avoid interpreting 'NA', 'NULL' as NaN if they are valid strings
        "encoding_errors": 'replace'    # Replace encoding errors with a placeholder
    }

    row_count = get_line_count(extracted_file_path)
    chunk_count = np.ceil(row_count/READ_CHUNK_SIZE)
    for index, df_chunk in enumerate(pd.read_csv(**csv_read_props)):
        for retry_count in range(MAX_RETRIES):
            try:
                # Transform chunk
                df_chunk = df_chunk.reset_index()

                # Remove index column
                del df_chunk['index']

                # Cast for string
                for column in df_chunk.columns:
                    df_chunk[column] = df_chunk[column].astype(str)

                # Rename columns
                df_chunk.columns = table_info.columns
                df_chunk = table_info.transform_map(df_chunk)

                update_progress(index*READ_CHUNK_SIZE, row_count, filename)

                # Query arguments
                query_args = {
                    "name": table_info.table_name,
                    "if_exists": 'append',
                    "con": database.engine,
                    "index": index,
                    "chunksize": 1000,
                }

                # Break the dataframe into chunks
                df_chunk.to_sql(**query_args)
                break

            except Exception as e:
                progress=f"({retry_count+1}/{MAX_RETRIES})"
                summary=f"Failed to insert chunk {index} of file {extracted_file_path}"
                logger.error(f'{progress} {summary}: {e}.')

            if(retry_count == MAX_RETRIES-1):
                raise Exception(f'Failed to insert chunk {index} of file {extracted_file_path}.')

        if(index == chunk_count-1):
            break
    
    update_progress(row_count, row_count, filename)
    print()
    
    logger.info('File ' + filename + ' inserted with success on database!')

    delete_var(df_chunk)

def populate_table_with_filenames(
    database: Database, table_info: TableInfo, source_folder: str, filenames: list
):
    """
    Populates a table in the database with data from multiple files.

    Args:
        database (Database): The database connection and session management object.
        table_info (TableInfo): Metadata for the target table.
        source_folder (str): Directory containing the source CSV files.
        filenames (list): A list of CSV file names to load into the table.

    Returns:
        None

    Processing:
        1. Drops the target table if it already exists to ensure fresh data load.
        2. Iterates through each filename in the `filenames` list.
        3. For each file, calls `populate_table_with_filename` to load its data.
        4. Implements a retry mechanism for each file load attempt.
    """
    title=f'Populating table {table_info.label.upper()} ({table_info.table_name}) from files: {filenames}'
    logger.info(title)
    
    # Drop table (if exists) to ensure a clean load
    logger.info(f"Dropping table {table_info.table_name} if it exists...")
    with database.engine.begin() as conn:
        drop_query = text(f"DROP TABLE IF EXISTS {table_info.table_name};")
        try:
            conn.execute(drop_query)
            logger.info(f"Table {table_info.table_name} dropped successfully or did not exist.")
        except OperationalError as e:
            logger.error(f"Failed to drop table {table_info.table_name}. This might be okay if it's the first run. Error: {e}")
    
    # Insert data from each specified file
    for filename in filenames:
        logger.info(f'Processing file: {filename} for table {table_info.table_name}')
        file_path = path.join(getcwd(), source_folder, filename) # Full path for logging
        try:
            for retry_count in range(MAX_RETRIES):
                try:
                    populate_table_with_filename(database, table_info, source_folder, filename)
                    logger.info(f"Successfully processed file {filename} into {table_info.table_name}.")
                    break # Success, exit retry loop for this file
                except Exception as e:
                    logger.error(f'Attempt ({retry_count+1}/{MAX_RETRIES}) failed to process file {file_path} into {table_info.table_name}. Error: {e}')
                    if retry_count == MAX_RETRIES - 1: # Last attempt failed
                        raise Exception(f'All {MAX_RETRIES} attempts failed for file {file_path}.')
        except Exception as e:
            # This catches the re-raised exception from the retry loop or other unexpected errors
            logger.error(f'Failed to save file {file_path} into table {table_info.table_name}. Error: {e}')
            # Depending on requirements, one might choose to continue with other files or halt.
            # Current behavior: logs error and continues with the next file.
    
    logger.info(f'Finished processing all specified files for table {table_info.label}.')


def table_name_to_table_info(table_name: str) -> TableInfo:
    """
    Retrieves table metadata from the global `TABLES_INFO_DICT` constant.

    Constructs and returns a `TableInfo` object containing configuration details
    for the specified table name, such as its label, column definitions,
    encoding, and any specific data transformation map.

    Args:
        table_name (str): The name of the table for which to retrieve info.

    Returns:
        TableInfo: An object containing metadata for the table.

    Raises:
        KeyError: If the `table_name` is not found in `TABLES_INFO_DICT`.
    """
    table_info_dict = TABLES_INFO_DICT[table_name]
    
    # Get table info from the dictionary
    label = table_info_dict['label']
    zip_group = table_info_dict['group']
    columns = table_info_dict['columns']
    encoding = table_info_dict['encoding']
    transform_map = table_info_dict.get('transform_map', lambda x: x)
    expression = table_info_dict['expression']

    # Create table info object
    return TableInfo(label, zip_group, table_name, columns, encoding, transform_map, expression)


def populate_table(
    database: Database, table_name: str, from_folder: str, table_files: list
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


def generate_tables_indices(engine, tables_to_indices):
    """
    Generates indices for the database tables.

    Args:
        engine: The SQLAlchemy database engine instance.
        tables_to_indices (Dict[str, Set[str]]): A dictionary where keys are table names (str)
                                                 and values are sets of column names (str)
                                                 on which to create indices.
                                                 Example: {"empresa": {"cnpj_basico"},
                                                           "estabelecimento": {"cnpj_basico", "uf"}}

    Returns:
        None

    Processing:
        Constructs and executes `CREATE INDEX` SQL statements for each specified
        table and column. Index names are generated as `f'{table_name}_{column_name}'`.
        Uses B-tree indices. Errors during index creation for individual columns
        are logged, but the process attempts to continue with other indices.
    """
    # Criar índices na base de dados:
    logger.info(f"Starting index generation process for tables: {list(tables_to_indices.keys())}")

    # Prepare list of (table_name, column_name, index_name) tuples
    fields_list = []
    for table_name, columns in tables_to_indices.items():
        for column_name in columns:
            fields_list.append((table_name, column_name, f'{table_name}_{column_name}'))

    # SQL template for creating an index. Using text() for SQLAlchemy compatibility.
    # Note: The "commit;" part in the original mask is typically not needed per statement
    # when using SQLAlchemy's connection, as commit is handled at the transaction level.
    # However, some databases might behave differently with DDL. For DuckDB, it's usually fine.
    index_creation_mask = "CREATE INDEX IF NOT EXISTS {index_name} ON {table_name} (\"{column_name}\");"

    # Execute index queries
    try:
        with engine.connect() as conn: # Use a single connection for all index creations
            transaction = conn.begin() # Start a transaction
            try:
                for table_name_, column_name_, index_name_ in fields_list:
                    query_str = index_creation_mask.format(
                        index_name=index_name_,
                        table_name=table_name_,
                        column_name=column_name_
                    )
                    query = text(query_str)
                    logger.debug(f"Executing index creation: {query_str}")

                    try:
                        conn.execute(query)
                        logger.info(f"Index {index_name_} created (or already existed) on column '{column_name_}' of table '{table_name_}'.")
                    except Exception as e:
                        # Log error for specific index creation and continue
                        logger.error(f"Error generating index {index_name_} on column '{column_name_}' for table '{table_name_}': {e}")

                transaction.commit() # Commit all successful index creations
                logger.info(f"Index generation process completed for tables: {list(tables_to_indices.keys())}")
            except Exception as e:
                logger.error(f"Error during the index creation transaction. Rolling back. Error: {e}")
                transaction.rollback() # Rollback if any error not caught per index occurred
    except Exception as e:
        logger.error(f"Failed to connect to the database or critical error during index generation: {e}")

