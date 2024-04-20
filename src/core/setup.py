from os import makedirs, getenv, environ, path, getcwd
from dotenv import load_dotenv

from os import getenv, path
from typing import Union
from sqlalchemy import create_engine
from psycopg2 import connect, OperationalError

from core.models import Database
from utils.misc import makedir 

def get_sink_folder():
    root_path = path.join(getcwd(), 'data') 
    default_output_file_path = path.join(root_path, 'OUTPUT_FILES')
    default_input_file_path = path.join(root_path, 'EXTRACTED_FILES') 
    
    # Read details from ".env" file:
    output_files = getenv('OUTPUT_FILES_PATH', default_output_file_path)
    extracted_files = getenv('EXTRACTED_FILES_PATH', default_input_file_path)

    makedir(output_files)
    makedir(extracted_files)

    return output_files, extracted_files

def setup_database() -> Union[Database, None]:
    """
    Connects to a PostgreSQL database using environment variables for connection details.

    Returns:
        Database: A NamedTuple with engine and conn attributes for the database connection.
        None: If there was an error connecting to the database.
    
    >>> setup_database()
    """
    
    try:
        # Get environment variables
        user = getenv('POSTGRES_USER')
        passw = getenv('POSTGRES_PASSWORD')
        host = getenv('POSTGRES_HOST', 'localhost')
        port = getenv('POSTGRES_PORT', '5432')
        database_name = getenv('POSTGRES_NAME')
        
        # Connect to the database
        db_uri = f'postgresql://{user}:{passw}@{host}:{port}/{database_name}'
        engine = create_engine(db_uri)
        
        db_info = f'dbname={database_name} user={user} host={host} port={port} password={passw}'
        
        conn = connect(db_info)

        print('Connection to the database established!')
        return Database(engine, conn)
    
    except OperationalError as e:
        print(f"Error connecting to database: {e}")
        return None

def setup_etl():
    # Load to environment variables
    env_path = path.join(getcwd(), '.env')
    load_dotenv(env_path)

    # Folders to 
    output_files_path, extracted_files_path = get_sink_folder()

    # Database with setup connection
    database = setup_database()

    return database, output_files_path, extracted_files_path
