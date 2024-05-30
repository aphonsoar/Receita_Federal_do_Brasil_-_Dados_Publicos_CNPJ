from os import getenv, path, getcwd
from dotenv import load_dotenv  
from typing import Union
from psycopg2 import OperationalError

from setup.logging import logger
from utils.misc import makedir 
from database.models import Base
from database.schemas import Database
from database.engine import create_database
from utils.docker import get_postgres_host

def get_sink_folder():
    """
    Get the output and extracted file paths based on the environment variables or default paths.

    Returns:
        Tuple[str, str]: A tuple containing the output file path and the extracted file path.
    """
    env_path = path.join(getcwd(), '.env')
    load_dotenv(env_path)
    
    root_path = path.join(getcwd(), 'data') 
    default_output_file_path = path.join(root_path, 'DOWNLOAD_FILES')
    default_input_file_path = path.join(root_path, 'EXTRACTED_FILES')
    
    # Read details from ".env" file:
    output_route = getenv('DOWNLOAD_PATH', default_output_file_path)
    extract_route = getenv('EXTRACT_PATH', default_input_file_path)
    
    # Create the output and extracted folders if they do not exist
    output_folder = path.join(root_path, output_route)
    extract_folder = path.join(root_path, extract_route)
        
    makedir(output_folder)
    makedir(extract_folder)
    
    return output_folder, extract_folder

def init_database() -> Union[Database, None]:
    """
    Connects to a PostgreSQL database using environment variables for connection details.

    Returns:
        Database: A NamedTuple with engine and conn attributes for the database connection.
        None: If there was an error connecting to the database.
    
    """
    env_path = path.join(getcwd(), '.env')
    load_dotenv(env_path)
    
    # Get the host based on the environment
    if getenv('ENVIRONMENT') == 'docker':
        host = get_postgres_host()
    else: 
        host = getenv('POSTGRES_HOST', 'localhost')

    try:
        # Get environment variables
        port = int(getenv('POSTGRES_PORT', '5432'))
        user = getenv('POSTGRES_USER', 'postgres')
        passw = getenv('POSTGRES_PASSWORD', 'postgres')
        database_name = getenv('POSTGRES_NAME')
        
        # setup_database(host, port, sudo_user, sudo_pwd, user, passw, database_name )
        
        # Connect to the database
        db_uri = f'postgresql://{user}:{passw}@{host}:{port}/{database_name}'

        # Create the database engine and session maker
        timeout=5*60*60 # 5 hours
        database_obj = create_database(db_uri, session_timeout=timeout)

        # Create all tables defined using the Base class (if not already created)
        Base.metadata.create_all(database_obj.engine)
        
        logger.info('Connection to the database established!')
        return database_obj
    
    except OperationalError as e:
        logger.error(f"Error connecting to database: {e}")
        return None

