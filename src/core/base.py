from os import getenv, path, getcwd
from dotenv import load_dotenv  
from typing import Union
from sqlalchemy import create_engine
from psycopg2 import OperationalError
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

from setup.logging import logger
from models.pydantic import Database
from utils.misc import makedir 

def get_sink_folder():
    """
    Get the output and extracted file paths based on the environment variables or default paths.

    Returns:
        Tuple[str, str]: A tuple containing the output file path and the extracted file path.
    """
    env_path = path.join(getcwd(), '.env')
    load_dotenv(env_path)
    
    root_path = path.join(getcwd(), 'data') 
    default_output_file_path = path.join(root_path, 'OUTPUT_FILES')
    default_input_file_path = path.join(root_path, 'EXTRACTED_FILES')
    
    # Read details from ".env" file:
    output_route = getenv('OUTPUT_PATH', default_output_file_path)
    extract_route = getenv('EXTRACT_PATH', default_input_file_path)
    
    # Create the output and extracted folders if they do not exist
    output_folder = path.join(root_path, output_route)
    extract_folder = path.join(root_path, extract_route)
        
    makedir(output_folder)
    makedir(extract_folder)
    
    return output_folder, extract_folder

def setup_database() -> Union[Database, None]:
    """
    Connects to a PostgreSQL database using environment variables for connection details.

    Returns:
        Database: A NamedTuple with engine and conn attributes for the database connection.
        None: If there was an error connecting to the database.
    
    """
    env_path = path.join(getcwd(), '.env')
    load_dotenv(env_path)
    
    try:
        # Get environment variables
        user = getenv('POSTGRES_USER', 'postgres')
        passw = getenv('POSTGRES_PASSWORD', 'postgres')
        host = getenv('POSTGRES_HOST', 'localhost')
        port = getenv('POSTGRES_PORT', '5432')
        database_name = getenv('POSTGRES_NAME')
        
        # Connect to the database
        db_uri = f'postgresql://{user}:{passw}@{host}:{port}/{database_name}'
        
        engine = create_engine(db_uri)
        SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
        
        # Create all tables defined using the Base class (if not already created)
        Base = declarative_base()
        Base.metadata.create_all(engine)
        
        logger.info('Connection to the database established!')
        return Database(engine=engine, session_maker=SessionLocal)
    
    except OperationalError as e:
        summary = "Error connecting to database"
        logger.error(f"{summary}: {e}")
        return None

