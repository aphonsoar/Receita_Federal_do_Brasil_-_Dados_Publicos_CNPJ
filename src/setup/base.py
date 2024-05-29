from os import getenv, path, getcwd
from dotenv import load_dotenv  
from typing import Union
from sqlalchemy import create_engine
from psycopg2 import OperationalError

from setup.logging import logger
from utils.misc import makedir 
from database.engine import Base
from database.schemas import Database
from database.engine import create_database

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

def setup_database(host: str, port: str, user: str, passw: str):
    # If the database name is not provided, use a default name
    DEFAULT_URI = f"postgresql://postgres:postgres@{host}:{port}/postgres"
    
    default_engine = create_engine(DEFAULT_URI)

    try:
        # Create database
        with default_engine.connect() as conn:
            # For PostgreSQL, a new database cannot be created within a transaction block
            descrip='Base de dados para gravar os dados públicos de CNPJ da Receita Federal do Brasil'

            conn.execute("commit")
            conn.execute('''CREATE DATABASE "Dados_RFB"
                            WITH OWNER = postgres
                            ENCODING = 'UTF8'
                            CONNECTION LIMIT = -1;''')
            
            conn.execute(f'''COMMENT ON DATABASE "Dados_RFB"
                            IS {descrip};''')
            
            conn.execute(f"CREATE USER {user} WITH PASSWORD '{passw}';")
            conn.execute(f"GRANT pg_read_all_data TO {user};")
            conn.execute(f"GRANT pg_write_all_data TO {user};")
    
    except OperationalError as e:
        logger.error(f"Error creating database: {e}")

def init_database() -> Union[Database, None]:
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
        port = int(getenv('POSTGRES_PORT', '5432'))
        database_name = getenv('POSTGRES_NAME')
        
        setup_database(host, port, user, passw)

        # Connect to the database
        db_uri = f'postgresql://{user}:{passw}@{host}:{port}/{database_name}'

        # Create the database engine and session maker
        timeout=5*60*60 # 5 hours
        database_obj = create_database(db_uri, timeout=timeout)

        # Create all tables defined using the Base class (if not already created)
        Base.metadata.create_all(database_obj.engine)
        
        logger.info('Connection to the database established!')
        return database_obj
    
    except OperationalError as e:
        logger.error(f"Error connecting to database: {e}")
        return None

