from os import makedirs, getenv, environ, path, getcwd
from dotenv import load_dotenv

from src.utils.database import setup_database
from src.utils.misc import makedir 

def get_sink_folder():
    root_path = path.join(getcwd(), 'data/') 
    default_output_file_path = path.join(root_path, 'OUTPUT_FILES')
    default_input_file_path = path.join(root_path, 'EXTRACTED_FILES') 

    # Read details from ".env" file:
    output_files = getenv('OUTPUT_FILES_PATH', default_output_file_path)
    extracted_files = getenv('EXTRACTED_FILES_PATH', default_input_file_path)

    makedir(output_files)
    makedir(extracted_files)
        
    return output_files, extracted_files

def setup_etl():
    # Load to environment variables
    load_dotenv()

    # Folders to 
    output_files_path, extracted_files_path = get_sink_folder()
    
    # Database with setup connection
    database = setup_database()

    return database, output_files_path, extracted_files_path
