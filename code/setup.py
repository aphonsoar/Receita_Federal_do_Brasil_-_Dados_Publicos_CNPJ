from os import makedirs, getenv, environ

from database_utils import connect_db
from utils import this_folder

def root_path():
    return this_folder()+"/../"

def load_env_path():
    environ['OUTPUT_FILES_PATH'] = root_path()+'data/OUTPUT_FILES'
    environ['EXTRACTED_FILES_PATH'] = root_path()+'data/EXTRACTED_FILES'
    environ['POSTGRES_HOST'] = 'localhost'
    environ['POSTGRES_PORT'] = '5432'
    environ['POSTGRES_USER'] = 'postgres'
    environ['POSTGRES_PASSWORD'] = 'postgres'
    environ['POSTGRES_DB'] = 'Dados_RFB'

def set_output_folders():
    # Read details from ".env" file:
    output_files = None
    extracted_files = None

    try:
        output_files = getenv('OUTPUT_FILES_PATH')
        print(output_files)
        makedirs(output_files)

        extracted_files = getenv('EXTRACTED_FILES_PATH')
        print(extracted_files)
        makedirs(extracted_files)

        print('Diretórios definidos: \n' +
            'output_files: ' + str(output_files)  + '\n' +
            'extracted_files: ' + str(extracted_files))
    
    except:
        ERROR_MESSAGE = 'Erro ao criar diretórios de saída! Verifique o arquivo ".env".'
        print(ERROR_MESSAGE)
    
    return output_files, extracted_files

def setup_etl():
    load_env_path()
    
    output_files_path, extracted_files_path = set_output_folders()
    engine, conn, cur = connect_db()

    return output_files_path, extracted_files_path, engine, conn, cur