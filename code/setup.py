from os import makedirs

from .utils.env import getEnv
from .setup import set_output_folders
from .database_connection import connect_db
from .utils import load_env_path

def set_output_folders():
    #%%
    # Read details from ".env" file:
    output_files = None
    extracted_files = None
    try:
        output_files = getEnv('OUTPUT_FILES_PATH')
        makedirs(output_files)

        extracted_files = getEnv('EXTRACTED_FILES_PATH')
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