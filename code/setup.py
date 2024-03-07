from os import makedirs, getenv, environ, path
from database_utils import connect_db

def load_env_path():
    root_path='/media/brunolnetto/2cb2282b-76a8-4719-a96f-46a36b718b19/opt/data/'
    
    environ['OUTPUT_FILES_PATH'] = root_path+'OUTPUT_FILES'
    environ['EXTRACTED_FILES_PATH'] = root_path+'EXTRACTED_FILES'
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
        print(getenv('OUTPUT_FILES_PATH'))
        output_files = getenv('OUTPUT_FILES_PATH')

        if not path.exists(output_files):
            makedirs(output_files)
            print('Diretório de saída definido: \n' + repr(str(output_files)))

        else:
            print(f'Diretório de saída {repr(str(output_files))} já existe!')

        extracted_files = getenv('EXTRACTED_FILES_PATH')
        
        if not path.exists(extracted_files):
            makedirs(extracted_files)
            print('Diretório de extração definido: \n' + repr(str(extracted_files)))

        else:
            print(f'Diretío de extração {repr(str(extracted_files))} já existe!')
    
    except:
        ERROR_MESSAGE = 'Erro ao criar diretórios de saída! Verifique o arquivo ".env".'
        print(ERROR_MESSAGE)
    
    return output_files, extracted_files

def setup_etl():
    load_env_path()
    
    output_files_path, extracted_files_path = set_output_folders()
    engine, conn = connect_db()

    return output_files_path, extracted_files_path, engine, conn