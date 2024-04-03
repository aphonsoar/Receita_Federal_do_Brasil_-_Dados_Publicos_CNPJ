from os import makedirs, getenv, environ, path, getcwd
from database_utils import conectar_banco

def load_env_path():
    root_path=path.join(getcwd(), 'data/') 
    
    environ['OUTPUT_FILES_PATH'] = path.join(root_path, 'OUTPUT_FILES')
    environ['EXTRACTED_FILES_PATH'] = path.join(root_path, 'EXTRACTED_FILES') 
    environ['POSTGRES_HOST'] = 'localhost'
    environ['POSTGRES_PORT'] = '5432'
    environ['POSTGRES_USER'] = 'postgres'
    environ['POSTGRES_PASSWORD'] = 'postgres'
    environ['POSTGRES_DB'] = 'Dados_RFB'

def definir_pastas_destino():
    # Read details from ".env" file:
    output_files = getenv('OUTPUT_FILES_PATH')
    extracted_files = getenv('EXTRACTED_FILES_PATH')

    if not path.exists(output_files):
        makedirs(output_files)
        print('Diretório de saída definido: \n' + repr(str(output_files)))

    else:
        print(f'Diretório de saída {repr(str(output_files))} já existe!')

    if not path.exists(extracted_files):
        makedirs(extracted_files)
        print('Diretório de extração definido: \n' + repr(str(extracted_files)))

    else:
        print(f'Diretório de extração {repr(str(extracted_files))} já existe!')
        
    return output_files, extracted_files

def configurar_etl():
    load_env_path()
    
    output_files_path, extracted_files_path = definir_pastas_destino()
    engine, conn = conectar_banco()

    return output_files_path, extracted_files_path, engine, conn