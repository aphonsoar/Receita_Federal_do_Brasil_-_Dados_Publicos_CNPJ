from dotenv import load_dotenv
from pathlib import Path
from utils import get_env 

#%%
load_dotenv()

extracted_files = Path(get_env('EXTRACTED_FILES_PATH'))
output_files = Path(get_env('OUTPUT_FILES_PATH'))
dados_rf = 'http://200.152.38.155/CNPJ/'

arquivos = {
    'empresa': [],
    'estabelecimento': [],
    'socios': [],
    'simples': [],
    'cnae': [],
    'moti': [],
    'munic': [],
    'natju': [],
    'pais': [],
    'quals': [],
}

# Dados da conexão com o BD
user=get_env('DB_USER')
password=get_env('DB_PASSWORD')
host=get_env('DB_HOST')
port=get_env('DB_PORT')
database=get_env('DB_NAME')

bar = [
  '░', '░', '░', '░', '░',
  '░', '░', '░', '░', '░',
  '░', '░', '░', '░', '░',
  '░', '░', '░', '░', '░'
]