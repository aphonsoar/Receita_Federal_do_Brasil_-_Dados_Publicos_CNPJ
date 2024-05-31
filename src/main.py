""" 
  - Nome do projeto : ETL - CNPJs da Receita Federal do Brasil
  - Objetivo        : Baixar, transformar e carregar dados da Receita Federal do Brasil
"""

from setup.base import get_sink_folder, init_database
from core.etl import CNPJ_ETL
from core.utils.schemas import create_file_groups
from database.utils.models import create_audits

# Folders and database setup
download_folder, extract_folder = get_sink_folder()
database = init_database()

# Source and target
data_url = 'http://200.152.38.155/CNPJ'
filename = 'LAYOUT_DADOS_ABERTOS_CNPJ.pdf'

# Você também pode acessar por: https://dados.rfb.gov.br/CNPJ/
layout_url = f'{data_url}/{filename}'

scrapper = CNPJ_ETL(
    database,
    data_url,
    layout_url,
    download_folder,
    extract_folder,
    is_parallel=False,
    delete_zips=False
)

# Scrap data
scrapper.run()

