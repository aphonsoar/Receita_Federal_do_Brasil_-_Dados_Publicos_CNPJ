from sqlalchemy import text
from datetime import datetime
from os import path, getcwd

from core.setup import get_sink_folder, setup_database
from core.etl import get_RF_data, load_database
from core.scrapper import scrap_RF
from utils.logging import logger
from utils.misc import process_filename, tuple_list_to_dict
from utils.models import create_audit
from utils.misc import list_zip_contents

print(
  """ 
    - Projeto           : Receita Federal do Brasil - Dados Públicos CNPJ
    - Objetivo          : Baixar, transformar e carregar dados da Receita Federal do Brasil
    - Fonte de dados    : http://200.152.38.155/CNPJ/
    - Desenvolvido por  : [
        (Aphonso Henrique do Amaral Rafael, @aphonsoar), 
        (Bruno Henrique Lobo Netto Peixoto, @brunolnetto)
      ]
    - Contribua         : https://github.com/brunolnetto/Receita_Federal_do_Brasil_-_Dados_Publicos_CNPJ
  """
)

# ############################################################################################ 
# INFORMAÇÕES SOBRE O PROCESSO
# #############################################################################################
# Tempo de execução do processo (em segundos): 12.657 (3hrs e 31 min)
# #############################################################################################
# Tempo de execução por arquivo:
# 
#   - Empresa                   : 4676 s
#   - Socios                    : 1479 s
#   - Estabelecimento           : 3331 s
#   - Simples nacional          : 3169 s
#   - CNAE                      : 0.22 s
#   - Motivos de situação atual : 0.03 s
#   - Municípios                : 0.45 s
#   - Natureza jurídica         : 0.45 s
#   - País                      : 0.45 s
#   - Qualificação de sócios    : 0.03 s
# 
# #############################################################################################

# Pastas e banco de dados
output_files_path, extracted_files_path = get_sink_folder()
database = setup_database()
files_info = scrap_RF()

audits = []
for file_info in files_info:
    audit = create_audit(database, file_info.filename, file_info.updated_at)
    if audit:
        audits.append(audit)

# For testing purposes
audits = [audits[0]]

# Buscar dados
audits, zip_files = get_RF_data(audits, output_files_path, extracted_files_path)

# Carregar banco
load_database(database, extracted_files_path, audits)

logger.info("""Fim do processo! Você pode utilizar o banco de dados!""")
