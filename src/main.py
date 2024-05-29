""" 
  - Nome do projeto : ETL - CNPJs da Receita Federal do Brasil
  - Objetivo        : Baixar, transformar e carregar dados da Receita Federal do Brasil
"""
from os import getenv
from shutil import rmtree

from setup.logging import logger
from setup.base import get_sink_folder, init_database
from utils.models import  (
  create_audits, 
  create_audit_metadata, 
  insert_audit,
)
from core.etl import get_RF_data, load_RF_data_on_database
from core.scrapper import scrap_RF


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

# Folders and database setup
output_path, extracted_path = get_sink_folder()
database = init_database()

# Get files info
files_info = scrap_RF()

# NOTE: Test purposes only
if getenv('ENVIRONMENT', 'development') == 'development':
  files_info = [ 
    files_info[0], 
    files_info[21],
    files_info[22],
    files_info[23],
    files_info[24]  
  ]

# Create audits
audits = create_audits(database, files_info)

if audits:
  # Retrieve data
  is_parallel = True
  audits = get_RF_data(audits, output_path, extracted_path, is_parallel)
  
  # Create audit metadata
  audit_metadata = create_audit_metadata(database, audits, output_path)

  # Deletar arquivos zip baixados
  rmtree(output_path)

  # Load database
  audit_metadata = load_RF_data_on_database(database, extracted_path, audit_metadata)

  # Insert audit metadata
  for audit in audit_metadata.audit_list:
      insert_audit(database, audit)

logger.info("""Fim do processo! Você pode utilizar o banco de dados!""")
