from src.core.setup import setup_etl
from src.core.etl import get_RF_data, load_database

print(
  """ 
    - Desenvolvido por: Aphonso Henrique do Amaral Rafael
    - Refatorado por: Bruno Henrique Lobo Netto Peixoto
    - Contribua com esse projeto aqui: 
    https://github.com/brunolnetto/Receita_Federal_do_Brasil_-_Dados_Publicos_CNPJ
  """
)

# #############################################################################################
# Tempo de execução do processo (em segundos): 12.657 (3hrs e 31 min)
#   - Empresa: 4676 s
#   - Socios: 1479 s
#   - Estabelecimento: 3331 s
#   - Simples nacional: 3169 s
#   - CNAE: 0.22 s
#   - Motivos de situação atual: 0.03 s
#   - Municípios: 0.45 s
#   - Natureza jurídica: 0.45 s
#   - País: 0.45 s
#   - Qualificação de sócios: 0.03 s
# 
# #############################################################################################
# Tamanho dos arquivos (Linhas):
# empresa = 45.811.638
# estabelecimento = 48.421.619
# socios = 20.426.417
# simples = 27.893.923
# #############################################################################################
output_files_path, extracted_files_path, database = setup_etl()

# # Buscar dados
get_RF_data(output_files_path, extracted_files_path, False)

# Carregar banco
# load_database(database, extracted_files_path)

print("""Fim do processo! Você pode utilizar o banco de dados!""")
