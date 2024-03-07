from setup import setup_etl
from setup import set_output_folders

from etl_pipeline import buscar_dados_receita_federal, \
  ler_dados_receita_federal
from database_utils import popular_banco, criar_indices_banco

print(
    """ 
      - Desenvolvido por: Aphonso Henrique do Amaral Rafael
      - Refatorado por: Bruno Henrique Lobo Netto Peixoto
      - Contribua com esse projeto aqui: 
      https://github.com/brunolnetto/Receita_Federal_do_Brasil_-_Dados_Publicos_CNPJ
    """
)

# Tempo de execução do processo (em segundos): 17.770 (4hrs e 57 min)

# ###############################
# Tamanho dos arquivos (Linhas):
# empresa = 45.811.638
# estabelecimento = 48.421.619
# socios = 20.426.417
# simples = 27.893.923
# ###############################

output_files_path, extracted_files_path, engine, conn = setup_etl()
# buscar_dados_receita_federal(output_files_path, extracted_files_path)

# Ler e inserir dados
arquivos = ler_dados_receita_federal(extracted_files_path)

popular_banco(engine, conn, extracted_files_path, arquivos)
criar_indices_banco(conn)

print("""Fim do processo! Você pode utilizar o banco de dados!""")