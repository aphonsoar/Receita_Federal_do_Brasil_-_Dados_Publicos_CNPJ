from pathlib import Path

YEAR = 2025
MONTH = str(5).zfill(2)
DB_FILE_TEMPLATE = "dadosrfb_{YEAR}{MONTH}.duckdb"
PARQUET_DIR = Path("parquet_out")
FILES_URL_TEMPLATE = "https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/{YEAR}-{MONTH}"
LAYOUT_URL = "https://www.gov.br/receitafederal/dados/cnpj-metadados.pdf"
