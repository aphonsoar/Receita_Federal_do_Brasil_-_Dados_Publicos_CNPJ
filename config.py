import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    OUTPUT_FILES = os.getenv("OUTPUT_FILES_PATH")
    EXTRACTED_FILES = os.getenv("EXTRACTED_FILES_PATH")
    DB_USER = os.getenv("DB_USER")
    DB_PASSWORD = os.getenv("DB_PASSWORD")
    DB_HOST = os.getenv("DB_HOST")
    DB_PORT = os.getenv("DB_PORT")
    DB_NAME = os.getenv("DB_NAME")
    BASE_URL = "https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/2025-09/"