import os
import psutil
from dotenv import load_dotenv

load_dotenv()

class Config:
    OUTPUT_FILES = os.getenv("OUTPUT_FILES_PATH", "./downloads")
    EXTRACTED_FILES = os.getenv("EXTRACTED_FILES_PATH", "./extracted")
    DB_USER = os.getenv("DB_USER")
    DB_PASSWORD = os.getenv("DB_PASSWORD")
    DB_HOST = os.getenv("DB_HOST", "localhost")
    DB_PORT = os.getenv("DB_PORT", "5432")
    DB_NAME = os.getenv("DB_NAME")
    BASE_URL = "https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/2025-09/"

    # Configurações de Performance
    @staticmethod
    def get_optimal_settings():
        """Calcula configurações ótimas baseadas no hardware"""
        memory_gb = psutil.virtual_memory().total / (1024**3)
        cpu_count = psutil.cpu_count()

        # Para 16GB RAM, deixamos ~4GB para SO e outras apps
        available_memory_gb = memory_gb * 0.75

        # Chunk size otimizado (baseado em ~100MB por chunk para CSVs típicos da RFB)
        chunk_size = min(500000, int(available_memory_gb * 50000))

        # Workers para download/extração (limitado por I/O)
        io_workers = min(4, cpu_count)

        # Workers para processamento de dados
        cpu_workers = max(2, cpu_count - 2)  # Deixa 2 cores livres

        # Pool de conexões do banco
        db_pool_size = min(10, cpu_workers * 2)

        return {
            'chunk_size': chunk_size,
            'io_workers': io_workers,
            'cpu_workers': cpu_workers,
            'db_pool_size': db_pool_size,
            'max_memory_mb': int(available_memory_gb * 1024 * 0.8)  # 80% da memória disponível
        }