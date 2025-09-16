import os
import psutil
from dotenv import load_dotenv

# Carrega variáveis de ambiente do arquivo .env
# Procura o arquivo .env no diretório atual e nos diretórios relacionados
env_paths = [
    '.env',                    # Diretório atual
    'code/.env',              # Subpasta code
    '../.env',                # Diretório pai
    os.path.join(os.path.dirname(__file__), '.env'),  # Mesmo diretório do config.py
    os.path.join(os.path.dirname(__file__), '..', '.env'),  # Diretório pai do config.py
    os.path.join(os.path.dirname(__file__), 'code', '.env'),  # code relativo ao config.py
]

env_loaded = False
for env_path in env_paths:
    if os.path.exists(env_path):
        try:
            load_dotenv(env_path)
            print(f"✅ Arquivo .env carregado de: {os.path.abspath(env_path)}")
            env_loaded = True
            break
        except Exception as e:
            continue

if not env_loaded:
    print("⚠️  Arquivo .env não encontrado nos locais esperados:")
    for path in env_paths:
        abs_path = os.path.abspath(path)
        exists = "✓" if os.path.exists(path) else "✗"
        print(f"   {exists} {abs_path}")
    print("💡 Usando variáveis de ambiente do sistema ou valores padrão")

class Config:
    OUTPUT_FILES = os.getenv("OUTPUT_FILES_PATH", "./downloads")
    EXTRACTED_FILES = os.getenv("EXTRACTED_FILES_PATH", "./extracted")
    DB_USER = os.getenv("DB_USER", "postgres")
    DB_PASSWORD = os.getenv("DB_PASSWORD", "")
    DB_HOST = os.getenv("DB_HOST", "localhost")
    DB_PORT = os.getenv("DB_PORT", "5432")
    DB_NAME = os.getenv("DB_NAME", "cnpj_dados")
    BASE_URL = "https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/2025-09/"

    # Configurações de Performance
    @staticmethod
    def get_optimal_settings():
        """Calcula configurações ótimas baseadas no hardware"""
        try:
            memory_gb = psutil.virtual_memory().total / (1024**3)
            cpu_count = psutil.cpu_count()
        except:
            # Valores padrão se psutil falhar
            memory_gb = 8.0
            cpu_count = 4

        # Para 16GB RAM, deixamos ~4GB para SO e outras apps
        available_memory_gb = memory_gb * 0.75

        # Chunk size otimizado (baseado em ~100MB por chunk para CSVs típicos da RFB)
        chunk_size = min(500000, int(available_memory_gb * 50000))
        chunk_size = max(50000, chunk_size)  # Mínimo de 50k linhas

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

    # Debug das configurações carregadas
    @classmethod
    def show_config(cls):
        """Mostra as configurações carregadas para debug"""
        print(f"\n📋 CONFIGURAÇÕES ATIVAS:")
        print(f"   OUTPUT_FILES: {cls.OUTPUT_FILES}")
        print(f"   EXTRACTED_FILES: {cls.EXTRACTED_FILES}")
        print(f"   DB_HOST: {cls.DB_HOST}")
        print(f"   DB_PORT: {cls.DB_PORT}")
        print(f"   DB_USER: {cls.DB_USER}")
        print(f"   DB_NAME: {cls.DB_NAME}")
        print(f"   DB_PASSWORD: {'*' * len(cls.DB_PASSWORD) if cls.DB_PASSWORD else 'VAZIO'}")
        
        # Verifica se os diretórios são válidos
        for name, path in [("OUTPUT_FILES", cls.OUTPUT_FILES), ("EXTRACTED_FILES", cls.EXTRACTED_FILES)]:
            try:
                abs_path = os.path.abspath(path)
                print(f"   {name} (absoluto): {abs_path}")
                if os.path.exists(path):
                    print(f"   ✅ {name}: Diretório existe")
                else:
                    print(f"   ⚠️  {name}: Diretório será criado")
            except Exception as e:
                print(f"   ❌ {name}: Erro - {e}")

# Mostra configurações ao carregar o módulo (apenas se executado diretamente)
if __name__ == "__main__" or env_loaded:
    Config.show_config()