import psycopg2
from sqlalchemy import create_engine, event
from sqlalchemy.pool import QueuePool
from sqlalchemy.engine import Engine
import sqlite3
from config import Config

def get_optimized_engine():
    """Cria engine otimizada para o banco PostgreSQL"""
    settings = Config.get_optimal_settings()

    # String de conexão com parâmetros otimizados
    connection_string = (
        f"postgresql://{Config.DB_USER}:{Config.DB_PASSWORD}@"
        f"{Config.DB_HOST}:{Config.DB_PORT}/{Config.DB_NAME}"
        "?application_name=cnpj_etl"
        "&connect_timeout=30"
        "&command_timeout=300"
        "&options=-c%20synchronous_commit%3Doff"  # Melhora performance de inserção
        "&options=-c%20wal_buffers%3D16MB"
        "&options=-c%20checkpoint_segments%3D32"
    )

    # Configurações do pool de conexões
    engine = create_engine(
        connection_string,
        poolclass=QueuePool,
        pool_size=settings['db_pool_size'],
        max_overflow=settings['db_pool_size'] * 2,
        pool_pre_ping=True,  # Verifica conexões antes de usar
        pool_recycle=3600,   # Reconecta a cada hora
        echo=False,          # Desabilita log SQL para performance
        connect_args={
            "options": "-c timezone=America/Sao_Paulo",
            "application_name": "cnpj_etl_optimized",
            # Configurações específicas do PostgreSQL para ETL
            "keepalives_idle": "600",
            "keepalives_interval": "30",
            "keepalives_count": "3",
        }
    )

    # Event listeners para otimização
    @event.listens_for(engine, "connect")
    def set_postgresql_optimizations(dbapi_connection, connection_record):
        """Aplica otimizações específicas do PostgreSQL para ETL"""
        with dbapi_connection.cursor() as cursor:
            # Configurações para melhor performance de inserção em massa
            cursor.execute("SET synchronous_commit = OFF")  # Não espera flush do WAL
            cursor.execute("SET wal_buffers = '16MB'")      # Buffer maior para WAL
            cursor.execute("SET checkpoint_completion_target = 0.9")
            cursor.execute("SET wal_writer_delay = '200ms'")
            cursor.execute("SET commit_delay = 100000")      # Micro delay para agrupar commits
            cursor.execute("SET commit_siblings = 10")       # Threshold para commit_delay
            cursor.execute("SET effective_cache_size = '4GB'")  # Assume 4GB disponível para cache
            cursor.execute("SET work_mem = '256MB'")         # Memória para operações de sort/hash
            cursor.execute("SET maintenance_work_mem = '1GB'")  # Para operações de manutenção
            cursor.execute("SET max_wal_size = '4GB'")       # WAL maior para menos checkpoints
            cursor.execute("SET shared_preload_libraries = 'pg_stat_statements'")

            # Para texto search otimizado (opcional)
            cursor.execute("SET default_text_search_config = 'portuguese'")

    return engine

def get_connection_optimized():
    """Conexão psycopg2 otimizada para operações específicas"""
    return psycopg2.connect(
        dbname=Config.DB_NAME,
        user=Config.DB_USER,
        password=Config.DB_PASSWORD,
        host=Config.DB_HOST,
        port=Config.DB_PORT,
        application_name="cnpj_etl_direct",
        # Configurações de timeout
        connect_timeout=30,
        # Buffer de rede maior
        options="-c synchronous_commit=off -c wal_buffers=16MB"
    )

class BulkCopyLoader:
    """Loader especializado usando COPY para inserção ultra-rápida"""

    def __init__(self):
        self.conn = get_connection_optimized()
        self.conn.autocommit = False

    def bulk_copy_from_file(self, file_path: str, table_name: str, columns: list):
        """Usa COPY FROM para carregar arquivo diretamente no PostgreSQL"""
        try:
            with self.conn.cursor() as cursor:
                # Prepara comando COPY
                columns_str = ','.join(columns)
                copy_sql = f"""
                    COPY {table_name} ({columns_str})
                    FROM STDIN
                    WITH (
                        FORMAT CSV,
                        DELIMITER ';',
                        ENCODING 'LATIN1',
                        NULL '',
                        QUOTE '"'
                    )
                """

                # Executa COPY
                with open(file_path, 'r', encoding='latin1') as f:
                    cursor.copy_expert(copy_sql, f)

                self.conn.commit()
                print(f"✅ COPY realizado para {table_name} de {file_path}")

        except Exception as e:
            self.conn.rollback()
            print(f"❌ Erro no COPY para {table_name}: {e}")
            raise

    def close(self):
        """Fecha conexão"""
        if self.conn:
            self.conn.close()

# Função de compatibilidade com código existente
def get_engine():
    """Mantém compatibilidade com código existente"""
    return get_optimized_engine()

def get_connection():
    """Mantém compatibilidade com código existente"""
    return get_connection_optimized()