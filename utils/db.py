import psycopg2
from sqlalchemy import create_engine, event, text
from sqlalchemy.pool import QueuePool
from sqlalchemy.engine import Engine
import sqlite3
from utils.config import Config

def get_optimized_engine():
    """Cria engine otimizada para o banco PostgreSQL"""
    settings = Config.get_optimal_settings()

    # String de conexão simplificada (removendo parâmetros inválidos)
    connection_string = (
        f"postgresql://{Config.DB_USER}:{Config.DB_PASSWORD}@"
        f"{Config.DB_HOST}:{Config.DB_PORT}/{Config.DB_NAME}"
    )

    print(f"🔗 Conectando ao banco: {Config.DB_HOST}:{Config.DB_PORT}/{Config.DB_NAME}")
    print(f"👤 Usuário: {Config.DB_USER}")

    # Configurações do pool de conexões
    try:
        engine = create_engine(
            connection_string,
            poolclass=QueuePool,
            pool_size=settings['db_pool_size'],
            max_overflow=settings['db_pool_size'] * 2,
            pool_pre_ping=True,  # Verifica conexões antes de usar
            pool_recycle=3600,   # Reconecta a cada hora
            echo=False,          # Desabilita log SQL para performance
            connect_args={
                "application_name": "cnpj_etl_optimized",
                # Configurações específicas do PostgreSQL para ETL
                "keepalives_idle": "600",
                "keepalives_interval": "30",
                "keepalives_count": "3",
            }
        )

        # Testa a conexão
        with engine.connect() as conn:
            result = conn.execute(text("SELECT version();"))
            version = result.fetchone()[0]
            print(f"✅ Conexão estabelecida com sucesso!")
            print(f"📊 PostgreSQL: {version.split(',')[0]}")

        # Event listeners para otimização
        @event.listens_for(engine, "connect")
        def set_postgresql_optimizations(dbapi_connection, connection_record):
            """Aplica otimizações específicas do PostgreSQL para ETL"""
            try:
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

                    # Para texto search otimizado (opcional)
                    cursor.execute("SET default_text_search_config = 'portuguese'")
                    
                    dbapi_connection.commit()
                    
            except Exception as e:
                print(f"⚠️  Aviso: Algumas otimizações PostgreSQL falharam: {e}")

        return engine

    except Exception as e:
        print(f"❌ Erro ao criar engine do banco: {e}")
        print("💡 Verifique se:")
        print("   - PostgreSQL está rodando")
        print("   - Banco 'Dados_RFB' existe")
        print("   - Usuário tem permissões corretas")
        print("   - Senha está correta no arquivo .env")
        raise

def get_connection_optimized():
    """Conexão psycopg2 otimizada para operações específicas"""
    try:
        conn = psycopg2.connect(
            dbname=Config.DB_NAME,
            user=Config.DB_USER,
            password=Config.DB_PASSWORD,
            host=Config.DB_HOST,
            port=Config.DB_PORT,
            application_name="cnpj_etl_direct",
            connect_timeout=30
        )
        return conn
    except Exception as e:
        print(f"❌ Erro ao conectar via psycopg2: {e}")
        raise

def test_connection():
    """Testa a conexão com o banco de dados"""
    print("🧪 Testando conexão com o banco...")
    
    try:
        engine = get_optimized_engine()
        with engine.connect() as conn:
            result = conn.execute(text("SELECT current_database(), current_user, version();"))
            db, user, version = result.fetchone()
            
            print(f"✅ Teste de conexão bem-sucedido!")
            print(f"   📊 Banco: {db}")
            print(f"   👤 Usuário: {user}")
            print(f"   🔧 Versão: {version.split(',')[0]}")
            
            # Testa se consegue criar tabela temporária
            conn.execute(text("CREATE TEMP TABLE test_table (id INTEGER);"))
            conn.execute(text("DROP TABLE test_table;"))
            print(f"   ✅ Permissões de escrita: OK")
            
            return True
            
    except Exception as e:
        print(f"❌ Teste de conexão falhou: {e}")
        return False

def ensure_database_exists():
    """Garante que o banco de dados existe"""
    print(f"🔍 Verificando se banco '{Config.DB_NAME}' existe...")
    
    try:
        # Conecta ao banco postgres (sempre existe) para verificar se nosso banco existe
        temp_conn = psycopg2.connect(
            dbname="postgres",
            user=Config.DB_USER,
            password=Config.DB_PASSWORD,
            host=Config.DB_HOST,
            port=Config.DB_PORT,
            connect_timeout=30
        )
        temp_conn.autocommit = True
        
        with temp_conn.cursor() as cursor:
            # Verifica se banco existe
            cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s;", (Config.DB_NAME,))
            exists = cursor.fetchone()
            
            if not exists:
                print(f"📊 Criando banco '{Config.DB_NAME}'...")
                cursor.execute(f'CREATE DATABASE "{Config.DB_NAME}";')
                print(f"✅ Banco '{Config.DB_NAME}' criado com sucesso!")
            else:
                print(f"✅ Banco '{Config.DB_NAME}' já existe")
        
        temp_conn.close()
        return True
        
    except Exception as e:
        print(f"❌ Erro ao verificar/criar banco: {e}")
        print("💡 Talvez você precise criar o banco manualmente:")
        print(f"   CREATE DATABASE \"{Config.DB_NAME}\";")
        return False

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