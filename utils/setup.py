"""
Script para otimizar PostgreSQL para ETL de dados CNPJ
Execute este script UMA VEZ para configurar o banco otimamente
"""

import psycopg2
import sys
from pathlib import Path
from config import Config

# Configurações otimizadas para PostgreSQL em Windows 10, 16GB RAM, i7
POSTGRESQL_OPTIMIZATIONS = {
    # Configurações de memória (para 16GB total, deixamos ~8GB para PG)
    'shared_buffers': '2GB',                    # 25% da RAM para cache compartilhado
    'effective_cache_size': '6GB',              # Estimativa do cache total disponível
    'work_mem': '256MB',                        # Memória para operações de sort/hash
    'maintenance_work_mem': '1GB',              # Para CREATE INDEX, VACUUM, etc
    'wal_buffers': '64MB',                      # Buffer para WAL (Write-Ahead Log)

    # Configurações de checkpoint e WAL para ETL
    'max_wal_size': '4GB',                      # Maior WAL = menos checkpoints
    'min_wal_size': '1GB',                      # Tamanho mínimo do WAL
    'checkpoint_completion_target': '0.9',      # Distribui I/O do checkpoint
    'wal_writer_delay': '200ms',                # Frequência de flush do WAL

    # Configurações de performance para ETL
    'synchronous_commit': 'off',                # Melhor performance para ETL (menos durabilidade)
    'fsync': 'on',                             # Mantém para segurança básica
    'commit_delay': '100000',                   # Micro delay para agrupar commits
    'commit_siblings': '10',                    # Threshold para commit_delay

    # Configurações de conexão e CPU
    'max_connections': '50',                    # Suficiente para ETL + aplicações
    'max_worker_processes': '8',                # Baseado no i7 (normalmente 8 cores)
    'max_parallel_workers': '6',                # Deixa 2 cores livres
    'max_parallel_workers_per_gather': '4',     # Para queries paralelas

    # Configurações de I/O
    'effective_io_concurrency': '200',          # Para SSDs modernos
    'random_page_cost': '1.1',                 # Assume SSD (padrão 4.0 é para HDD)
    'seq_page_cost': '1.0',                    # Custo sequencial

    # Configurações de logging (para ETL, menos logging = melhor performance)
    'log_statement': 'none',                    # Desabilita log de statements
    'log_min_duration_statement': '5000',       # Só loga queries > 5s
    'log_checkpoints': 'on',                   # Útil para monitorar ETL
    'log_lock_waits': 'on',                    # Debug de locks

    # Configurações específicas para análise de texto
    'default_text_search_config': 'portuguese', # Para busca textual em PT-BR
}

# Extensions úteis para análise de dados CNPJ
USEFUL_EXTENSIONS = [
    'pg_stat_statements',   # Estatísticas de queries
    'pg_trgm',             # Busca fuzzy em texto
    'unaccent',            # Remove acentos para busca
    'btree_gin',           # Índices GIN para tipos básicos
]

# Índices adicionais recomendados
RECOMMENDED_INDEXES = """
-- Índices específicos para análises comuns de CNPJ

-- Busca textual otimizada
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_empresa_razao_social_trgm 
ON empresa USING gin(razao_social gin_trgm_ops);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_estabelecimento_fantasia_trgm 
ON estabelecimento USING gin(nome_fantasia gin_trgm_ops);

-- Análises geográficas
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_estabelecimento_uf_municipio 
ON estabelecimento(uf, municipio);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_estabelecimento_cep 
ON estabelecimento(cep) WHERE cep IS NOT NULL;

-- Análises setoriais
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_estabelecimento_cnae_uf 
ON estabelecimento(cnae_principal, uf);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_empresa_porte_natureza 
ON empresa(porte_empresa, natureza_juridica);

-- Análises de situação cadastral
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_estabelecimento_situacao_data 
ON estabelecimento(situacao_cadastral, data_situacao_cadastral);

-- Análises de sócios
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_socios_qualificacao_pais 
ON socios(qualificacao_socio, pais);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_socios_faixa_etaria 
ON socios(faixa_etaria) WHERE faixa_etaria IS NOT NULL;

-- Simples Nacional
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_simples_opcao_data 
ON simples(opcao_simples, data_opcao_simples);
"""

class PostgreSQLOptimizer:
    def __init__(self):
        self.conn = None

    def connect(self):
        """Conecta ao PostgreSQL"""
        try:
            self.conn = psycopg2.connect(
                dbname=Config.DB_NAME,
                user=Config.DB_USER,
                password=Config.DB_PASSWORD,
                host=Config.DB_HOST,
                port=Config.DB_PORT
            )
            self.conn.autocommit = True
            print("✅ Conectado ao PostgreSQL")
            return True
        except Exception as e:
            print(f"❌ Erro ao conectar: {e}")
            return False

    def check_current_settings(self):
        """Verifica configurações atuais"""
        print("\n📊 CONFIGURAÇÕES ATUAIS DO POSTGRESQL")
        print("=" * 60)

        with self.conn.cursor() as cursor:
            for setting, recommended in POSTGRESQL_OPTIMIZATIONS.items():
                cursor.execute(f"SHOW {setting};")
                current = cursor.fetchone()[0]
                print(f"{setting:<30} | Atual: {current:<15} | Recomendado: {recommended}")

    def apply_optimizations(self, dry_run=True):
        """Aplica otimizações (dry_run=True apenas mostra o que seria feito)"""
        print(f"\n🔧 {'SIMULAÇÃO DE ' if dry_run else ''}APLICAÇÃO DE OTIMIZAÇÕES")
        print("=" * 60)

        changes_made = 0

        with self.conn.cursor() as cursor:
            for setting, value in POSTGRESQL_OPTIMIZATIONS.items():
                try:
                    # Verifica valor atual
                    cursor.execute(f"SHOW {setting};")
                    current = cursor.fetchone()[0]

                    if current != value:
                        if dry_run:
                            print(f"🔄 {setting}: {current} → {value}")
                        else:
                            cursor.execute(f"ALTER SYSTEM SET {setting} = '{value}';")
                            print(f"✅ {setting}: {current} → {value}")
                        changes_made += 1
                    else:
                        print(f"✓ {setting}: já otimizado ({current})")

                except Exception as e:
                    print(f"❌ Erro ao configurar {setting}: {e}")

        if changes_made > 0:
            if dry_run:
                print(f"\n💡 {changes_made} configurações seriam alteradas")
                print("💡 Execute com --apply para aplicar as mudanças")
            else:
                print(f"\n✅ {changes_made} configurações alteradas")
                print("⚠️  REINICIE o PostgreSQL para aplicar todas as mudanças!")

                # Recarrega configurações que podem ser aplicadas sem restart
                try:
                    cursor.execute("SELECT pg_reload_conf();")
                    print("🔄 Configurações recarregadas (algumas requerem restart)")
                except Exception as e:
                    print(f"⚠️  Erro ao recarregar configurações: {e}")
        else:
            print("\n✅ Todas as configurações já estão otimizadas!")

    def install_extensions(self):
        """Instala extensions úteis"""
        print("\n🔌 INSTALANDO EXTENSIONS")
        print("=" * 40)

        with self.conn.cursor() as cursor:
            for ext in USEFUL_EXTENSIONS:
                try:
                    cursor.execute(f"CREATE EXTENSION IF NOT EXISTS {ext};")
                    print(f"✅ Extension {ext} instalada")
                except Exception as e:
                    print(f"⚠️  Erro ao instalar {ext}: {e}")

    def create_recommended_indexes(self):
        """Cria índices recomendados para análise de dados CNPJ"""
        print("\n📇 CRIANDO ÍNDICES RECOMENDADOS")
        print("=" * 40)
        print("⚠️  Esta operação pode demorar vários minutos...")

        with self.conn.cursor() as cursor:
            try:
                cursor.execute(RECOMMENDED_INDEXES)
                print("✅ Índices recomendados criados com sucesso")
            except Exception as e:
                print(f"❌ Erro ao criar índices: {e}")

    def analyze_database(self):
        """Executa ANALYZE para atualizar estatísticas"""
        print("\n📈 ATUALIZANDO ESTATÍSTICAS DO BANCO")
        print("=" * 40)

        with self.conn.cursor() as cursor:
            try:
                cursor.execute("ANALYZE;")
                print("✅ Estatísticas atualizadas")
            except Exception as e:
                print(f"❌ Erro ao atualizar estatísticas: {e}")

    def generate_postgresql_conf(self):
        """Gera arquivo postgresql.conf otimizado"""
        conf_content = "# Configurações otimizadas para ETL CNPJ\n"
        conf_content += "# Gerado automaticamente - Windows 10, 16GB RAM, i7\n\n"

        for setting, value in POSTGRESQL_OPTIMIZATIONS.items():
            conf_content += f"{setting} = '{value}'\n"

        conf_file = Path("postgresql_cnpj_optimized.conf")
        conf_file.write_text(conf_content, encoding='utf-8')

        print(f"\n📄 Arquivo de configuração gerado: {conf_file}")
        print("💡 Você pode incluir este arquivo no postgresql.conf com:")
        print(f"   include '{conf_file.absolute()}'")

    def close(self):
        """Fecha conexão"""
        if self.conn:
            self.conn.close()

def main():
    """Função principal"""
    import argparse

    parser = argparse.ArgumentParser(description="Otimizador PostgreSQL para ETL CNPJ")
    parser.add_argument("--check", action="store_true", help="Verifica configurações atuais")
    parser.add_argument("--apply", action="store_true", help="Aplica otimizações")
    parser.add_argument("--extensions", action="store_true", help="Instala extensions úteis")
    parser.add_argument("--indexes", action="store_true", help="Cria índices recomendados")
    parser.add_argument("--analyze", action="store_true", help="Atualiza estatísticas")
    parser.add_argument("--generate-conf", action="store_true", help="Gera arquivo postgresql.conf")
    parser.add_argument("--all", action="store_true", help="Executa todas as otimizações")

    args = parser.parse_args()

    if not any(vars(args).values()):
        # Se nenhum argumento, mostra help
        parser.print_help()
        return

    optimizer = PostgreSQLOptimizer()

    if not optimizer.connect():
        return

    try:
        if args.check or args.all:
            optimizer.check_current_settings()

        if args.apply or args.all:
            optimizer.apply_optimizations(dry_run=False)
        elif not args.apply and not args.all:
            # Se não especificou --apply, faz dry run
            optimizer.apply_optimizations(dry_run=True)

        if args.extensions or args.all:
            optimizer.install_extensions()

        if args.indexes or args.all:
            optimizer.create_recommended_indexes()

        if args.analyze or args.all:
            optimizer.analyze_database()

        if args.generate_conf or args.all:
            optimizer.generate_postgresql_conf()

        print(f"\n🎉 OTIMIZAÇÃO CONCLUÍDA!")
        print("=" * 30)
        print("💡 Dicas importantes:")
        print("   - Reinicie o PostgreSQL após aplicar configurações")
        print("   - Monitor o desempenho durante o ETL")
        print("   - Ajuste configurações se necessário")

    finally:
        optimizer.close()

if __name__ == "__main__":
    main()