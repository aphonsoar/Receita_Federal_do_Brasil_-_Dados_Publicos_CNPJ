import os
import sys
import pandas as pd
import numpy as np
from sqlalchemy import text, create_engine
from pathlib import Path
import psutil
import time
from typing import Dict, List
from tqdm import tqdm

# Adiciona o diretório pai ao path para encontrar os módulos
current_dir = Path(__file__).parent
parent_dir = current_dir.parent
sys.path.insert(0, str(parent_dir))

try:
    from utils.config import Config
except ImportError:
    print("❌ Erro: Não foi possível importar os módulos necessários")
    print("💡 Execute o script a partir da raiz do projeto:")
    print(" python main.py")
    sys.exit(1)

# Configurações otimizadas baseadas no hardware
def get_optimal_chunk_size():
    """Calcula chunk size otimizado baseado na RAM disponível"""
    memory_gb = psutil.virtual_memory().total / (1024**3)
    if memory_gb >= 16:
        return 500000  # 500k linhas para máquinas com bastante RAM
    elif memory_gb >= 8:
        return 250000  # 250k linhas para máquinas médias
    else:
        return 100000  # 100k linhas para máquinas com pouca RAM

OPTIMIZED_CHUNKSIZE = get_optimal_chunk_size()

class OptimizedDataFrameProcessor:
    """Classe para otimizar DataFrames antes da inserção"""

    @staticmethod
    def optimize_dtypes(df: pd.DataFrame, table_name: str) -> pd.DataFrame:
        """Otimiza tipos de dados para economizar memória"""
        type_optimizations = {
            'empresa': {
                'cnpj_basico': 'string',
                'porte_empresa': 'category',
                'capital_social': 'float32',
            },
            'estabelecimento': {
                'cnpj_basico': 'string',
                'cnpj_ordem': 'string',
                'cnpj_dv': 'string',
                'situacao_cadastral': 'category',
                'uf': 'category',
                'matriz_filial': 'category',
            },
            'socios': {
                'cnpj_basico': 'string',
                'identificador_socio': 'category',
                'qualificacao_socio': 'category',
                'pais': 'category',
                'faixa_etaria': 'category'
            }
        }

        if table_name in type_optimizations:
            for col, target_type in type_optimizations[table_name].items():
                if col in df.columns:
                    try:
                        if target_type == 'category':
                            df[col] = df[col].astype('category')
                        elif target_type == 'string':
                            df[col] = df[col].astype('string')
                        elif target_type == 'float32':
                            df[col] = pd.to_numeric(df[col], errors='coerce').astype('float32')
                    except Exception:
                        continue  # Mantém tipo original se falhar
        return df

    @staticmethod
    def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
        """Limpeza básica do DataFrame"""
        df.dropna(how='all', inplace=True)
        return df

def get_engine_connection():
    """Retorna uma conexão única com o banco de dados"""
    from utils.db import get_engine
    return get_engine()

def limpar_banco(engine):
    """Apaga todos os dados das tabelas antes de iniciar o carregamento."""
    print("🧹 Limpando tabelas do banco de dados...")
    tabelas = ['empresa', 'estabelecimento', 'socios', 'simples', 'cnae', 'moti', 'munic', 'natju', 'pais', 'quals']

    with engine.begin() as conn:
        for tabela in tabelas:
            try:
                conn.execute(text(f"TRUNCATE TABLE {tabela} CASCADE;"))
                print(f"✅ Tabela {tabela} limpa com sucesso.")
            except Exception as e:
                print(f"⚠️ Erro ao limpar tabela {tabela}: {e}")

def _load_csv_ultra_optimized(file_path, table_name, columns_map, engine, dtype=None, sep=";", encoding="latin1"):
    """Versão ultra-otimizada da função de carregamento, mantendo compatibilidade"""
    print(f"📊 Processando {Path(file_path).name} → {table_name}")
    try:
        processor = OptimizedDataFrameProcessor()

        pd_read_config = {
            'sep': sep,
            'dtype': dtype or {},
            'encoding': encoding,
            'header': None,
            'chunksize': OPTIMIZED_CHUNKSIZE,
            'low_memory': False,
            'na_values': ['', 'NULL', 'null', 'None', 'nan'],
            'keep_default_na': True,
            'engine': 'c',
            'on_bad_lines': 'skip',
        }

        total_rows = 0
        chunk_count = 0

        with engine.begin() as conn:  # Transação única para todos os chunks
            for chunk in pd.read_csv(file_path, **pd_read_config):
                chunk.rename(columns=columns_map, inplace=True)
                chunk = processor.optimize_dtypes(chunk, table_name)
                chunk = processor.clean_dataframe(chunk)

                if len(chunk) == 0:
                    continue

                try:
                    chunk.to_sql(
                        table_name,
                        conn,
                        if_exists="append",
                        index=False,
                        method='multi',
                        chunksize=min(50000, len(chunk))
                    )
                    total_rows += len(chunk)
                    chunk_count += 1
                except Exception as e:
                    print(f"⚠️ Erro ao inserir chunk {chunk_count + 1}: {e}")

                del chunk

        print(f"✅ {table_name}: {total_rows:,} linhas inseridas ({chunk_count} chunks)")
        return total_rows
    except Exception as e:
        print(f"❌ Erro fatal ao processar {file_path}: {e}")
        return 0

def load_empresa(files, engine):
    """Versão otimizada da função original"""
    col_map = {
        0: "cnpj_basico",
        1: "razao_social",
        2: "natureza_juridica",
        3: "qualificacao_responsavel",
        4: "capital_social",
        5: "porte_empresa",
        6: "ente_federativo_responsavel",
    }
    dtype_hints = {i: 'string' for i in range(7)}
    total_rows = 0
    for f in files:
        if f and os.path.exists(os.path.join(Config.EXTRACTED_FILES, f)):
            print(f"📊 Carregando EMPRESA: {f}")
            file_path = os.path.join(Config.EXTRACTED_FILES, f)
            rows = _load_csv_ultra_optimized(file_path, "empresa", col_map, engine, dtype_hints)
            total_rows += rows
        else:
            print(f"⚠️ Arquivo não encontrado: {f}")
    return total_rows

def load_estabelecimento(files, engine):
    """Versão otimizada da função original"""
    col_map = {
        0: "cnpj_basico",
        1: "cnpj_ordem",
        2: "cnpj_dv",
        3: "identificador_matriz_filial",
        4: "nome_fantasia",
        5: "situacao_cadastral",
        6: "data_situacao_cadastral",
        7: "motivo_situacao_cadastral",
        8: "nome_cidade_exterior",
        9: "pais",
        10: "data_inicio_atividade",
        11: "cnae_fiscal_principal",
        12: "cnae_fiscal_secundaria",
        13: "tipo_logradouro",
        14: "logradouro",
        15: "numero",
        16: "complemento",
        17: "bairro",
        18: "cep",
        19: "uf",
        20: "municipio",
        21: "ddd_1",
        22: "telefone_1",
        23: "ddd_2",
        24: "telefone_2",
        25: "ddd_fax",
        26: "fax",
        27: "correio_eletronico",
        28: "situacao_especial",
        29: "data_situacao_especial",
    }
    dtype_hints = {i: 'string' for i in range(30)}
    total_rows = 0
    for f in files:
        if f and os.path.exists(os.path.join(Config.EXTRACTED_FILES, f)):
            print(f"📊 Carregando ESTABELECIMENTO: {f}")
            file_path = os.path.join(Config.EXTRACTED_FILES, f)
            rows = _load_csv_ultra_optimized(file_path, "estabelecimento", col_map, engine, dtype_hints)
            total_rows += rows
        else:
            print(f"⚠️ Arquivo não encontrado: {f}")
    return total_rows

def load_socios(files, engine):
    """Versão otimizada da função original"""
    col_map = {
        0: "cnpj_basico",
        1: "identificador_socio",
        2: "nome_socio_razao_social",
        3: "cpf_cnpj_socio",
        4: "qualificacao_socio",
        5: "data_entrada_sociedade",
        6: "pais",
        7: "representante_legal",
        8: "nome_do_representante",
        9: "qualificacao_representante_legal",
        10: "faixa_etaria",
    }
    dtype_hints = {i: 'string' for i in range(11)}
    total_rows = 0
    for f in files:
        if f and os.path.exists(os.path.join(Config.EXTRACTED_FILES, f)):
            print(f"📊 Carregando SOCIOS: {f}")
            file_path = os.path.join(Config.EXTRACTED_FILES, f)
            rows = _load_csv_ultra_optimized(file_path, "socios", col_map, engine, dtype_hints)
            total_rows += rows
        else:
            print(f"⚠️ Arquivo não encontrado: {f}")
    return total_rows

def load_simples(files, engine):
    """Versão otimizada da função original"""
    col_map = {
        0: "cnpj_basico",
        1: "opcao_pelo_simples",
        2: "data_opcao_simples",
        3: "data_exclusao_simples",
        4: "opcao_mei",
        5: "data_opcao_mei",
        6: "data_exclusao_mei",
    }
    dtype_hints = {i: 'string' for i in range(7)}
    total_rows = 0
    for f in files:
        if f and os.path.exists(os.path.join(Config.EXTRACTED_FILES, f)):
            print(f"📊 Carregando SIMPLES: {f}")
            file_path = os.path.join(Config.EXTRACTED_FILES, f)
            rows = _load_csv_ultra_optimized(file_path, "simples", col_map, engine, dtype_hints)
            total_rows += rows
        else:
            print(f"⚠️ Arquivo não encontrado: {f}")
    return total_rows

def load_tabelas_auxiliares(files, table_name, columns, engine):
    """Versão otimizada das tabelas auxiliares"""
    col_map = {i: col for i, col in enumerate(columns)}
    dtype_hints = {i: 'string' for i in range(len(columns))}
    total_rows = 0
    for f in files:
        if f and os.path.exists(os.path.join(Config.EXTRACTED_FILES, f)):
            print(f"📊 Carregando {table_name.upper()}: {f}")
            file_path = os.path.join(Config.EXTRACTED_FILES, f)
            rows = _load_csv_ultra_optimized(file_path, table_name, col_map, engine, dtype_hints)
            total_rows += rows
        else:
            print(f"⚠️ Arquivo não encontrado: {f}")
    return total_rows

def criar_indices_ultra_otimizados(engine):
    """Cria índices ultra-otimizados para consultas de alta performance"""
    print("🔧 Criando índices ultra-otimizados...")

    indices_estrategicos = [
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_empresa_cnpj_hash ON empresa USING hash(cnpj_basico);",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_estabelecimento_cnpj_hash ON estabelecimento USING hash(cnpj_basico);",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_socios_cnpj_hash ON socios USING hash(cnpj_basico);",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_simples_cnpj_hash ON simples USING hash(cnpj_basico);",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_empresa_cnpj_btree ON empresa(cnpj_basico);",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_estabelecimento_cnpj_btree ON estabelecimento(cnpj_basico);",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_socios_cnpj_btree ON socios(cnpj_basico);",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_simples_cnpj_btree ON simples(cnpj_basico);",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_estabelecimento_uf_situacao ON estabelecimento(uf, situacao_cadastral);",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_estabelecimento_cnae_uf ON estabelecimento(cnae_fiscal_principal, uf);",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_empresa_porte_natureza ON empresa(porte_empresa, natureza_juridica);",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_estabelecimento_situacao ON estabelecimento(situacao_cadastral);",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_estabelecimento_uf ON estabelecimento(uf);",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_estabelecimento_municipio ON estabelecimento(municipio);",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_empresa_porte ON empresa(porte_empresa);",
    ]

    with engine.connect() as conn:
        total_indices = len(indices_estrategicos)
        print(f"📝 Criando {total_indices} índices...")
        success_count = 0
        for i, idx_sql in enumerate(indices_estrategicos, 1):
            try:
                conn.execute(text(idx_sql))
                conn.commit()
                success_count += 1
            except Exception as e:
                print(f"⚠️ Índice {i}/{total_indices} falhou: {str(e)[:100]}...")

        print(f"✅ Índices criados: {success_count}/{total_indices}")
        if success_count < total_indices:
            print("⚠️ Alguns índices falharam, mas o sistema continuará funcionando")
            print("💡 Índices podem falhar se as tabelas estiverem vazias ou se já existirem")

def aplicar_otimizacoes_postgresql(engine):
    """Aplica otimizações específicas na sessão atual"""
    try:
        otimizacoes_sessao = [
            "SET work_mem = '256MB';",
            "SET maintenance_work_mem = '1GB';",
            "SET synchronous_commit = OFF;",
            "SET wal_buffers = '16MB';",
            "SET checkpoint_completion_target = 0.9;",
            "SET effective_cache_size = '4GB';",
        ]

        print("🔧 Aplicando otimizações PostgreSQL para ETL...")
        with engine.connect() as conn:
            for opt in otimizacoes_sessao:
                try:
                    conn.execute(text(opt))
                    conn.commit()
                except Exception as e:
                    print(f"⚠️ Otimização falhou: {opt} - {e}")

        print("✅ Otimizações aplicadas!")
    except Exception as e:
        print(f"❌ Erro ao aplicar otimizações: {e}")
        raise

def load_all_ultra_optimized(groups):
    """Função principal otimizada, mantendo compatibilidade total"""
    print("🚀 Iniciando carregamento ultra-otimizado...")
    start_time = time.time()

    try:
        engine = get_engine_connection()

        # Limpa o banco uma única vez no início
        limpar_banco(engine)

        # Aplica otimizações no PostgreSQL
        aplicar_otimizacoes_postgresql(engine)

        # Mostra configurações calculadas
        memory_gb = psutil.virtual_memory().total / (1024**3)
        print(f"💻 Sistema: {memory_gb:.1f}GB RAM, Chunk size: {OPTIMIZED_CHUNKSIZE:,} linhas")

        # Ordem otimizada: tabelas de referência primeiro (menores e necessárias para FKs)
        print("\n📋 Carregando tabelas de referência...")
        reference_tables = [
            ("cnae", ["codigo", "descricao"]),
            ("moti", ["codigo", "descricao"]),
            ("munic", ["codigo", "descricao"]),
            ("natju", ["codigo", "descricao"]),
            ("pais", ["codigo", "descricao"]),
            ("quals", ["codigo", "descricao"])
        ]

        for table_name, columns in reference_tables:
            if groups.get(table_name):
                print(f"📊 Carregando {table_name}...")
                load_tabelas_auxiliares(groups[table_name], table_name, columns, engine)
            else:
                print(f"⚠️ Nenhum arquivo encontrado para {table_name}")

        # Tabelas principais por ordem de tamanho (menor para maior)
        print("\n📊 Carregando tabelas principais...")
        main_tables = [
            ("empresa", load_empresa),
            ("simples", load_simples),
            ("socios", load_socios),
            ("estabelecimento", load_estabelecimento)  # Maior tabela por último
        ]

        for table_name, load_func in main_tables:
            if groups.get(table_name):
                print(f"📊 Carregando {table_name}...")
                load_func(groups[table_name], engine)
            else:
                print(f"⚠️ Nenhum arquivo encontrado para {table_name}")

        # Cria índices após todos os dados carregados
        print("\n🔧 Criando índices...")
        criar_indices_ultra_otimizados(engine)

        # Atualiza estatísticas do banco
        print("📈 Atualizando estatísticas...")
        with engine.connect() as conn:
            conn.execute(text("ANALYZE;"))
            conn.commit()

        elapsed_time = time.time() - start_time
        print(f"\n🎉 Carregamento ultra-otimizado concluído em {elapsed_time:.2f}s ({elapsed_time/60:.1f} min)!")

        # Estatísticas finais
        print(f"\n📊 RESUMO DO CARREGAMENTO:")
        print("=" * 50)
        with engine.connect() as conn:
            total_records = 0
            for table in ['empresa', 'estabelecimento', 'socios', 'simples', 'cnae', 'moti', 'munic', 'natju', 'pais', 'quals']:
                try:
                    result = conn.execute(text(f"SELECT COUNT(*) FROM {table};")).fetchone()
                    if result and result[0] > 0:
                        count = result[0]
                        total_records += count
                        print(f"📋 {table:<15}: {count:>10,} registros")
                except Exception as e:
                    print(f"⚠️ {table:<15}: Erro ao contar - {e}")

            print("=" * 50)
            print(f"📊 TOTAL GERAL : {total_records:>10,} registros")

        return True
    except Exception as e:
        print(f"❌ Erro durante carregamento: {e}")
        import traceback
        print("🔍 Detalhes do erro:")
        traceback.print_exc()
        return False

# Mantém função original para compatibilidade
def load_all(groups):
    """Wrapper para manter compatibilidade com o código existente"""
    return load_all_ultra_optimized(groups)