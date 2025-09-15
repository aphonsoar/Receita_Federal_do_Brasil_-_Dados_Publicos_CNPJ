import os
import pandas as pd
import numpy as np
from sqlalchemy import text
from pathlib import Path
import psutil
import gc
import time
from typing import Dict, List
from tqdm import tqdm
import threading
from concurrent.futures import ThreadPoolExecutor

from utils.db import get_engine
from config import Config

# Configurações otimizadas baseadas no hardware
def get_optimal_chunk_size():
    """Calcula chunk size otimizado baseado na RAM disponível"""
    memory_gb = psutil.virtual_memory().total / (1024**3)
    # Para 16GB RAM: chunk maior, para menos RAM: chunk menor
    if memory_gb >= 16:
        return 500000  # 500k linhas para máquinas com bastante RAM
    elif memory_gb >= 8:
        return 250000  # 250k linhas para máquinas médias
    else:
        return 100000  # 100k linhas para máquinas com pouca RAM

# Chunk size dinâmico
OPTIMIZED_CHUNKSIZE = get_optimal_chunk_size()

class OptimizedDataFrameProcessor:
    """Classe para otimizar DataFrames antes da inserção"""

    @staticmethod
    def optimize_dtypes(df: pd.DataFrame, table_name: str) -> pd.DataFrame:
        """Otimiza tipos de dados para economizar memória"""

        # Otimizações específicas por tabela (as mais importantes)
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
                'faixa_etaria': 'category',
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
        # Remove linhas completamente vazias
        df.dropna(how='all', inplace=True)
        return df

def _load_csv_ultra_optimized(file_path, table_name, columns_map, dtype=None, sep=";", encoding="latin1"):
    """Versão ultra-otimizada da função de carregamento, mantendo compatibilidade"""

    print(f"📊 Processando {Path(file_path).name} → {table_name}")

    engine = get_engine()
    processor = OptimizedDataFrameProcessor()

    # Configurações otimizadas para pandas
    pd_read_config = {
        'sep': sep,
        'dtype': dtype or {},
        'encoding': encoding,
        'header': None,
        'chunksize': OPTIMIZED_CHUNKSIZE,
        'low_memory': False,
        'na_values': ['', 'NULL', 'null', 'None', 'nan'],
        'keep_default_na': True,
        'engine': 'c',  # Parser C é mais rápido
        'memory_map': True,  # Melhora performance para arquivos grandes
    }

    first_chunk = True
    total_rows = 0
    chunk_count = 0

    try:
        # Progress bar estimado baseado no tamanho do arquivo
        file_size_mb = Path(file_path).stat().st_size / (1024**2)
        estimated_chunks = max(1, int(file_size_mb / 50))  # Estima ~50MB por chunk

        with tqdm(desc=f"Carregando {table_name}", unit="chunk") as pbar:
            for chunk in pd.read_csv(file_path, **pd_read_config):
                # Renomeia colunas
                chunk.rename(columns=columns_map, inplace=True)

                # Otimiza tipos de dados
                chunk = processor.optimize_dtypes(chunk, table_name)

                # Limpeza básica
                chunk = processor.clean_dataframe(chunk)

                if len(chunk) == 0:
                    continue

                # Inserção otimizada no banco
                chunk.to_sql(
                    table_name,
                    engine,
                    if_exists="replace" if first_chunk else "append",
                    index=False,
                    method='multi',  # Inserção em lote mais eficiente
                    chunksize=20000  # Lotes ótimos para PostgreSQL
                )

                total_rows += len(chunk)
                chunk_count += 1
                first_chunk = False

                # Atualiza progress bar
                pbar.update(1)
                pbar.set_postfix({
                    'linhas': f"{total_rows:,}",
                    'chunk': chunk_count
                })

                # Libera memória agressivamente
                del chunk
                if chunk_count % 10 == 0:  # A cada 10 chunks
                    gc.collect()

    except Exception as e:
        print(f"❌ Erro ao processar {file_path}: {e}")
        raise

    print(f"✅ {table_name}: {total_rows:,} linhas inseridas ({chunk_count} chunks)")
    return total_rows

# Funções originais otimizadas (mantém compatibilidade total)

def load_empresa(files):
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

    dtype_hints = {
        0: 'string', 1: 'string', 2: 'string',
        3: 'string', 4: 'string', 5: 'string', 6: 'string'
    }

    for f in files:
        print(f"Carregando EMPRESA: {f}")
        file_path = os.path.join(Config.EXTRACTED_FILES, f)
        _load_csv_ultra_optimized(file_path, "empresa", col_map, dtype_hints)

def load_estabelecimento(files):
    """Versão otimizada da função original"""
    col_map = {
        0: "cnpj_basico", 1: "cnpj_ordem", 2: "cnpj_dv", 3: "identificador_matriz_filial",
        4: "nome_fantasia", 5: "situacao_cadastral", 6: "data_situacao_cadastral",
        7: "motivo_situacao_cadastral", 8: "nome_cidade_exterior", 9: "pais",
        10: "data_inicio_atividade", 11: "cnae_fiscal_principal", 12: "cnae_fiscal_secundaria",
        13: "tipo_logradouro", 14: "logradouro", 15: "numero", 16: "complemento",
        17: "bairro", 18: "cep", 19: "uf", 20: "municipio", 21: "ddd_1",
        22: "telefone_1", 23: "ddd_2", 24: "telefone_2", 25: "ddd_fax",
        26: "fax", 27: "correio_eletronico", 28: "situacao_especial", 29: "data_situacao_especial",
    }

    # Para estabelecimento, força string em tudo para evitar problemas
    dtype_hints = {i: 'string' for i in range(30)}

    for f in files:
        print(f"Carregando ESTABELECIMENTO: {f}")
        file_path = os.path.join(Config.EXTRACTED_FILES, f)
        _load_csv_ultra_optimized(file_path, "estabelecimento", col_map, dtype_hints)

def load_socios(files):
    """Versão otimizada da função original"""
    col_map = {
        0: "cnpj_basico", 1: "identificador_socio", 2: "nome_socio_razao_social",
        3: "cpf_cnpj_socio", 4: "qualificacao_socio", 5: "data_entrada_sociedade",
        6: "pais", 7: "representante_legal", 8: "nome_do_representante",
        9: "qualificacao_representante_legal", 10: "faixa_etaria",
    }

    dtype_hints = {i: 'string' for i in range(11)}

    for f in files:
        print(f"Carregando SOCIOS: {f}")
        file_path = os.path.join(Config.EXTRACTED_FILES, f)
        _load_csv_ultra_optimized(file_path, "socios", col_map, dtype_hints)

def load_simples(files):
    """Versão otimizada da função original"""
    col_map = {
        0: "cnpj_basico", 1: "opcao_pelo_simples", 2: "data_opcao_simples",
        3: "data_exclusao_simples", 4: "opcao_mei", 5: "data_opcao_mei", 6: "data_exclusao_mei",
    }

    dtype_hints = {i: 'string' for i in range(7)}

    for f in files:
        print(f"Carregando SIMPLES: {f}")
        file_path = os.path.join(Config.EXTRACTED_FILES, f)
        _load_csv_ultra_optimized(file_path, "simples", col_map, dtype_hints)

def load_tabelas_auxiliares(files, table_name, columns):
    """Versão otimizada das tabelas auxiliares"""
    col_map = {i: col for i, col in enumerate(columns)}
    dtype_hints = {i: 'string' for i in range(len(columns))}

    for f in files:
        print(f"Carregando {table_name.upper()}: {f}")
        file_path = os.path.join(Config.EXTRACTED_FILES, f)
        _load_csv_ultra_optimized(file_path, table_name, col_map, dtype_hints)

def criar_indices_ultra_otimizados():
    """Cria índices ultra-otimizados para consultas de alta performance"""
    engine = get_engine()
    print("🔧 Criando índices ultra-otimizados...")

    # Índices estratégicos baseados nos patterns de uso mais comuns
    indices_estrategicos = [
        # Índices hash para joins por CNPJ (mais rápidos que B-tree para =)
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_empresa_cnpj_hash ON empresa USING hash(cnpj_basico);",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_estabelecimento_cnpj_hash ON estabelecimento USING hash(cnpj_basico);",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_socios_cnpj_hash ON socios USING hash(cnpj_basico);",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_simples_cnpj_hash ON simples USING hash(cnpj_basico);",

        # Índices compostos para consultas geográficas/setoriais
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_estabelecimento_uf_situacao ON estabelecimento(uf, situacao_cadastral);",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_estabelecimento_cnae_uf ON estabelecimento(cnae_fiscal_principal, uf);",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_empresa_porte_natureza ON empresa(porte_empresa, natureza_juridica);",

        # Índices para busca textual (GIN é mais eficiente para LIKE/ILIKE)
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_empresa_razao_gin ON empresa USING gin(to_tsvector('portuguese', coalesce(razao_social, '')));",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_estabelecimento_fantasia_gin ON estabelecimento USING gin(to_tsvector('portuguese', coalesce(nome_fantasia, '')));"
    ]

    with engine.connect() as conn:
        for i, idx_sql in enumerate(tqdm(indices_estrategicos, desc="Criando índices")):
            try:
                conn.execute(text(idx_sql))
                conn.commit()
            except Exception as e:
                print(f"⚠️  Índice {i+1} falhou: {e}")

    print("✅ Índices ultra-otimizados criados!")

def aplicar_otimizacoes_postgresql():
    """Aplica otimizações específicas na sessão atual"""
    engine = get_engine()

    otimizacoes_sessao = [
        "SET work_mem = '256MB';",  # Mais memória para sorts/joins
        "SET maintenance_work_mem = '1GB';",  # Para criação de índices
        "SET synchronous_commit = OFF;",  # Melhor performance para ETL
        "SET wal_buffers = '16MB';",  # Buffer maior para WAL
        "SET checkpoint_completion_target = 0.9;",  # Distribui I/O
        "SET effective_cache_size = '4GB';",  # Assume cache disponível
    ]

    print("🔧 Aplicando otimizações PostgreSQL para ETL...")

    with engine.connect() as conn:
        for opt in otimizacoes_sessao:
            try:
                conn.execute(text(opt))
            except Exception as e:
                print(f"⚠️  Otimização falhou: {opt} - {e}")

    print("✅ Otimizações aplicadas!")

def load_all_ultra_optimized(groups):
    """Função principal otimizada, mantendo compatibilidade total"""

    print("🚀 Iniciando carregamento ultra-otimizado...")
    start_time = time.time()

    # Aplica otimizações no PostgreSQL
    aplicar_otimizacoes_postgresql()

    # Mostra configurações calculadas
    memory_gb = psutil.virtual_memory().total / (1024**3)
    print(f"💻 Sistema: {memory_gb:.1f}GB RAM, Chunk size: {OPTIMIZED_CHUNKSIZE:,} linhas")

    # Ordem otimizada: tabelas de referência primeiro (menores e necessárias para FKs)
    print("\n📋 Carregando tabelas de referência...")
    load_tabelas_auxiliares(groups.get("cnae", []), "cnae", ["codigo", "descricao"])
    load_tabelas_auxiliares(groups.get("moti", []), "moti", ["codigo", "descricao"])
    load_tabelas_auxiliares(groups.get("munic", []), "munic", ["codigo", "descricao"])
    load_tabelas_auxiliares(groups.get("natju", []), "natju", ["codigo", "descricao"])
    load_tabelas_auxiliares(groups.get("pais", []), "pais", ["codigo", "descricao"])
    load_tabelas_auxiliares(groups.get("quals", []), "quals", ["codigo", "descricao"])

    # Tabelas principais por ordem de tamanho (menor para maior)
    print("\n📊 Carregando tabelas principais...")

    if groups.get("empresa"):
        load_empresa(groups["empresa"])

    if groups.get("simples"):
        load_simples(groups["simples"])

    if groups.get("socios"):
        load_socios(groups["socios"])

    # Estabelecimento por último (maior tabela)
    if groups.get("estabelecimento"):
        load_estabelecimento(groups["estabelecimento"])

    # Cria índices após todos os dados carregados
    print("\n🔧 Criando índices...")
    criar_indices_ultra_otimizados()

    # Atualiza estatísticas do banco
    print("📈 Atualizando estatísticas...")
    engine = get_engine()
    with engine.connect() as conn:
        conn.execute(text("ANALYZE;"))
        conn.commit()

    elapsed_time = time.time() - start_time
    print(f"\n🎉 Carregamento ultra-otimizado concluído em {elapsed_time:.2f}s ({elapsed_time/60:.1f} min)!")

    # Estatísticas finais
    with engine.connect() as conn:
        for table in ['empresa', 'estabelecimento', 'socios', 'simples']:
            try:
                result = conn.execute(text(f"SELECT COUNT(*) FROM {table};")).fetchone()
                if result:
                    print(f"📊 {table}: {result[0]:,} registros")
            except:
                pass

# Mantém função original para compatibilidade
def load_all(groups):
    """Wrapper para manter compatibilidade com o código existente"""
    return load_all_ultra_optimized(groups)