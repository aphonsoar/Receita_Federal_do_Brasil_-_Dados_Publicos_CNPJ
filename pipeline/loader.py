import os
import pandas as pd
from sqlalchemy import text
from utils.db import get_engine
from config import Config

CHUNKSIZE = 100000  # número de linhas por lote (ajuste conforme sua RAM)

def _load_csv(file_path, table_name, columns_map, dtype=None, sep=";", encoding="latin1"):
    """Carrega um CSV em chunks e insere no banco."""
    engine = get_engine()
    first = True

    for chunk in pd.read_csv(
            file_path,
            sep=sep,
            dtype=dtype,
            encoding=encoding,
            header=None,
            chunksize=CHUNKSIZE
    ):
        # renomeia colunas
        chunk.rename(columns=columns_map, inplace=True)

        # insere no banco
        chunk.to_sql(
            table_name,
            engine,
            if_exists="replace" if first else "append",
            index=False
        )
        first = False

def load_empresa(files):
    col_map = {
        0: "cnpj_basico",
        1: "razao_social",
        2: "natureza_juridica",
        3: "qualificacao_responsavel",
        4: "capital_social",
        5: "porte_empresa",
        6: "ente_federativo_responsavel",
    }
    for f in files:
        print(f"Carregando EMPRESA: {f}")
        _load_csv(os.path.join(Config.EXTRACTED_FILES, f), "empresa", col_map)


def load_estabelecimento(files):
    col_map = {
        0: "cnpj_basico",
        1: "cnpj_ordem",
        2: "cnpj_dv",
        3: "matriz_filial",
        4: "nome_fantasia",
        5: "situacao_cadastral",
        6: "data_situacao_cadastral",
        7: "motivo_situacao_cadastral",
        8: "nome_cidade_exterior",
        9: "pais",
        10: "data_inicio_atividade",
        11: "cnae_principal",
        12: "cnae_secundaria",
        13: "tipo_logradouro",
        14: "logradouro",
        15: "numero",
        16: "complemento",
        17: "bairro",
        18: "cep",
        19: "uf",
        20: "municipio",
        21: "ddd1",
        22: "telefone1",
        23: "ddd2",
        24: "telefone2",
        25: "ddd_fax",
        26: "fax",
        27: "correio_eletronico",
        28: "situacao_especial",
        29: "data_situacao_especial",
    }
    for f in files:
        print(f"Carregando ESTABELECIMENTO: {f}")
        _load_csv(os.path.join(Config.EXTRACTED_FILES, f), "estabelecimento", col_map)


def load_socios(files):
    col_map = {
        0: "cnpj_basico",
        1: "identificador_socio",
        2: "nome_razao_social",
        3: "cnpj_cpf_socio",
        4: "qualificacao_socio",
        5: "data_entrada_sociedade",
        6: "pais",
        7: "representante_legal",
        8: "nome_representante",
        9: "qualificacao_representante",
        10: "faixa_etaria",
    }
    for f in files:
        print(f"Carregando SOCIOS: {f}")
        _load_csv(os.path.join(Config.EXTRACTED_FILES, f), "socios", col_map)


def load_simples(files):
    col_map = {
        0: "cnpj_basico",
        1: "opcao_simples",
        2: "data_opcao_simples",
        3: "data_exclusao_simples",
        4: "opcao_mei",
        5: "data_opcao_mei",
        6: "data_exclusao_mei",
    }
    for f in files:
        print(f"Carregando SIMPLES: {f}")
        _load_csv(os.path.join(Config.EXTRACTED_FILES, f), "simples", col_map)


def load_tabelas_auxiliares(files, table_name, columns):
    """Carrega tabelas menores (CNAE, Municípios, etc.)"""
    col_map = {i: col for i, col in enumerate(columns)}
    for f in files:
        print(f"Carregando {table_name.upper()}: {f}")
        _load_csv(os.path.join(Config.EXTRACTED_FILES, f), table_name, col_map)


def criar_indices():
    """Cria índices para as colunas principais"""
    engine = get_engine()
    print("Criando índices no banco de dados...")
    queries = [
        "CREATE INDEX IF NOT EXISTS idx_empresa_cnpj ON empresa(cnpj_basico);",
        "CREATE INDEX IF NOT EXISTS idx_estabelecimento_cnpj ON estabelecimento(cnpj_basico);",
        "CREATE INDEX IF NOT EXISTS idx_socios_cnpj ON socios(cnpj_basico);",
        "CREATE INDEX IF NOT EXISTS idx_simples_cnpj ON simples(cnpj_basico);"
    ]
    with engine.connect() as conn:
        for q in queries:
            conn.execute(text(q))
    print("Índices criados com sucesso!")


def load_all(groups):
    load_empresa(groups["empresa"])
    load_estabelecimento(groups["estabelecimento"])
    load_socios(groups["socios"])
    load_simples(groups["simples"])

    load_tabelas_auxiliares(groups["cnae"], "cnae", ["codigo", "descricao"])
    load_tabelas_auxiliares(groups["moti"], "motivo", ["codigo", "descricao"])
    load_tabelas_auxiliares(groups["munic"], "municipio", ["codigo", "descricao"])
    load_tabelas_auxiliares(groups["natju"], "natureza_juridica", ["codigo", "descricao"])
    load_tabelas_auxiliares(groups["pais"], "pais", ["codigo", "descricao"])
    load_tabelas_auxiliares(groups["quals"], "qualificacao", ["codigo", "descricao"])

    criar_indices()