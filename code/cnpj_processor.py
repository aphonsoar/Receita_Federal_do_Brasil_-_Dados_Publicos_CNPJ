import datetime
import gc
import io
import pathlib
from dotenv import load_dotenv
import bs4 as bs
import os
import pandas as pd
import pyodbc
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
import re
import sys
import time
import requests
import urllib.request
import urllib.parse
import wget
import zipfile

# =============================================================================
# FUNÇÕES DE CONFIGURAÇÃO E AMBIENTE
# =============================================================================

def load_environment_variables():
    """
    Carrega as variáveis de ambiente do arquivo .env e retorna os caminhos e a URL.
    """
    script_dir = pathlib.Path(__file__).parent.resolve()
    dotenv_path = os.path.join(script_dir, '.env')

    if not os.path.isfile(dotenv_path):
        print(f"ERRO: Arquivo de configuração '.env' não encontrado em '{script_dir}'.")
        print("Por favor, copie o arquivo '.env_template' para '.env' e preencha suas configurações.")
        sys.exit(1)

    print(f"Carregando configurações de: {dotenv_path}")
    load_dotenv(dotenv_path=dotenv_path)

    config = {
        "data_url": os.getenv('DADOS_RF_URL'),
        "output_path": os.getenv('OUTPUT_FILES_PATH'),
        "extracted_path": os.getenv('EXTRACTED_FILES_PATH'),
        "db_driver": os.getenv('DB_DRIVER'),
        "db_server": os.getenv('DB_SERVER'),
        "db_name": os.getenv('DB_NAME'),
        "db_user": os.getenv('DB_USER'),
        "db_password": os.getenv('DB_PASSWORD')
    }

    if not all(config.values()):
        print("ERRO: Uma ou mais variáveis de ambiente não foram definidas no arquivo .env.")
        sys.exit(1)

    makedirs(config["output_path"])
    makedirs(config["extracted_path"])

    print('Diretórios definidos:')
    print(f'  - Saída de arquivos ZIP: {config["output_path"]}')
    print(f'  - Extração de arquivos CSV: {config["extracted_path"]}')
    print(f'URL dos dados: {config["data_url"]}')

    return config

def makedirs(path):
    """Cria um diretório se ele não existir."""
    if not os.path.exists(path):
        os.makedirs(path)

# =============================================================================
# FUNÇÕES DE DOWNLOAD E EXTRAÇÃO
# =============================================================================

def download_data_files(data_url, output_path):
    """
    Baixa todos os arquivos .zip do diretório de dados da Receita Federal.
    """
    print("\n--- INICIANDO ETAPA DE DOWNLOAD ---")

    try:
        files_to_download = get_zip_files_from_url(data_url)
    except (urllib.error.URLError, SystemExit):
        print("Não foi possível encontrar arquivos .zip na URL base, tentando encontrar subdiretório mais recente...")
        try:
            latest_data_url = get_latest_data_url(data_url)
            files_to_download = get_zip_files_from_url(latest_data_url)
            data_url = latest_data_url
        except (urllib.error.URLError, SystemExit):
            sys.exit(1) # Erro já foi logado pelas funções filhas

    print('Arquivos que serão baixados:')
    for i, f in enumerate(files_to_download, 1):
        print(f'{i} - {f}')

    for file_name in files_to_download:
        url = urllib.parse.urljoin(data_url, file_name)
        local_file_path = os.path.join(output_path, file_name)

        print(f'\nBaixando arquivo: {file_name}')
        if not os.path.isfile(local_file_path):
            wget.download(url, out=output_path, bar=bar_progress)
        else:
            print("Arquivo já existe localmente. Pulando download.")

def extract_zip_files(output_path, extracted_path):
    """
    Extrai todos os arquivos .zip da pasta de output para a pasta de extração.
    """
    print("\n--- INICIANDO ETAPA DE EXTRAÇÃO ---")
    zip_files = [f for f in os.listdir(output_path) if f.endswith('.zip')]

    for i, file_name in enumerate(zip_files, 1):
        print(f'Descompactando arquivo: {i}/{len(zip_files)} - {file_name}')
        full_path = os.path.join(output_path, file_name)
        try:
            with zipfile.ZipFile(full_path, 'r') as zip_ref:
                zip_ref.extractall(extracted_path)
        except zipfile.BadZipFile:
            print(f"AVISO: O arquivo {file_name} não é um ZIP válido ou está corrompido. Ignorando.")
        except Exception as e:
            print(f"AVISO: Erro inesperado ao descompactar {file_name}: {e}. Ignorando.")

def get_latest_data_url(base_url):
    """Encontra o diretório de dados mais recente na URL base."""
    print(f"Buscando diretórios em: {base_url}")
    response = urlopen_with_retry(base_url)
    page = bs.BeautifulSoup(response.read(), 'lxml')
    date_pattern = re.compile(r'^\d{4}-\d{2}/$')
    dir_links = [a['href'] for a in page.find_all('a') if date_pattern.match(a['href'])]

    if not dir_links:
        print("ERRO: Nenhum diretório de dados (AAAA-MM/) encontrado na URL.")
        sys.exit(1)

    latest_dir = sorted(dir_links)[-1]
    latest_data_url = urllib.parse.urljoin(base_url, latest_dir)
    print(f"Diretório de dados mais recente: {latest_data_url}")
    return latest_data_url

def get_zip_files_from_url(data_url):
    """Lista todos os arquivos .zip de uma URL."""
    print(f"Buscando arquivos .zip em: {data_url}")
    response = urlopen_with_retry(data_url)
    page = bs.BeautifulSoup(response.read(), 'lxml')
    zip_files = [a['href'] for a in page.find_all('a') if a['href'].endswith('.zip')]

    if not zip_files:
        print("ERRO: Nenhum arquivo .zip encontrado na URL de dados.")
        sys.exit(1)
    return zip_files

def urlopen_with_retry(url, max_retries=3, delay_seconds=10):
    """Tenta abrir uma URL com retentativas em caso de falha."""
    for attempt in range(max_retries):
        try:
            return urllib.request.urlopen(url, timeout=60)
        except urllib.error.URLError as e:
            print(f"AVISO: Falha ao acessar {url}. Erro: {e}")
            if attempt < max_retries - 1:
                print(f"Aguardando {delay_seconds}s para nova tentativa...")
                time.sleep(delay_seconds)
            else:
                print(f"ERRO: Todas as tentativas de conexão com {url} falharam.")
                raise

def bar_progress(current, total, width=80):
    """Barra de progresso para o wget."""
    progress_message = f"Baixando: {current / total * 100:.1f}% [{current} / {total}] bytes"
    sys.stdout.write("\r" + progress_message)
    sys.stdout.flush()

# =============================================================================
# FUNÇÕES DE BANCO DE DADOS
# =============================================================================

def get_db_engine(config):
    """
    Cria e retorna um engine do SQLAlchemy para o SQL Server.
    """
    connection_url = URL.create(
        "mssql+pyodbc",
        username=config["db_user"],
        password=config["db_password"],
        host=config["db_server"],
        database=config["db_name"],
        query={"driver": config["db_driver"]},
    )

    try:
        engine = create_engine(connection_url)
        # Testa a conexão
        with engine.connect() as connection:
            print("\nConexão com o SQL Server (via SQLAlchemy) bem-sucedida!")
        return engine
    except Exception as e:
        print(f"ERRO: Falha ao criar engine de conexão com o SQL Server. Erro: {e}")
        sys.exit(1)

def setup_database_tables(engine):
    """
    Cria ou recria todas as tabelas necessárias no banco de dados usando o engine do SQLAlchemy.
    """
    print("\n--- CONFIGURANDO TABELAS NO BANCO DE DADOS ---")

    ddl_commands = {
        "empresa": """CREATE TABLE empresa (
            cnpj_basico VARCHAR(8), razao_social VARCHAR(MAX), natureza_juridica VARCHAR(4),
            qualificacao_responsavel VARCHAR(2), capital_social VARCHAR(50), porte_empresa VARCHAR(2),
            ente_federativo_responsavel VARCHAR(MAX)
        );""",
        "estabelecimento": """CREATE TABLE estabelecimento (
            cnpj_basico VARCHAR(8), cnpj_ordem VARCHAR(4), cnpj_dv VARCHAR(2),
            identificador_matriz_filial VARCHAR(1), nome_fantasia VARCHAR(MAX), situacao_cadastral VARCHAR(2),
            data_situacao_cadastral VARCHAR(10), motivo_situacao_cadastral VARCHAR(2), nome_cidade_exterior VARCHAR(MAX),
            pais VARCHAR(3), data_inicio_atividade VARCHAR(10), cnae_fiscal_principal VARCHAR(7),
            cnae_fiscal_secundaria VARCHAR(MAX), tipo_logradouro VARCHAR(MAX), logradouro VARCHAR(MAX),
            numero VARCHAR(MAX), complemento VARCHAR(MAX), bairro VARCHAR(MAX), cep VARCHAR(8),
            uf VARCHAR(2), municipio VARCHAR(4), ddd_1 VARCHAR(4), telefone_1 VARCHAR(9),
            ddd_2 VARCHAR(4), telefone_2 VARCHAR(9), ddd_fax VARCHAR(4), fax VARCHAR(9),
            correio_eletronico VARCHAR(MAX), situacao_especial VARCHAR(MAX), data_situacao_especial VARCHAR(10)
        );""",
        "socios": """CREATE TABLE socios (
            cnpj_basico VARCHAR(8), identificador_socio VARCHAR(1), nome_socio_razao_social VARCHAR(MAX),
            cpf_cnpj_socio VARCHAR(14), qualificacao_socio VARCHAR(2), data_entrada_sociedade VARCHAR(10),
            pais VARCHAR(3), representante_legal VARCHAR(14), nome_do_representante VARCHAR(MAX),
            qualificacao_representante_legal VARCHAR(2), faixa_etaria VARCHAR(1)
        );""",
        "simples": """CREATE TABLE simples (
            cnpj_basico VARCHAR(8), opcao_pelo_simples VARCHAR(1), data_opcao_simples VARCHAR(10),
            data_exclusao_simples VARCHAR(10), opcao_mei VARCHAR(1), data_opcao_mei VARCHAR(10),
            data_exclusao_mei VARCHAR(10)
        );""",
        "cnae": "CREATE TABLE cnae (codigo VARCHAR(7), descricao VARCHAR(MAX));",
        "moti": "CREATE TABLE moti (codigo VARCHAR(2), descricao VARCHAR(MAX));",
        "munic": "CREATE TABLE munic (codigo VARCHAR(4), descricao VARCHAR(MAX));",
        "natju": "CREATE TABLE natju (codigo VARCHAR(4), descricao VARCHAR(MAX));",
        "pais": "CREATE TABLE pais (codigo VARCHAR(3), descricao VARCHAR(MAX));",
        "quals": "CREATE TABLE quals (codigo VARCHAR(2), descricao VARCHAR(MAX));"
    }

    with engine.connect() as connection:
        for table_name, ddl in ddl_commands.items():
            print(f"  - Recriando tabela '{table_name}'...")
            connection.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name};")
            connection.execute(ddl)
        connection.commit()
    print("Tabelas configuradas com sucesso.")

def create_database_indexes(engine):
    """Cria índices nas tabelas para otimizar as consultas."""
    print("\n--- CRIANDO ÍNDICES NO BANCO DE DADOS ---")
    with engine.connect() as connection:
        try:
            connection.execute("CREATE INDEX idx_empresa_cnpj ON empresa(cnpj_basico);")
            connection.execute("CREATE INDEX idx_estabelecimento_cnpj ON estabelecimento(cnpj_basico);")
            connection.execute("CREATE INDEX idx_socios_cnpj ON socios(cnpj_basico);")
            connection.execute("CREATE INDEX idx_simples_cnpj ON simples(cnpj_basico);")
            connection.commit()
            print("Índices criados com sucesso para a coluna `cnpj_basico`.")
        except Exception as e:
            print(f"AVISO: Não foi possível criar os índices. Eles podem já existir. Erro: {e}")

# =============================================================================
# FUNÇÕES DE PROCESSAMENTO E CARGA DE DADOS
# =============================================================================

def process_and_load_data(engine, extracted_path):
    """
    Orquestra o processo de limpeza e carga de todos os arquivos CSV no banco de dados.
    """
    print("\n--- INICIANDO ETAPA DE PROCESSAMENTO E CARGA DE DADOS ---")

    file_mappings = classify_files(extracted_path)
    schemas = get_table_schemas()

    for table_name, files in file_mappings.items():
        if files:
            process_table_files(engine, table_name, files, schemas[table_name], extracted_path)

def process_table_files(engine, table_name, files, schema, extracted_path):
    """Processa e carrega todos os arquivos de um tipo específico de tabela."""
    insert_start = time.time()
    print(f"\nProcessando tabela: {table_name.upper()}")

    total_chunks = 0
    for file_name in files:
        print(f'  Trabalhando no arquivo: {file_name}...')
        file_path = os.path.join(extracted_path, file_name)

        sanitized_buffer = sanitize_file(file_path, len(schema['cols']))
        if sanitized_buffer.tell() == 0:
            print("    - Arquivo vazio após higienização. Pulando.")
            continue

        sanitized_buffer.seek(0)

        reader = pd.read_csv(
            sanitized_buffer, sep=';', header=None, names=schema['cols'],
            dtype=schema['dtype'], encoding='latin-1', quotechar='"',
            chunksize=500_000, on_bad_lines='warn'
        )

        for i, chunk in enumerate(reader):
            bulk_insert_to_sql(engine, chunk, table_name)
            total_chunks += 1
            print(f'\r    Chunk {i+1} do arquivo {file_name} inserido com sucesso!', end='')

        print(f'\n  Arquivo {file_name} finalizado.')
        gc.collect()

    tempo_insert = round(time.time() - insert_start)
    print(f"Tabela {table_name.upper()} finalizada! {total_chunks} chunks inseridos em {tempo_insert}s.")

def sanitize_file(filepath, num_expected_columns):
    """Lê e corrige um arquivo CSV malformado, retornando um buffer em memória."""
    print(f"    - Higienizando o arquivo...")
    clean_buffer = io.StringIO()
    with open(filepath, 'r', encoding='latin-1') as f:
        for line in f:
            cleaned_line = line.replace('\n', '').replace('\r', '').replace('"', '')
            parts = cleaned_line.split(';')

            if len(parts) > num_expected_columns:
                reconstructed_field = " ".join(parts[1:-(num_expected_columns - 2)])
                corrected_parts = [parts[0]] + [reconstructed_field] + parts[-(num_expected_columns - 2):]
                final_line = ";".join(corrected_parts)
            elif len(parts) < num_expected_columns:
                corrected_parts = parts + [''] * (num_expected_columns - len(parts))
                final_line = ";".join(corrected_parts)
            else:
                final_line = ";".join(parts)

            clean_buffer.write(final_line + '\n')

    return clean_buffer

def bulk_insert_to_sql(engine, df, table_name):
    """Insere um DataFrame em uma tabela do SQL Server usando to_sql e um engine SQLAlchemy."""
    try:
        df.to_sql(table_name, con=engine, if_exists='append', index=False, chunksize=10000, method='multi')
    except Exception as error:
        print(f"\nERRO ao inserir dados na tabela {table_name}: {error}")

def classify_files(extracted_path):
    """Classifica os arquivos extraídos em categorias de tabelas."""
    all_files = [name for name in os.listdir(extracted_path) if os.path.isfile(os.path.join(extracted_path, name))]

    file_mappings = {
        'empresa': [f for f in all_files if 'EMPRECSV' in f.upper()],
        'estabelecimento': [f for f in all_files if 'ESTABELE' in f.upper()],
        'socios': [f for f in all_files if 'SOCIOCSV' in f.upper()],
        'simples': [f for f in all_files if 'SIMPLES.CSV' in f.upper()],
        'cnae': [f for f in all_files if 'CNAECSV' in f.upper()],
        'moti': [f for f in all_files if 'MOTICSV' in f.upper()],
        'munic': [f for f in all_files if 'MUNICCSV' in f.upper()],
        'natju': [f for f in all_files if 'NATJUCSV' in f.upper()],
        'pais': [f for f in all_files if 'PAISCSV' in f.upper()],
        'quals': [f for f in all_files if 'QUALSCSV' in f.upper()]
    }

    # Reportar arquivos não classificados
    classified_files = set(sum(file_mappings.values(), []))
    unclassified_files = set(all_files) - classified_files
    if unclassified_files:
        print("\nAVISO: Os seguintes arquivos não foram classificados e serão ignorados:")
        for f in unclassified_files:
            print(f"  - {f}")

    return file_mappings

def get_table_schemas():
    """Retorna um dicionário com os schemas (colunas e dtypes) para cada tabela."""
    schemas = {
        'empresa': {'cols': ['cnpj_basico', 'razao_social', 'natureza_juridica', 'qualificacao_responsavel', 'capital_social', 'porte_empresa', 'ente_federativo_responsavel']},
        'estabelecimento': {'cols': ['cnpj_basico', 'cnpj_ordem', 'cnpj_dv', 'identificador_matriz_filial', 'nome_fantasia', 'situacao_cadastral', 'data_situacao_cadastral', 'motivo_situacao_cadastral', 'nome_cidade_exterior', 'pais', 'data_inicio_atividade', 'cnae_fiscal_principal', 'cnae_fiscal_secundaria', 'tipo_logradouro', 'logradouro', 'numero', 'complemento', 'bairro', 'cep', 'uf', 'municipio', 'ddd_1', 'telefone_1', 'ddd_2', 'telefone_2', 'ddd_fax', 'fax', 'correio_eletronico', 'situacao_especial', 'data_situacao_especial']},
        'socios': {'cols': ['cnpj_basico', 'identificador_socio', 'nome_socio_razao_social', 'cpf_cnpj_socio', 'qualificacao_socio', 'data_entrada_sociedade', 'pais', 'representante_legal', 'nome_do_representante', 'qualificacao_representante_legal', 'faixa_etaria']},
        'simples': {'cols': ['cnpj_basico', 'opcao_pelo_simples', 'data_opcao_simples', 'data_exclusao_simples', 'opcao_mei', 'data_opcao_mei', 'data_exclusao_mei']},
        'cnae': {'cols': ['codigo', 'descricao']},
        'moti': {'cols': ['codigo', 'descricao']},
        'munic': {'cols': ['codigo', 'descricao']},
        'natju': {'cols': ['codigo', 'descricao']},
        'pais': {'cols': ['codigo', 'descricao']},
        'quals': {'cols': ['codigo', 'descricao']}
    }
    # Adicionar dtypes como string para todas as colunas
    for table in schemas:
        schemas[table]['dtype'] = {col: str for col in schemas[table]['cols']}
    return schemas

# =============================================================================
# FUNÇÃO PRINCIPAL
# =============================================================================

def main():
    """
    Função principal que orquestra todo o processo de ETL.
    """
    start_time = time.time()

    # 1. Carregar Configurações
    config = load_environment_variables()

    # 2. Download e Extração
    download_data_files(config['data_url'], config['output_path'])
    extract_zip_files(config['output_path'], config['extracted_path'])

    # 3. Conexão e Configuração do Banco de Dados
    engine = get_db_engine(config)
    setup_database_tables(engine)

    # 4. Processamento e Carga dos Dados
    process_and_load_data(engine, config['extracted_path'])

    # 5. Otimização do Banco (Índices)
    create_database_indexes(engine)

    # Fechar a conexão
    engine.dispose()

    total_time = round(time.time() - start_time)
    print(f"\nProcesso 100% finalizado em {total_time} segundos!")
    print("Você já pode usar seus dados no SQL Server.")
    print("Contribua com esse projeto em: https://github.com/aphonsoar/Receita_Federal_do_Brasil_-_Dados_Publicos_CNPJ")

if __name__ == '__main__':
    main()
