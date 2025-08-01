import datetime
import gc
import io
import logging
import pathlib
from dotenv import load_dotenv
import bs4 as bs
import os
import pandas as pd
import pyodbc
from sqlalchemy import create_engine, text
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

def setup_logging():
    """Configura o logging para o projeto."""
    log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    # Logger raiz
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    # Handler para o console
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_formatter)
    root_logger.addHandler(console_handler)

    # Handler para o arquivo
    file_handler = logging.FileHandler('etl.log', mode='w')
    file_handler.setFormatter(log_formatter)
    root_logger.addHandler(file_handler)

def load_environment_variables():
    """
    Carrega as variáveis de ambiente do arquivo .env e retorna os caminhos e a URL.
    """
    script_dir = pathlib.Path(__file__).parent.resolve()
    dotenv_path = os.path.join(script_dir, '.env')

    if not os.path.isfile(dotenv_path):
        logging.error(f"Arquivo de configuração '.env' não encontrado em '{script_dir}'.")
        logging.error("Por favor, copie o arquivo '.env_template' para '.env' e preencha suas configurações.")
        sys.exit(1)

    logging.info(f"Carregando configurações de: {dotenv_path}")
    load_dotenv(dotenv_path=dotenv_path)

    config = {
        "data_url": os.getenv('DADOS_RF_URL'),
        "output_path": os.getenv('OUTPUT_FILES_PATH'),
        "extracted_path": os.getenv('EXTRACTED_FILES_PATH'),
        "db_driver": os.getenv('DB_DRIVER'),
        "db_server": os.getenv('DB_SERVER'),
        "db_user": os.getenv('DB_USER'),
        "db_password": os.getenv('DB_PASSWORD')
    }
    db_name = os.getenv('DB_NAME')

    if not all(config.values()):
        logging.error("Uma ou mais variáveis de ambiente não foram definidas no arquivo .env.")
        sys.exit(1)

    makedirs(config["output_path"])
    makedirs(config["extracted_path"])

    logging.info('Diretórios definidos:')
    logging.info(f'  - Saída de arquivos ZIP: {config["output_path"]}')
    logging.info(f'  - Extração de arquivos CSV: {config["extracted_path"]}')
    logging.info(f'URL dos dados: {config["data_url"]}')

    return config, db_name

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
    logging.info("--- INICIANDO ETAPA DE DOWNLOAD ---")

    try:
        files_to_download = get_zip_files_from_url(data_url)
    except (urllib.error.URLError, SystemExit):
        logging.warning("Não foi possível encontrar arquivos .zip na URL base, tentando encontrar subdiretório mais recente...")
        try:
            latest_data_url = get_latest_data_url(data_url)
            files_to_download = get_zip_files_from_url(latest_data_url)
            data_url = latest_data_url
        except (urllib.error.URLError, SystemExit):
            sys.exit(1) # Erro já foi logado pelas funções filhas

    logging.info('Arquivos que serão baixados:')
    for i, f in enumerate(files_to_download, 1):
        logging.info(f'{i} - {f}')

    for file_name in files_to_download:
        url = urllib.parse.urljoin(data_url, file_name)
        local_file_path = os.path.join(output_path, file_name)

        logging.info(f'Baixando arquivo: {file_name}')
        if not os.path.isfile(local_file_path):
            wget.download(url, out=output_path, bar=bar_progress)
        else:
            logging.info("Arquivo já existe localmente. Pulando download.")

def extract_zip_files(output_path, extracted_path):
    """
    Extrai todos os arquivos .zip da pasta de output para a pasta de extração.
    """
    logging.info("--- INICIANDO ETAPA DE EXTRAÇÃO ---")
    zip_files = [f for f in os.listdir(output_path) if f.endswith('.zip')]

    for i, file_name in enumerate(zip_files, 1):
        logging.info(f'Descompactando arquivo: {i}/{len(zip_files)} - {file_name}')
        full_path = os.path.join(output_path, file_name)
        try:
            with zipfile.ZipFile(full_path, 'r') as zip_ref:
                zip_ref.extractall(extracted_path)
        except zipfile.BadZipFile:
            logging.warning(f"O arquivo {file_name} não é um ZIP válido ou está corrompido. Ignorando.")
        except Exception as e:
            logging.warning(f"Erro inesperado ao descompactar {file_name}: {e}. Ignorando.")

def get_latest_data_url(base_url):
    """Encontra o diretório de dados mais recente na URL base."""
    logging.info(f"Buscando diretórios em: {base_url}")
    response = urlopen_with_retry(base_url)
    page = bs.BeautifulSoup(response.read(), 'lxml')
    date_pattern = re.compile(r'^\d{4}-\d{2}/$')
    dir_links = [a['href'] for a in page.find_all('a') if date_pattern.match(a['href'])]

    if not dir_links:
        logging.error("Nenhum diretório de dados (AAAA-MM/) encontrado na URL.")
        sys.exit(1)

    latest_dir = sorted(dir_links)[-1]
    latest_data_url = urllib.parse.urljoin(base_url, latest_dir)
    logging.info(f"Diretório de dados mais recente: {latest_data_url}")
    return latest_data_url

def get_zip_files_from_url(data_url):
    """Lista todos os arquivos .zip de uma URL."""
    logging.info(f"Buscando arquivos .zip em: {data_url}")
    response = urlopen_with_retry(data_url)
    page = bs.BeautifulSoup(response.read(), 'lxml')
    zip_files = [a['href'] for a in page.find_all('a') if a['href'].endswith('.zip')]

    if not zip_files:
        logging.error("Nenhum arquivo .zip encontrado na URL de dados.")
        sys.exit(1)
    return zip_files

def urlopen_with_retry(url, max_retries=3, delay_seconds=10):
    """Tenta abrir uma URL com retentativas em caso de falha."""
    for attempt in range(max_retries):
        try:
            return urllib.request.urlopen(url, timeout=60)
        except urllib.error.URLError as e:
            logging.warning(f"Falha ao acessar {url}. Erro: {e}")
            if attempt < max_retries - 1:
                logging.info(f"Aguardando {delay_seconds}s para nova tentativa...")
                time.sleep(delay_seconds)
            else:
                logging.error(f"Todas as tentativas de conexão com {url} falharam.")
                raise

def bar_progress(current, total, width=80):
    """Barra de progresso para o wget."""
    progress_message = f"Baixando: {current / total * 100:.1f}% [{current} / {total}] bytes"
    sys.stdout.write("\r" + progress_message)
    sys.stdout.flush()

# =============================================================================
# FUNÇÕES DE BANCO DE DADOS
# =============================================================================

def get_db_engine(config, db_name=None):
    """
    Cria e retorna um engine do SQLAlchemy para o SQL Server.
    Se 'db_name' é None, usa o banco de dados padrão do servidor (geralmente 'master').
    """
    # Se db_name não for fornecido, não especifica um banco de dados na URL,
    # conectando-se ao padrão do servidor (master).
    connection_url = URL.create(
        "mssql+pyodbc",
        username=config["db_user"],
        password=config["db_password"],
        host=config["db_server"],
        database=db_name,
        query={"driver": config["db_driver"]},
    )
    try:
        engine = create_engine(connection_url)
        # Apenas para teste de conexão, não deixa a conexão aberta.
        with engine.connect() as connection:
            db_context = db_name if db_name else 'master'
            logging.info(f"Conexão com o servidor SQL '{config['db_server']}' (banco: {db_context}) bem-sucedida!")
        return engine
    except Exception as e:
        if 'Login failed' in str(e):
            logging.error("Falha de logon. Verifique se o usuário e a senha no seu arquivo .env estão corretos.")
        logging.error(f"Falha ao criar engine de conexão com o SQL Server. Erro: {e}")
        sys.exit(1)

def prepare_database(master_engine, db_name):
    """
    Garante que o banco de dados de destino exista e esteja limpo.
    Usa um engine conectado ao 'master' para realizar as operações de DROP e CREATE.
    """
    logging.info(f"Preparando o banco de dados '{db_name}'...")
    with master_engine.connect() as connection:
        connection = connection.execution_options(isolation_level="AUTOCOMMIT")
        try:
            logging.info(f"Removendo o banco de dados '{db_name}' se ele existir...")
            connection.execute(text(f"DROP DATABASE IF EXISTS [{db_name}]"))
            logging.info(f"Criando o banco de dados '{db_name}'...")
            connection.execute(text(f"CREATE DATABASE [{db_name}]"))
            logging.info(f"Banco de dados '{db_name}' criado com sucesso.")
        except Exception as e:
            logging.error(f"Falha ao preparar o banco de dados '{db_name}'. Erro: {e}")
            logging.error("Verifique as permissões do usuário no servidor SQL.")
            sys.exit(1)

def setup_database_tables(engine):
    """
    Cria ou recria todas as tabelas necessárias no banco de dados usando o engine do SQLAlchemy.
    Lê os arquivos .sql do diretório 'sql/ddl'.
    """
    logging.info("--- CONFIGURANDO TABELAS NO BANCO DE DADOS ---")

    script_dir = pathlib.Path(__file__).parent.parent.resolve()
    ddl_dir = os.path.join(script_dir, 'sql', 'ddl')

    if not os.path.isdir(ddl_dir):
        logging.error(f"Diretório de DDL '{ddl_dir}' não encontrado.")
        sys.exit(1)

    ddl_files = [f for f in os.listdir(ddl_dir) if f.endswith('.sql')]

    with engine.connect() as connection:
        for ddl_file in sorted(ddl_files):
            table_name = os.path.splitext(ddl_file)[0]
            logging.info(f"  - Recriando tabela '{table_name}'...")

            with open(os.path.join(ddl_dir, ddl_file), 'r', encoding='utf-8') as f:
                ddl_content = f.read()

            connection.execute(text(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name};"))
            connection.execute(text(ddl_content))

        connection.commit()
    logging.info("Tabelas configuradas com sucesso.")

def create_database_indexes(engine):
    """Cria índices nas tabelas para otimizar as consultas."""
    logging.info("--- CRIANDO ÍNDICES NO BANCO DE DADOS ---")
    with engine.connect() as connection:
        try:
            connection.execute(text("CREATE INDEX idx_empresa_cnpj ON empresa(cnpj_basico);"))
            connection.execute(text("CREATE INDEX idx_estabelecimento_cnpj ON estabelecimento(cnpj_basico);"))
            connection.execute(text("CREATE INDEX idx_socios_cnpj ON socios(cnpj_basico);"))
            connection.execute(text("CREATE INDEX idx_simples_cnpj ON simples(cnpj_basico);"))
            connection.commit()
            logging.info("Índices criados com sucesso para a coluna `cnpj_basico`.")
        except Exception as e:
            logging.warning(f"Não foi possível criar os índices. Eles podem já existir. Erro: {e}")

# =============================================================================
# FUNÇÕES DE PROCESSAMENTO E CARGA DE DADOS
# =============================================================================

def process_and_load_data(engine, extracted_path):
    """
    Orquestra o processo de limpeza e carga de todos os arquivos CSV no banco de dados.
    """
    logging.info("--- INICIANDO ETAPA DE PROCESSAMENTO E CARGA DE DADOS ---")

    file_mappings = classify_files(extracted_path)
    schemas = get_table_schemas()

    for table_name, files in file_mappings.items():
        if files:
            process_table_files(engine, table_name, files, schemas[table_name], extracted_path)

def process_table_files(engine, table_name, files, schema, extracted_path):
    """Processa e carrega todos os arquivos de um tipo específico de tabela."""
    insert_start = time.time()
    logging.info(f"Processando tabela: {table_name.upper()}")

    total_rows_inserted = 0
    for file_name in files:
        logging.info(f'  Trabalhando no arquivo: {file_name}...')
        file_path = os.path.join(extracted_path, file_name)

        try:
            reader = pd.read_csv(
                file_path,
                sep=';',
                header=None,
                names=schema['cols'],
                dtype=schema['dtype'],
                encoding='latin-1',
                quotechar='"',
                escapechar='\\',
                chunksize=100_000,
                on_bad_lines='skip' # Use 'skip' for compatibility with older pandas versions
            )

            for i, chunk in enumerate(reader):
                bulk_insert_to_sql(engine, chunk, table_name)
                total_rows_inserted += len(chunk)
                # O \r foi removido para um log mais limpo. A verbosidade excessiva foi removida.
                # logging.info(f'    Chunk {i+1} do arquivo {file_name} inserido com sucesso.')

            logging.info(f'  Arquivo {file_name} finalizado.')
            gc.collect()

        except Exception as e:
            logging.error(f"Falha ao processar o arquivo {file_name}. Erro: {e}")
            logging.warning(f"O arquivo {file_name} será ignorado.")
            continue

    tempo_insert = round(time.time() - insert_start)
    logging.info(f"Tabela {table_name.upper()} finalizada! {total_rows_inserted} linhas inseridas em {tempo_insert}s.")

def bulk_insert_to_sql(engine, df, table_name):
    """Insere um DataFrame em uma tabela do SQL Server usando to_sql e um engine SQLAlchemy."""
    try:
        # method=None é mais lento mas é a opção mais robusta contra erros de limite de parâmetros
        df.to_sql(table_name, con=engine, if_exists='append', index=False, chunksize=10_000, method=None)
    except Exception as error:
        logging.error(f"Erro ao inserir dados na tabela {table_name}: {error}")
        # Decide-se não parar o processo inteiro, mas registrar o erro de inserção do chunk.

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
        logging.warning("Os seguintes arquivos não foram classificados e serão ignorados:")
        for f in unclassified_files:
            logging.warning(f"  - {f}")

    return file_mappings

def get_table_schemas():
    """
    Retorna um dicionário com os schemas (colunas e dtypes) para cada tabela.
    AVISO: Estes schemas são baseados na análise do layout anterior. Se o ETL falhar,
    verifique o documento 'NOVOLAYOUTDOSDADOSABERTOSDOCNPJ.pdf' para confirmar se as
    colunas e a ordem delas não foram alteradas pela Receita Federal.
    """
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
    setup_logging()
    start_time = time.time()

    logging.info(">>> INICIANDO PROCESSO DE ETL DE DADOS DA RECEITA FEDERAL <<<")

    # 1. Carregar Configurações
    config, db_name = load_environment_variables()

    # 2. Download e Extração
    download_data_files(config['data_url'], config['output_path'])
    extract_zip_files(config['output_path'], config['extracted_path'])

    # 3. Conexão e Configuração do Banco de Dados
    logging.info("Iniciando preparação do banco de dados...")
    master_engine = get_db_engine(config, db_name='master')
    prepare_database(master_engine, db_name)
    master_engine.dispose() # Descarta o engine do master
    logging.info("Preparação do banco de dados finalizada.")

    # Cria um novo engine conectado diretamente ao banco de dados de destino
    logging.info(f"Criando nova conexão para o banco de dados '{db_name}'...")
    target_engine = get_db_engine(config, db_name=db_name)

    try:
        setup_database_tables(target_engine)

        # 4. Processamento e Carga dos Dados
        process_and_load_data(target_engine, config['extracted_path'])

        # 5. Otimização do Banco (Índices)
        create_database_indexes(target_engine)
    finally:
        # Garante que a conexão final seja fechada
        logging.info("Fechando conexão com o banco de dados de destino.")
        target_engine.dispose()

    total_time = round(time.time() - start_time)
    logging.info(f"--- PROCESSO 100% FINALIZADO EM {total_time} SEGUNDOS! ---")
    logging.info("Você já pode usar seus dados no SQL Server.")
    logging.info("Contribua com esse projeto em: https://github.com/aphonsoar/Receita_Federal_do_Brasil_-_Dados_Publicos_CNPJ")

if __name__ == '__main__':
    main()
