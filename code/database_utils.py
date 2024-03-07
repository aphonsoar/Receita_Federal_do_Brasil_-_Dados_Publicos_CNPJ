from timy import timer
from os import getenv, path
from sqlalchemy import create_engine
from psycopg2 import connect, sql
import pandas as pd

from utils import repeat_token, delete_var, to_sql
from constants import COMPRIMENTO_CERCA

def connect_db():
    """
    Connects to a PostgreSQL database using environment variables for connection details.

    Returns:
        engine: SQLAlchemy engine object for the database connection.
        conn: Psycopg2 connection object for the database connection.
        cur: Psycopg2 cursor object for the database connection.
    """
    
    try:
        # Obter informações da variável de ambiente
        user=getenv('POSTGRES_USER')
        passw=getenv('POSTGRES_PASSWORD')
        host=getenv('POSTGRES_HOST')
        port=getenv('POSTGRES_PORT')
        database=getenv('POSTGRES_DB')

        # Conectar:
        db_uri=f'postgresql://{user}:{passw}@{host}:{port}/{database}'
        engine = create_engine(db_uri)
        db_info=f'dbname={database} user={user} host={host} port={port} password={passw}'
        
        conn = connect(db_info)

        print('Conexão com o banco de dados estabelecida!')

        return engine, conn
    
    except OperationalError as e:
        print(f"Error connecting to database: {e}")
        return None, None, None

##########################################################################
## LOAD AND TRANSFORM
##########################################################################
def inserir_dados(engine, file, table_name, columns, to_folder, 
                    encoding, cleanse_transform_map):
    print('Trabalhando no arquivo: ' + file + ' [...]')
    artefato = pd.DataFrame(columns=list(range(0, len(columns))))
    dtypes = {column: 'object' for column in columns}
    extracted_file_path = path.join(to_folder, file)

    artefato = pd.read_csv(
        filepath_or_buffer=extracted_file_path,
        sep=';',
        skiprows=0,
        header=None,
        dtype=dtypes,
        encoding=encoding,
    )

    # Tratamento do arquivo antes de inserir na base:
    artefato = artefato.reset_index()
    del artefato['index']

    # Renomear colunas
    artefato.columns = columns

    artefato = cleanse_transform_map(artefato)

    # Gravar dados no banco:
    artefato.to_sql(name=table_name, con=engine, if_exists='append', index=False)
    print('Arquivos ' + file + ' inserido com sucesso no banco de dados!')

@timer('Popular tabela')
def popular_tabela(
    engine, conn, 
    label: str, table_name: str, files: list,
    columns: list, to_folder: str, 
    encoding: str, cleanse_transform_map: lambda x: x
):
    fence=repeat_token('#', COMPRIMENTO_CERCA)
    title=f'## Arquivos de {label.upper()}:'
    header=f'{fence}\n{title}\n{fence}'
    print(header)

    # Drop table antes do insert
    with conn.cursor() as cur:
        query = sql.SQL('DROP TABLE IF EXISTS {};').format(sql.Identifier(table_name))
        cur.execute(query)
        conn.commit()

    artefato = None

    # Inserir dados
    for file in files:
        print('Trabalhando no arquivo: '+file+' [...]')
        delete_var(artefato)

        artefato = pd.DataFrame(columns=list(range(0, len(columns))))
        dtypes = { column: 'object' for column in columns }
        extracted_file_path = path.join(to_folder, file)

        artefato = pd.read_csv(
            filepath_or_buffer=extracted_file_path,
            sep=';',
            skiprows=0,
            header=None,
            dtype=dtypes,
            encoding=encoding,
        )

        # Tratamento do arquivo antes de inserir na base:
        artefato = artefato.reset_index()
        del artefato['index']

        # Renomear colunas
        artefato.columns = columns

        artefato = cleanse_transform_map(artefato)

        # Gravar dados no banco:
        to_sql(artefato, name=table_name, con=engine, if_exists='append', index=False)
        print('Arquivos ' + file + ' inserido com sucesso no banco de dados!')

        delete_var(artefato)

        print(f'Arquivos de {label} finalizados!')


def popular_empresa(engine, conn, extracted_files_path, arquivos_empresa):
    # Arquivos de empresa:
    def empresa_transform_map(artifact):
        # Replace "," por "."
        comma_to_period=lambda x: x.replace(',','.')
        artifact['capital_social'] = artifact['capital_social'].apply(comma_to_period)
        artifact['capital_social'] = artifact['capital_social'].astype(float)

        return artifact

    empresa_columns=[
        'cnpj_basico', 'razao_social', 
        'natureza_juridica', 'qualificacao_responsavel', 
        'capital_social', 'porte_empresa', 'ente_federativo_responsavel'
    ]

    popular_tabela(\
        engine, conn, 
        'EMPRESA', 'empresa', arquivos_empresa,
        empresa_columns, extracted_files_path, 
        'latin-1', empresa_transform_map
    )

def popular_estabelecimento(engine, conn, extracted_files_path, arquivos_estabelecimento):
    # Arquivos de estabelecimento:
    estabelecimento_columns=[
        'cnpj_basico',
        'cnpj_ordem',
        'cnpj_dv',
        'identificador_matriz_filial',
        'nome_fantasia',
        'situacao_cadastral',
        'data_situacao_cadastral',
        'motivo_situacao_cadastral',
        'nome_cidade_exterior',
        'pais',
        'data_inicio_atividade',
        'cnae_fiscal_principal',
        'cnae_fiscal_secundaria',
        'tipo_logradouro',
        'logradouro',
        'numero',
        'complemento',
        'bairro',
        'cep',
        'uf',
        'municipio',
        'ddd_1',
        'telefone_1',
        'ddd_2',
        'telefone_2',
        'ddd_fax',
        'fax',
        'correio_eletronico',
        'situacao_especial',
        'data_situacao_especial'
    ]

    popular_tabela(\
        engine, conn, 
        'Estabelecimento', 'estabelecimento', arquivos_estabelecimento,
        estabelecimento_columns, extracted_files_path, 'latin-1'
    )

def popular_socios(engine, conn, extracted_files_path, arquivos_socios):
    # Arquivos de socios:
    socios_columns=['cnpj_basico',
                    'identificador_socio',
                    'nome_socio_razao_social',
                    'cpf_cnpj_socio',
                    'qualificacao_socio',
                    'data_entrada_sociedade',
                    'pais',
                    'representante_legal',
                    'nome_do_representante',
                    'qualificacao_representante_legal',
                    'faixa_etaria']

    popular_tabela(\
        engine, conn, 'Socios', 'socios', arquivos_socios,
        socios_columns, extracted_files_path, 'latin-1'
    )

def popular_simples_nacional(engine, conn, extracted_files_path, arquivos_simples):
    simples_columns=['cnpj_basico',
                    'opcao_pelo_simples',
                    'data_opcao_simples',
                    'data_exclusao_simples',
                    'opcao_mei',
                    'data_opcao_mei',
                    'data_exclusao_mei']

    popular_tabela(\
        engine, conn, 
        'Simples nacional', 'simples', arquivos_simples,
        simples_columns, extracted_files_path, 'latin-1'
    )

def popular_cnae(engine, conn, extracted_files_path, arquivos_cnae):
    cnae_columns=['codigo', 'descricao']

    popular_tabela(\
        engine, conn, 
        'cnae', 'cnae', arquivos_cnae,
        cnae_columns, extracted_files_path, 'ANSI'
    )

def popular_situacao_atual(engine, conn, extracted_files_path, arquivos_moti):
    cnae_columns=['codigo', 'descricao']

    popular_tabela(\
        engine, conn, 
        'motivos da situação atual', 'moti', arquivos_moti,
        cnae_columns, extracted_files_path, 'ANSI'
    )

def popular_municipios(engine, conn, extracted_files_path, arquivos_munic):
    munic_columns=['codigo', 'descricao']

    popular_tabela(\
        engine, conn, 
        'motivos da situação atual', 'moti', arquivos_munic,
        munic_columns, extracted_files_path, 'ANSI'
    )

def popular_natureza_juridica(engine, conn, extracted_files_path, arquivos_natju):
    natju_columns=['codigo', 'descricao']

    popular_tabela(\
        engine, conn, 
        'natureza jurídica', 'natju', arquivos_natju,
        natju_columns, extracted_files_path, 'ANSI'
    )

def popular_pais(engine, conn, extracted_files_path, arquivos_pais):
    pais_columns=['codigo', 'descricao']

    popular_tabela(\
        engine, conn, 
        'país', 'pais', arquivos_pais,
        pais_columns, extracted_files_path, 'ANSI'
    )

def popular_quals(engine, conn, extracted_files_path, arquivos_quals):
    quals_columns=['codigo', 'descricao']

    popular_tabela(\
        engine, conn, 
        'qualificação de sócios', 'quals', arquivos_quals,
        quals_columns, extracted_files_path, 'ANSI'
    )

@timer(ident='Popular banco')
def popular_banco(engine, conn, extracted_files_path, arquivos):
    #######################
    ## Arquivos de EMPRESA:
    #######################
    popular_empresa(
        engine, conn, extracted_files_path, arquivos['empresa']
    )
    
    ###################################
    ## Arquivos de Estabelecimento:
    ###################################
    popular_estabelecimento(
        engine, conn, extracted_files_path, arquivos['estabelecimento']
    )

    ######################
    ## Arquivos de SOCIOS:
    ######################
    popular_socios(
        engine, conn, extracted_files_path, arquivos['socios']
    )

    ################################
    ## Arquivos do SIMPLES NACIONAL:
    ################################
    popular_simples_nacional(
        engine, conn, extracted_files_path, arquivos['simples']
    )

    ######################
    ## Arquivos de cnae:
    ######################
    popular_cnae(engine, conn, extracted_files_path, arquivos['cnae'])

    #########################################
    ## Arquivos de motivos da situação atual:
    #########################################
    popular_situacao_atual(engine, conn, extracted_files_path, arquivos['moti'])

    ##########################
    ## Arquivos de municípios:
    ##########################
    popular_municipios(engine, conn, extracted_files_path, arquivos['munic'])

    #################################
    ## Arquivos de natureza jurídica:
    #################################
    popular_natureza_juridica(engine, conn, extracted_files_path, arquivos['natju'])

    ######################
    ## Arquivos de país:
    ######################
    popular_pais(engine, conn, extracted_files_path, arquivos['pais'])

    ######################################
    ## Arquivos de qualificação de sócios:
    ######################################
    popular_quals(engine, conn, extracted_files_path, arquivos['quals'])

@timer('Criar indices do banco')
def criar_indices_banco(conn, cur):
    print("""
    #############################################
    ## Processo de carga dos arquivos finalizado!
    #############################################
    """)

    # Criar índices na base de dados:
    print("""
    #######################################
    ## Criar índices na base de dados [...]
    #######################################
    """)

    cur.execute("""
    create index empresa_cnpj on empresa(cnpj_basico);
    commit;
    create index estabelecimento_cnpj on estabelecimento(cnpj_basico);
    commit;
    create index socios_cnpj on socios(cnpj_basico);
    commit;
    create index simples_cnpj on simples(cnpj_basico);
    commit;
    """)
    
    conn.commit()
    print("""
    ############################################################
    ## Índices criados nas tabelas, para a coluna `cnpj_basico`:
    - empresa
    - estabelecimento
    - socios
    - simples
    ############################################################
    """)