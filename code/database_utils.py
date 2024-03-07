from .etl_pipeline import popular_tabela
from timy import timer

from utils import getEnv
from sqlalchemy import create_engine
from psycopg2 import connect, OperationalError

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
        user=getEnv('DB_USER')
        passw=getEnv('DB_PASSWORD')
        host=getEnv('DB_HOST')
        port=getEnv('DB_PORT')
        database=getEnv('DB_NAME')

        # Conectar:
        db_uri=f'postgresql://{user}:{passw}@{host}:{port}/{database}'
        engine = create_engine(db_uri)
        db_info=f'dbname={database} user={user} host={host} port={port} password={passw}'
        conn = connect(db_info)
        cur = conn.cursor()

        return engine, conn, cur
    
    except OperationalError as e:
        print(f"Error connecting to database: {e}")
        return None, None, None

def popular_empresa(engine, cur, conn, extracted_files_path, arquivos_empresa):
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

    popular_tabela(engine, cur, conn, 
        'EMPRESA', 'empresa', arquivos_empresa,
        empresa_columns, extracted_files_path, 
        'latin-1', empresa_transform_map
    )

def popular_estabelecimento(engine, cur, conn, extracted_files_path, arquivos_estabelecimento):
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
        engine, cur, conn, 
        'Estabelecimento', 'estabelecimento', arquivos_estabelecimento,
        estabelecimento_columns, extracted_files_path, 'latin-1'
    )

def popular_socios(engine, cur, conn, extracted_files_path, arquivos_socios):
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

    popular_tabela(
        engine, cur, conn, 'Socios', 'socios', arquivos_socios,
        socios_columns, extracted_files_path, 'latin-1'
    )

def popular_simples_nacional(engine, cur, conn, extracted_files_path, arquivos_simples):
    simples_columns=['cnpj_basico',
                    'opcao_pelo_simples',
                    'data_opcao_simples',
                    'data_exclusao_simples',
                    'opcao_mei',
                    'data_opcao_mei',
                    'data_exclusao_mei']

    popular_tabela(
        engine, cur, conn, 
        'Simples nacional', 'simples', arquivos_simples,
        simples_columns, extracted_files_path, 'latin-1'
    )

def popular_cnae(engine, cur, conn, extracted_files_path, arquivos_cnae):
    cnae_columns=['codigo', 'descricao']

    cnae = popular_tabela(engine, cur, conn, 
        'cnae', 'cnae', arquivos_cnae,
        cnae_columns, extracted_files_path, 'ANSI'
    )

def popular_situacao_atual(engine, cur, conn, extracted_files_path, arquivos_moti):
    cnae_columns=['codigo', 'descricao']

    popular_tabela(engine, cur, conn, 
        'motivos da situação atual', 'moti', arquivos_moti,
        cnae_columns, extracted_files_path, 'ANSI'
    )

def popular_municipios(engine, cur, conn, extracted_files_path, arquivos_munic):
    munic_columns=['codigo', 'descricao']

    popular_tabela(engine, cur, conn, 
        'motivos da situação atual', 'moti', arquivos_munic,
        munic_columns, extracted_files_path, 'ANSI'
    )

def popular_natureza_juridica(engine, cur, conn, extracted_files_path, arquivos_natju):
    natju_columns=['codigo', 'descricao']

    popular_tabela(engine, cur, conn, 
        'natureza jurídica', 'natju', arquivos_natju,
        natju_columns, extracted_files_path, 'ANSI'
    )

def popular_pais(engine, cur, conn, extracted_files_path, arquivos_pais):
    pais_columns=['codigo', 'descricao']

    popular_tabela(engine, cur, conn, 
        'país', 'pais', arquivos_pais,
        pais_columns, extracted_files_path, 'ANSI'
    )

def popular_quals(engine, cur, conn, extracted_files_path, arquivos_quals):
    quals_columns=['codigo', 'descricao']

    popular_tabela(engine, cur, conn, 
        'qualificação de sócios', 'quals', arquivos_quals,
        quals_columns, extracted_files_path, 'ANSI'
    )

@timer(ident='Popular banco')
def popular_banco(engine, cur, conn, extracted_files_path, arquivos):
    #######################
    ## Arquivos de EMPRESA:
    #######################
    popular_empresa(
        engine, cur, conn, extracted_files_path, arquivos['empresa']
    )

    ###################################
    ## Arquivos de Estabelecimento:
    ###################################
    popular_estabelecimento(
        engine, cur, conn, extracted_files_path, arquivos['estabelecimento']
    )

    ######################
    ## Arquivos de SOCIOS:
    ######################
    popular_socios(
        engine, cur, conn, extracted_files_path, arquivos['socios']
    )

    ################################
    ## Arquivos do SIMPLES NACIONAL:
    ################################
    popular_simples_nacional(
        engine, cur, conn, extracted_files_path, arquivos['simples']
    )

    ######################
    ## Arquivos de cnae:
    ######################
    popular_cnae(engine, cur, conn, extracted_files_path, arquivos['cnae'])

    #%%
    #########################################
    ## Arquivos de motivos da situação atual:
    #########################################
    popular_situacao_atual(engine, cur, conn, extracted_files_path, arquivos['moti'])

    ##########################
    ## Arquivos de municípios:
    ##########################
    popular_municipios(engine, cur, conn, extracted_files_path, arquivos['munic'])

    #################################
    ## Arquivos de natureza jurídica:
    #################################
    popular_natureza_juridica(engine, cur, conn, extracted_files_path, arquivos['natju'])

    ######################
    ## Arquivos de país:
    ######################
    popular_pais(engine, cur, conn, extracted_files_path, arquivos['pais'])

    #%%
    ######################################
    ## Arquivos de qualificação de sócios:
    ######################################
    popular_quals(engine, cur, conn, extracted_files_path, arquivos['quals'])

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