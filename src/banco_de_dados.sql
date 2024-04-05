-- Criar a base de dados "Dados_RFB"
CREATE DATABASE "Dados_RFB"
    WITH
    OWNER = postgres
    ENCODING = 'UTF8'
    CONNECTION LIMIT = -1;

COMMENT ON DATABASE "Dados_RFB"
    IS 'Base de dados para gravar os dados públicos de CNPJ da Receita Federal do Brasil';

-- Diretório físico do banco de dados:
--SHOW data_directory;