-- Criar a base de dados "Dados_RFB"
CREATE DATABASE "Dados_RFB"
    WITH
    OWNER = postgres
    ENCODING = 'UTF8'
    CONNECTION LIMIT = -1;

COMMENT ON DATABASE "Dados_RFB"
    IS 'Base de dados para gravar os dados públicos de CNPJ da Receita Federal do Brasil';


CREATE TABLE audit (
    audi_id UUID PRIMARY KEY,                    -- Use SERIAL for auto-incrementing integer ID
    audi_filename VARCHAR(255) NOT NULL,            -- Filename without extension
    audi_file_size_bytes INTEGER NOT NULL,          -- Filename without extension
    audi_source_updated_at TIMESTAMP DEFAULT NULL,  -- Date and time of source file update 
    audi_created_at  TIMESTAMP DEFAULT NULL,        -- 
    audi_processed_at TIMESTAMP DEFAULT NULL,       -- Date and time of file processing
    audi_downloaded_at TIMESTAMP DEFAULT NULL,      -- Date and time of file download  
    audi_inserted_at TIMESTAMP DEFAULT NULL         -- Date and time of file insertion
);

