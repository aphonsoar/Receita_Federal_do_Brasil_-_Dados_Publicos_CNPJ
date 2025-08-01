CREATE TABLE estabelecimento (
    cnpj_basico VARCHAR(8), cnpj_ordem VARCHAR(4), cnpj_dv VARCHAR(2),
    identificador_matriz_filial VARCHAR(1), nome_fantasia VARCHAR(MAX), situacao_cadastral VARCHAR(2),
    data_situacao_cadastral VARCHAR(10), motivo_situacao_cadastral VARCHAR(2), nome_cidade_exterior VARCHAR(MAX),
    pais VARCHAR(3), data_inicio_atividade VARCHAR(10), cnae_fiscal_principal VARCHAR(7),
    cnae_fiscal_secundaria VARCHAR(MAX), tipo_logradouro VARCHAR(MAX), logradouro VARCHAR(MAX),
    numero VARCHAR(MAX), complemento VARCHAR(MAX), bairro VARCHAR(MAX), cep VARCHAR(8),
    uf VARCHAR(2), municipio VARCHAR(4), ddd_1 VARCHAR(4), telefone_1 VARCHAR(9),
    ddd_2 VARCHAR(4), telefone_2 VARCHAR(9), ddd_fax VARCHAR(4), fax VARCHAR(9),
    correio_eletronico VARCHAR(MAX), situacao_especial VARCHAR(MAX), data_situacao_especial VARCHAR(10)
);
