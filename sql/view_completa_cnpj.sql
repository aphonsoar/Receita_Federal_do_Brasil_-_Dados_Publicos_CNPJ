CREATE OR ALTER VIEW view_completa_cnpj AS
SELECT
    -- Empresa
    emp.cnpj_basico,
    emp.razao_social,
    emp.capital_social,
    emp.porte_empresa,
    emp.ente_federativo_responsavel,

    -- Natureza Jurídica
    nj.descricao AS natureza_juridica,

    -- Estabelecimento
    est.cnpj_ordem,
    est.cnpj_dv,
    CASE est.identificador_matriz_filial
        WHEN '1' THEN 'MATRIZ'
        WHEN '2' THEN 'FILIAL'
        ELSE 'OUTRO'
    END AS matriz_filial,
    est.nome_fantasia,
    est.situacao_cadastral,
    est.data_situacao_cadastral,
    est.data_inicio_atividade,
    est.correio_eletronico,

    -- Endereço
    est.tipo_logradouro,
    est.logradouro,
    est.numero,
    est.complemento,
    est.bairro,
    est.cep,
    mun.descricao AS municipio,
    est.uf,

    -- Contato
    est.ddd_1,
    est.telefone_1,
    est.ddd_2,
    est.telefone_2,

    -- CNAE
    cnae.descricao AS cnae_principal,

    -- Simples Nacional
    smp.opcao_pelo_simples,
    smp.data_opcao_simples,
    smp.data_exclusao_simples,
    smp.opcao_mei,
    smp.data_opcao_mei,
    smp.data_exclusao_mei,

    -- Sócios
    soc.nome_socio_razao_social,
    soc.cpf_cnpj_socio,
    qs.descricao as qualificacao_socio,
    soc.data_entrada_sociedade

FROM
    empresa emp
LEFT JOIN
    estabelecimento est ON emp.cnpj_basico = est.cnpj_basico
LEFT JOIN
    socios soc ON emp.cnpj_basico = soc.cnpj_basico
LEFT JOIN
    simples smp ON emp.cnpj_basico = smp.cnpj_basico
LEFT JOIN
    natju nj ON emp.natureza_juridica = nj.codigo
LEFT JOIN
    munic mun ON est.municipio = mun.codigo
LEFT JOIN
    cnae ON est.cnae_fiscal_principal = cnae.codigo
LEFT JOIN
    quals qs ON soc.qualificacao_socio = qs.codigo;
