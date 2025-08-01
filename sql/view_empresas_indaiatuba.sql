CREATE OR ALTER VIEW view_empresas_indaiatuba AS
SELECT
    e.cnpj_basico,
    e.razao_social,
    est.nome_fantasia,
    est.logradouro,
    est.numero,
    est.complemento,
    est.bairro,
    m.descricao AS municipio,
    est.uf,
    est.cep,
    est.ddd_1,
    est.telefone_1,
    est.ddd_2,
    est.telefone_2,
    est.correio_eletronico
FROM
    empresa e
JOIN
    estabelecimento est ON e.cnpj_basico = est.cnpj_basico
JOIN
    munic m ON est.municipio = m.codigo
WHERE
    m.descricao = 'INDAIATUBA';
