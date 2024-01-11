with

rfb_dataset as
(
  select
    es.cnpj
    ,emp.razao_social
    ,es.sigla_uf
    ,es.id_municipio
    ,mun.nome as nome_municipio
    ,es.cnae_fiscal_principal
    ,cnae.descricao as descricao_atividade_principal
    ,split(es.cnae_fiscal_secundaria) as cnae_secundario
    ,emp.data as data_info

  from `basedosdados.br_me_cnpj.empresas` emp
    left join `basedosdados.br_me_cnpj.estabelecimentos` es
      on emp.cnpj_basico = es.cnpj_basico

    left join `basedosdados.br_bd_diretorios_brasil.cnae_2_3_subclasses` cnae
      on es.cnae_fiscal_principal = cnae.cnae_2_3_subclasses

    left join `basedosdados.br_bd_diretorios_brasil.municipio` mun
      on es.id_municipio = mun.id_municipio

  where emp.data = '2023-05-18'
  and es.data = '2023-05-18'
  and ( razao_social like 'COPEL%'
  )
)

,rfb_dataset_unnest_cnae as
(
  select
    rfb.* except (cnae_secundario)
    ,cnae as cnae_secundario
    ,row_number() over(partition by rfb.cnpj) as row_number_cnpj
  from rfb_dataset as rfb
    left join rfb.cnae_secundario as cnae
)

,rfb_dataset_cnae_sec_desc as
(
  select
    t1.cnpj
    ,t1.row_number_cnpj
    ,t1.* except(cnpj, row_number_cnpj)
    ,cnae2.descricao as descricao_atividade_secundaria

  from rfb_dataset_unnest_cnae t1
    left join `basedosdados.br_bd_diretorios_brasil.cnae_2_3_subclasses` cnae2
      on t1.cnae_secundario = cnae2.cnae_2_3_subclasses
)

,rfb_dataset_cnae_sec_desc_transposed as
(
  select distinct
    t1.* except(cnae_secundario, descricao_atividade_secundaria, row_number_cnpj, data_info)
    ,case when row_number_cnpj = 1 then cnae_secundario else null end as cnae_secundario1
    ,case when row_number_cnpj = 1 then descricao_atividade_secundaria else null end as descricao_atividade_secundaria1
    ,case when row_number_cnpj = 2 then cnae_secundario else null end as cnae_secundario2
    ,case when row_number_cnpj = 2 then descricao_atividade_secundaria else null end as descricao_atividade_secundaria2
    ,case when row_number_cnpj = 3 then cnae_secundario else null end as cnae_secundario3
    ,case when row_number_cnpj = 3 then descricao_atividade_secundaria else null end as descricao_atividade_secundaria3
    ,case when row_number_cnpj = 4 then cnae_secundario else null end as cnae_secundario4
    ,case when row_number_cnpj = 4 then descricao_atividade_secundaria else null end as descricao_atividade_secundaria4
    ,case when row_number_cnpj = 5 then cnae_secundario else null end as cnae_secundario5
    ,case when row_number_cnpj = 5 then descricao_atividade_secundaria else null end as descricao_atividade_secundaria5
    ,case when row_number_cnpj = 6 then cnae_secundario else null end as cnae_secundario6
    ,case when row_number_cnpj = 6 then descricao_atividade_secundaria else null end as descricao_atividade_secundaria6
    ,case when row_number_cnpj = 7 then cnae_secundario else null end as cnae_secundario7
    ,case when row_number_cnpj = 7 then descricao_atividade_secundaria else null end as descricao_atividade_secundaria7
    ,case when row_number_cnpj = 8 then cnae_secundario else null end as cnae_secundario8
    ,case when row_number_cnpj = 8 then descricao_atividade_secundaria else null end as descricao_atividade_secundaria8
    ,case when row_number_cnpj = 9 then cnae_secundario else null end as cnae_secundario9
    ,case when row_number_cnpj = 9 then descricao_atividade_secundaria else null end as descricao_atividade_secundaria9
    ,case when row_number_cnpj = 10 then cnae_secundario else null end as cnae_secundario10
    ,case when row_number_cnpj = 10 then descricao_atividade_secundaria else null end as descricao_atividade_secundaria10
    ,data_info
  from rfb_dataset_cnae_sec_desc t1
)

select
  cnpj
  ,razao_social
  ,sigla_uf
  ,id_municipio
  ,nome_municipio
  ,cnae_fiscal_principal
  ,descricao_atividade_principal
  ,max(cnae_secundario1) as cnae_secundario_1
  ,max(descricao_atividade_secundaria1) as descricao_atividade_secundaria_1
  ,max(cnae_secundario2) as cnae_secundario_2
  ,max(descricao_atividade_secundaria2) as descricao_atividade_secundaria_2
  ,max(cnae_secundario3) as cnae_secundario_3
  ,max(descricao_atividade_secundaria3) as descricao_atividade_secundaria_3
  ,max(cnae_secundario4) as cnae_secundario_4
  ,max(descricao_atividade_secundaria4) as descricao_atividade_secundaria_4
  ,max(cnae_secundario5) as cnae_secundario_5
  ,max(descricao_atividade_secundaria5) as descricao_atividade_secundaria_5
  ,max(cnae_secundario6) as cnae_secundario_6
  ,max(descricao_atividade_secundaria6) as descricao_atividade_secundaria_6
  ,max(cnae_secundario7) as cnae_secundario_7
  ,max(descricao_atividade_secundaria7) as descricao_atividade_secundaria_7
  ,max(cnae_secundario8) as cnae_secundario_8
  ,max(descricao_atividade_secundaria8) as descricao_atividade_secundaria_8
  ,max(cnae_secundario9) as cnae_secundario_9
  ,max(descricao_atividade_secundaria9) as descricao_atividade_secundaria_9
  ,max(cnae_secundario10) as cnae_secundario_10
  ,max(descricao_atividade_secundaria10) as descricao_atividade_secundaria_10
  ,t1.data_info
from rfb_dataset_cnae_sec_desc_transposed t1
--where cnpj = '02558157040032'
group by 1,2,3,4,5,6,7,28