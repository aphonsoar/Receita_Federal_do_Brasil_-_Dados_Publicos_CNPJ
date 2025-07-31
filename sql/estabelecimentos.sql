SELECT 'Receita Federal' as "Origem do dado da Empresa",
nome_fantasia as "Nome Lead", 
'Agente Fiduciário' as "Tipo de Cliente", 
'' as "Prioridade", 
'Prospectando' as "Status", 
cnpj_basico as "CNPJ", 
situacao_cadastral as "Situação Cadastral", 
correio_eletronico 
FROM public.estabelecimento
