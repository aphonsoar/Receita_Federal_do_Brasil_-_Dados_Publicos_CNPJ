SELECT 'Receita Federal' as "Origem do dado da Empresa",
	   estabelecimento.nome_fantasia as "Empresa",
	   'Sócio' as "Cargo",
	   
	   socios.nome_socio_razao_social as "Nome do contato", 
	   '' as "Email"
	   
	   FROM socios

join estabelecimento on estabelecimento.cnpj_basico = socios.cnpj_basico

LIMIT 100;