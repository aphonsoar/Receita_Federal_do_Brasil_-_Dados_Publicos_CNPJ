create index if not exists empresa_cnpj on empresa(cnpj_basico);
create index if not exists estabelecimento_cnpj on estabelecimento(cnpj_basico);
create index if not exists socios_cnpj on socios(cnpj_basico);
create index if not exists simples_cnpj on simples(cnpj_basico);