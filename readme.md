                            # 🚀 Pipeline ETL CNPJ Otimizado

Sistema ETL **ultra-otimizado** para processar dados da Receita Federal do Brasil em máquinas locais Windows 10 com 16GB RAM.

## 🎯 **Otimizações Implementadas**

### ⚡ **Performance Gains Esperados**
- **Download**: 5-10x mais rápido (paralelo vs sequencial)
- **Extração**: 3-5x mais rápido (multi-threading)
- **Carregamento**: 10-20x mais rápido (otimizações PostgreSQL + chunking inteligente)
- **Uso de RAM**: 60% mais eficiente
- **Uso de CPU**: Utilização otimizada de todos os cores

### 🧠 **Otimizações de Memória**
- Chunking adaptativo baseado na RAM disponível
- Otimização automática de tipos de dados pandas
- Garbage collection agressivo
- Streaming de dados quando possível

### 🔄 **Paralelização**
- Downloads assíncronos com controle de rate limiting
- Extração multi-threaded de arquivos ZIP
- Processamento paralelo de chunks
- Pool de conexões otimizado do banco

## 🛠️ **Configuração Inicial**

### 1. **Instalar Dependências**
```bash
pip install -r requirements_optimized.txt
```

### 2. **Configurar PostgreSQL**
```bash
# Otimizar PostgreSQL (EXECUTE UMA VEZ)
python setup_optimization.py --all

# Ou verificar apenas as configurações atuais
python setup_optimization.py --check
```

### 3. **Configurar Variáveis de Ambiente**
Crie arquivo `.env`:
```env
# Caminhos (ajuste conforme necessário)
OUTPUT_FILES_PATH=./downloads
EXTRACTED_FILES_PATH=./extracted

# Banco PostgreSQL
DB_USER=seu_usuario
DB_PASSWORD=sua_senha
DB_HOST=localhost
DB_PORT=5432
DB_NAME=cnpj_dados
```

## 🚀 **Uso do Pipeline**

### **Pipeline Completo (Recomendado)**
```bash
# Download + Extração + Carregamento otimizados
python main_optimized.py --download --extract --load --smart-organize
```

### **Etapas Individuais**
```bash
# Apenas download paralelo
python main_optimized.py --download

# Apenas extração paralela
python main_optimized.py --extract

# Apenas carregamento otimizado
python main_optimized.py --load

# Organização inteligente com análise
python main_optimized.py --smart-organize
```

### **Verificar Configurações do Sistema**
```bash
# Mostra configurações otimizadas calculadas automaticamente
python main_optimized.py --show-config
```

## 📊 **Monitoramento e Performance**

### **Durante a Execução**
O pipeline mostra automaticamente:
- Progress bars detalhados
- Uso de CPU e RAM em tempo real
- Estimativas de tempo restante
- Estatísticas de throughput

### **Métricas Esperadas** (16GB RAM, i7)
| Fase | Tempo Estimado | Throughput |
|------|----------------|------------|
| Download (paralelo) | 10-30 min | 50-150 MB/s |
| Extração (multi-thread) | 5-15 min | 200-500 MB/s |
| Carregamento | 30-90 min | 100-300k rows/s |

## 🔧 **Configurações Avançadas**

### **Personalizar Chunk Size**
```bash
# Força chunk size específico (sobrescreve automático)
python main_optimized.py --load --chunk-size 250000
```

### **Controlar Workers**
```bash
# Força número de workers (sobrescreve automático)
python main_optimized.py --download --workers 6
```

### **PostgreSQL Ultra-Otimizado**
```bash
# Aplicar todas as otimizações do PostgreSQL
python setup_optimization.py --all

# Criar índices recomendados para análises
python setup_optimization.py --indexes

# Instalar extensions úteis
python setup_optimization.py --extensions
```

## 🎛️ **Configurações Automáticas**

O sistema calcula automaticamente:

### **Para 16GB RAM:**
- **Chunk Size**: ~400k linhas (otimizado para tabelas RFB)
- **Workers I/O**: 4 (limitado por bandwidth)
- **Workers CPU**: 6 (deixa 2 cores livres)
- **Pool DB**: 10 conexões
- **Limite RAM**: ~12GB (75% da total)

### **Para Outras Configurações:**
O sistema adapta automaticamente baseado no hardware detectado.

## 🔍 **Troubleshooting**

### **Erro de Memória**
```bash
# Reduzir chunk size
python main_optimized.py --load --chunk-size 100000

# Verificar RAM disponível
python main_optimized.py --show-config
```

### **Erro de Conexão DB**
```bash
# Verificar configurações PostgreSQL
python setup_optimization.py --check

# Testar conexão
psql -h localhost -U seu_usuario -d cnpj_dados
```

### **Download Lento**
```bash
# Reduzir workers (pode ajudar com conexões instáveis)
python main_optimized.py --download --workers 2
```

## 📈 **Análises Recomendadas Pós-ETL**

### **Queries Otimizadas Incluídas**
O sistema cria automaticamente índices para:
- Busca textual por razão social (GIN + trigram)
- Análises geográficas (UF + município)
- Análises setoriais (CNAE + localização)
- Consultas por situação cadastral
- Análises do Simples Nacional

### **Exemplo de Query Otimizada**
```sql
-- Busca empresas por texto (usa índice GIN + trigram)
SELECT razao_social, cnpj_basico 
FROM empresa 
WHERE razao_social % 'PETROBRAS'  -- Busca fuzzy
ORDER BY similarity(razao_social, 'PETROBRAS') DESC;

-- Análise setorial por região (usa índices compostos)
SELECT 
  cnae_principal,
  uf,
  COUNT(*) as total_estabelecimentos
FROM estabelecimento 
WHERE situacao_cadastral = '02'  -- Ativa
GROUP BY cnae_principal, uf
ORDER BY total_estabelecimentos DESC;
```

## 🎯 **Dicas de Performance**

### **Para Máximo Desempenho:**
1. **SSD**: Use SSD para downloads e dados temporários
2. **RAM**: Feche outros programas durante o ETL
3. **PostgreSQL**: Use as configurações geradas pelo script
4. **Antivírus**: Adicione diretórios do projeto na exclusão
5. **Rede**: Use conexão estável para downloads

### **Para Desenvolvimento:**
```bash
# Processar apenas subset para testes rápidos
# (modifique downloader para filtrar arquivos específicos)
```

## 🛡️ **Recuperação de Erros**

### **Pipeline Resiliente**
- Downloads com retry automático
- Detecção de arquivos já processados
- Checkpoint automático no banco
- Logs detalhados para debugging

### **Restart Seguro**
```bash
# O pipeline detecta automaticamente o que já foi feito
python main_optimized.py --download --extract --load
# Pula downloads existentes, extração já feita, continua carregamento
```

## 📋 **Checklist Pré-Execução**

- [ ] PostgreSQL instalado e rodando
- [ ] Python 3.8+ instalado
- [ ] Dependências instaladas (`pip install -r requirements_optimized.txt`)
- [ ] PostgreSQL otimizado (`python setup_optimization.py --all`)
- [ ] Arquivo `.env` configurado
- [ ] Pelo menos 50GB livres em disco
- [ ] RAM suficiente (mínimo 8GB, recomendado 16GB+)

## 🎉 **Resultados Esperados**

Após a execução completa, você terá:
- ✅ Base completa de CNPJs da RFB
- ✅ Índices otimizados para consultas rápidas
- ✅ Extensions PostgreSQL para análise avançada
- ✅ Estatísticas atualizadas do banco
- ✅ Sistema pronto para análises de alto volume

---

## 💡 **Contribuições**

Para melhorar ainda mais a performance:
1. Implemente cache Redis para consultas frequentes
2. Adicione particionamento de tabelas por UF
3. Integre com Apache Spark para processamento distribuído
4. Adicione compression nos dados intermediários

---

**Desenvolvido com foco em performance extrema para análise de dados CNPJ em ambiente local.**