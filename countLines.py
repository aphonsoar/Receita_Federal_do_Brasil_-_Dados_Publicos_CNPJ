import os
import glob
import polars as pl

# Variável global para o caminho dos arquivos
CAMINHO_ARQUIVOS = r'D:\Documentos\Projetos\Dados_RFB\EXTRACTED_FILES'

# Função para formatar números de forma simples
def formatar_simples(n):
    if n >= 1_000_000:
        return f"{n / 1_000_000:.1f}M"
    elif n >= 1_000:
        return f"{n / 1_000:.0f} mil"
    else:
        return str(n)

# Padrão dos arquivos
padrao_arquivos = os.path.join(CAMINHO_ARQUIVOS, "*.D50913.ESTABELE")
arquivos = sorted(glob.glob(padrao_arquivos))

resultados = []

for arquivo in arquivos:
    with open(arquivo, 'r', encoding='utf-8', errors='ignore') as f:
        num_linhas = sum(1 for _ in f)
    resultados.append({
        "arquivo": os.path.basename(arquivo),
        "linhas": num_linhas,
        "linhas_simples": formatar_simples(num_linhas)
    })

# Calcula total
total_linhas = sum(r["linhas"] for r in resultados)
resultados.append({
    "arquivo": "TOTAL",
    "linhas": total_linhas,
    "linhas_simples": formatar_simples(total_linhas)
})

# Converte para DataFrame com Polars
df_resultado = pl.DataFrame(resultados)

print(df_resultado)