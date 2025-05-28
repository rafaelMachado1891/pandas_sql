import pandas as pd

caminho = pd.read_csv("../data/relatorio_estoque_completo.csv", sep=";")

df = pd.DataFrame(caminho)

colunas_selecionadas = ['Codigo', 'Descricao', 'Data_Baixa', 'MES', 'ANO', 'TOTAL_MOVIMENTO']

df = df[colunas_selecionadas]

colunas_renomeadas = {
    'Codigo': 'codigo',
    'Descricao': 'descricao',
    'Data_Baixa': 'data_baixa',
    'MES': 'mes',
    'ANO':'ano',
    'TOTAL_MOVIMENTO':'total_movimento'
}

df.rename(columns=colunas_renomeadas, inplace=True)

tipo_dados = {
    'codigo': str,
    'descricao': str,
    'data_baixa': object,
    'mes': int,
    'ano': int,
    'total_movimento': int
}

df['grupo'] = "susepe"

agregacao = df.groupby["codigo", "ano", "mes", "grupo"]
print(df.head())