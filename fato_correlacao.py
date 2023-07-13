import pandas as pd
from google.cloud import storage
from google.oauth2 import service_account

# Carregar os dados das multas
df_multas = pd.read_csv('fato_multas.csv')

# Carregar os dados dos casos de COVID-19
df_covid = pd.read_csv('fato_covid.csv')

# Transformar a coluna de data da tabela de COVID-19 em mês e ano
df_covid['mes'] = pd.to_datetime(df_covid['data']).dt.month
df_covid['ano'] = pd.to_datetime(df_covid['data']).dt.year
df_covid['ano_mes'] = df_covid['mes'].astype(str) + df_covid['ano'].astype(str)

# Agrupar os casos de COVID-19 por mês, ano e estado e calcular o número total de casos e mortes
df_covid_agregado = df_covid.groupby(['mes', 'ano','ano_mes', 'id_estado']).agg({'confirmados': 'sum', 'mortes': 'sum'}).reset_index()
print(df_covid_agregado)

# Renomear colunas para correspondência com a tabela fato
df_multas_agregado = df_multas.rename(columns={'id_ano_mes': 'id_ano_mes_multa', 'comparativa_periodo_anterior': 'comparativa_periodo_anterior_multa', 'id_estado': 'id_estado_multa'})

