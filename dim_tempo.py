from google.cloud import storage
import pandas as pd
from datetime import datetime

# Função para mapear os nomes dos dias da semana em português
def obter_nome_dia_semana(data):
    dias_semana = ['segunda-feira', 'terça-feira', 'quarta-feira', 'quinta-feira', 'sexta-feira', 'sábado', 'domingo']
    return dias_semana[data.weekday()]

# Função para mapear os nomes dos meses em português
def obter_nome_mes(data):
    meses = ['janeiro', 'fevereiro', 'março', 'abril', 'maio', 'junho', 'julho', 'agosto', 'setembro', 'outubro', 'novembro', 'dezembro']
    return meses[data.month - 1]

# Função para definir o trimestre com base no número do mês
def definir_trimestre(mes):
    trimestres = {1: '1º trimestre', 2: '1º trimestre', 3: '1º trimestre',
                  4: '2º trimestre', 5: '2º trimestre', 6: '2º trimestre',
                  7: '3º trimestre', 8: '3º trimestre', 9: '3º trimestre',
                  10: '4º trimestre', 11: '4º trimestre', 12: '4º trimestre'}
    return trimestres[mes]

# Criação do DataFrame da dimensão de tempo
start_date = '01/01/2020'
end_date = '31/12/2022'

# Especifica o formato das datas para análise consistente
date_format = "%d/%m/%Y"

# Cria o range de datas com o formato especificado
periodos = pd.date_range(start=pd.to_datetime(start_date, format=date_format),
                         end=pd.to_datetime(end_date, format=date_format),
                         freq='D')

# Criação do DataFrame da dimensão de tempo
df_dim_tempo = pd.DataFrame({'data': periodos})

# Adiciona as informações de período ao DataFrame
df_dim_tempo['id_tempo'] = df_dim_tempo.reset_index().index + 1
df_dim_tempo['ano'] = df_dim_tempo['data'].dt.year
df_dim_tempo['mes'] = df_dim_tempo['data'].dt.month
df_dim_tempo['nome_mes'] = df_dim_tempo['data'].apply(obter_nome_mes)
# Cria a coluna "trimestre" com base no número do mês
df_dim_tempo['trimestre'] = df_dim_tempo['mes'].apply(definir_trimestre)
df_dim_tempo['ano_mes'] = df_dim_tempo['nome_mes'] + ' de ' + df_dim_tempo['data'].dt.strftime('%Y')
df_dim_tempo['semana_ano'] = df_dim_tempo['data'].dt.isocalendar().week
df_dim_tempo['ano_semana_iso'] = df_dim_tempo['data'].dt.strftime('%d de ') + df_dim_tempo['nome_mes'] + ' de ' + df_dim_tempo['data'].dt.strftime('%Y') + ' a ' + df_dim_tempo['data'].dt.strftime('%d de ') + df_dim_tempo['nome_mes'] + ' de ' + df_dim_tempo['data'].dt.strftime('%Y') + ' (semana ' + df_dim_tempo['semana_ano'].astype(str) + ')'
#df_dim_tempo['data_hora'] = df_dim_tempo['data'].dt.strftime('%d de %B de %Y, %H:%M:%S')
df_dim_tempo['trimestre'] = df_dim_tempo['trimestre'].astype(str)
#df_dim_tempo['mes'] = df_dim_tempo['data'].dt.strftime('%B')
df_dim_tempo['semana_iso'] = 'Semana ' + df_dim_tempo['semana_ano'].astype(str)
df_dim_tempo['mes_dia'] = df_dim_tempo['data'].dt.strftime('%d de ') + df_dim_tempo['nome_mes']
df_dim_tempo['dia_semana'] = df_dim_tempo['data'].apply
df_dim_tempo['dia_semana'] = df_dim_tempo['data'].apply(obter_nome_dia_semana)
df_dim_tempo['dia_do_mes'] = df_dim_tempo['data'].dt.day
df_dim_tempo['id_ano_mes'] = df_dim_tempo['ano'].astype(str) + df_dim_tempo['mes'].astype(str)

df_dim_tempo.to_csv('dim_tempo.csv', index=False)

print(df_dim_tempo)

# Define o caminho do arquivo JSON de credenciais
credentials_path = 'C:/Users/BeatrizAndrade/case-builders-1ff59ff8f179.json'

# Criar o cliente do Cloud Storage
storage_client = storage.Client.from_service_account_json(credentials_path)

# Salvar o DataFrame como arquivo CSV no bucket do Google Cloud Storage
bucket_name = 'bucket_case-builders'
file_name = 'dim_tempo.csv'
bucket = storage_client.get_bucket(bucket_name)
blob = bucket.blob(file_name)
blob.upload_from_string(df_dim_tempo.to_csv(index=False), 'text/csv')

print("Tabela de dimensão de tempo salva com sucesso!")