from google.cloud import storage
import pandas as pd

# Função para mapear os nomes dos dias da semana em português, porque parece que a configuração do locale "pt_BR.UTF-8" não é suportada no ambiente atual do Colab >:(
def obter_nome_dia_semana(data):
    dias_semana = ['segunda-feira', 'terça-feira', 'quarta-feira', 'quinta-feira', 'sexta-feira', 'sábado', 'domingo']
    return dias_semana[data.weekday()]

# Função para mapear os nomes dos meses em português, porque parece que a configuração do locale "pt_BR.UTF-8" não é suportada no ambiente atual do Colab >:(
def obter_nome_mes(data):
    meses = ['janeiro', 'fevereiro', 'março', 'abril', 'maio', 'junho', 'julho', 'agosto', 'setembro', 'outubro', 'novembro', 'dezembro']
    return meses[data.month - 1]

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
df_dim_tempo['id_tempo'] = df_dim_tempo.reset_index().index + 1 # coluna 'id_tempo' com base no índice sequencial
df_dim_tempo['data'] = periodos
df_dim_tempo['ano'] = df_dim_tempo['data'].dt.year
df_dim_tempo['mes'] = df_dim_tempo['data'].dt.month
df_dim_tempo['nome_mes'] = df_dim_tempo['data'].apply(obter_nome_mes)
df_dim_tempo['semana_ano'] = df_dim_tempo['data'].dt.isocalendar().week  # Adiciona a coluna de semana do ano usando o método isocalendar().week
df_dim_tempo['dia_semana'] = df_dim_tempo['data'].apply(obter_nome_dia_semana)
df_dim_tempo['trimestre'] = df_dim_tempo['data'].dt.quarter
df_dim_tempo['semestre'] = (df_dim_tempo['mes'] - 1) // 6 + 1

#df_dim_tempo = ['id_tempo','data','ano','mes','nome_mes','semana_ano','dia_semana','trimestre','semestre']

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