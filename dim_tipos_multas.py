import pymongo
from pymongo import MongoClient
import pandas as pd
from google.cloud import storage
from google.oauth2 import service_account

# Estabelecer conexão com o banco de dados
mongo_url = "mongodb+srv://teste_dados_leitura:o7c4Cc8NDeXYbAMH@mongodbtestebuilders.vuzqjs5.mongodb.net/?retryWrites=true&w=majority"
client = MongoClient(mongo_url)
db = client.teste_dados

# Acessar a coleção "multas"
collection_multas = db['multas']

# Obter os dados únicos da coleção "multas"
tipos_multa = collection_multas.distinct('descricao_infracao')

# Criar DataFrame da tabela de dimensão "dim_multa"
df_dim_multa = pd.DataFrame({'descricao_multa': tipos_multa})

# Criar DataFrame da tabela de dimensão "dim_multa" com ID
df_dim_multa = pd.DataFrame({'id_tipo_multa': range(1, len(tipos_multa) + 1),
                             'descricao_multa': tipos_multa})

df_dim_multa = df_dim_multa.rename(columns={'descricao_multa': 'descricao_tipo_multa'})

# Exibir o DataFrame da tabela de dimensão
#print(df_dim_multa)

# Salvar o DataFrame como um arquivo CSV em um bucket do Google Cloud Storage
bucket_name = 'bucket_case-builders'
file_name = 'dim_tipos_multas.csv'
project_id = '664810481607'
credentials_path = 'C:/Users/BeatrizAndrade/builderscase/case-builders-1ff59ff8f179.json'

def save_dataframe_to_gcs(dataframe, bucket_name, file_name, project_id, credentials_path):
    # Carrega as credenciais
    credentials = service_account.Credentials.from_service_account_file(credentials_path)

    # Cria o cliente do Google Cloud Storage com as credenciais
    client = storage.Client(project=project_id, credentials=credentials)

    # Obtém o bucket
    bucket = client.get_bucket(bucket_name)

    # Salva o DataFrame como um arquivo CSV no bucket
    blob = bucket.blob(file_name)
    blob.upload_from_string(dataframe.to_csv(index=False), content_type='text/csv')

save_dataframe_to_gcs(df_dim_multa, bucket_name, file_name, project_id, credentials_path)

print(f"O DataFrame foi salvo como {file_name} no Google Cloud Storage.")

# Criar o mapeamento entre tipos de multa e IDs
#mapeamento_multas = dict(zip(df_dim_multa['descricao_multa'], df_dim_multa['id_descricao_multa']))