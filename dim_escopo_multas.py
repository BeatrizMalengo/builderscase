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
escopo_multa = collection_multas.distinct('escopo_autuacao')

# Criar DataFrame da tabela de dimensão "dim_multa"
dim_esc_multa = pd.DataFrame({'escopo_autuacao': escopo_multa})

# Criar DataFrame da tabela de dimensão "dim_multa" com ID
dim_esc_multa = pd.DataFrame({'id_escopo_multa': range(1, len(escopo_multa) + 1),
                             'escopo_multa': escopo_multa})

# Exibir o DataFrame da tabela de dimensão
print(dim_esc_multa)

# Salvar o DataFrame em um arquivo CSV
dim_esc_multa.to_csv('dim_escopo_multas.csv', index=False)

# Salvar o DataFrame como um arquivo CSV em um bucket do Google Cloud Storage
bucket_name = 'bucket_case-builders'
file_name = 'dim_escopo_multas.csv'
project_id = '664810481607'
credentials_path = 'C:/Users/BeatrizAndrade/case-builders-1ff59ff8f179.json'

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

save_dataframe_to_gcs(dim_esc_multa, bucket_name, file_name, project_id, credentials_path)

print(f"O DataFrame foi salvo como {file_name} no Google Cloud Storage.")