import requests
import pandas as pd
from google.cloud import storage
from google.oauth2 import service_account

# Fazer a requisição GET para obter os dados dos municípios
response = requests.get('https://servicodados.ibge.gov.br/api/v1/localidades/municipios')

# Verificar o código de status da resposta
if response.status_code == 200:
    # A resposta foi bem-sucedida
    municipios_data = response.json()
    
    # Criar listas para armazenar as informações das cidades
    cidades = []
    estados = []
    ids_cidades = []
    ids_estados =[]
    ufs = []
    
    # Extrair as informações relevantes dos dados dos municípios
    for municipio in municipios_data:
        nome_cidade = municipio['nome']
        codigo_cidade = municipio['id']
        codigo_estado = municipio['microrregiao']['mesorregiao']['UF']['id']
        nome_estado = municipio['microrregiao']['mesorregiao']['UF']['nome']
        sigla_estado = municipio['microrregiao']['mesorregiao']['UF']['sigla']
        
        # Adicionar as informações às listas
        cidades.append(nome_cidade)
        estados.append(nome_estado)
        ids_cidades.append(codigo_cidade)
        ids_estados.append(codigo_estado)
        ufs.append(sigla_estado)
    
    # Criar o DataFrame de dimensão
    dim_estados_cidades = pd.DataFrame({
        'id_cidade': ids_cidades,
        'cidade': cidades,
        'id_estado': ids_estados,
        'estado': estados,
        'uf': ufs
    })
    
    # Salvar o DataFrame em um arquivo CSV
    dim_estados_cidades.to_csv('dim_estados_cidades.csv', index=False)
    
    # Exibir o DataFrame
    print(dim_estados_cidades.head())
else:
    # A requisição não foi bem-sucedida
    print(f"Erro na requisição: {response.status_code}")


# Salvar o DataFrame como um arquivo CSV em um bucket do Google Cloud Storage
bucket_name = 'bucket_case-builders'
file_name = 'dim_estados_cidades.csv'
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

save_dataframe_to_gcs(dim_estados_cidades, bucket_name, file_name, project_id, credentials_path)

print(f"O DataFrame foi salvo como {file_name} no Google Cloud Storage.")
