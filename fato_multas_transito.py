import pymongo
from pymongo import MongoClient
import pandas as pd
from google.cloud import storage
from google.oauth2 import service_account

# URL de conexão com o MongoDB
mongo_url = "mongodb+srv://teste_dados_leitura:o7c4Cc8NDeXYbAMH@mongodbtestebuilders.vuzqjs5.mongodb.net/?retryWrites=true&w=majority"

# Estabeleça a conexão
client = MongoClient(mongo_url)

# Selecionar o banco de dados
db = client.teste_dados

# Obter a lista de coleções
collection_names = db.list_collection_names()

# Selecionar uma coleção (por exemplo, a primeira coleção)
if collection_names:
    collection = db[collection_names[0]]
else:
    # Tratar o caso em que não há coleções disponíveis
    collection = None

# Ler apenas os documentos da coleção
data = collection.find()

# Transformar em um DataFrame
df = pd.DataFrame(data)

# DataFrame com os dados das multas de trânsito
df_multas_transito = df

# Mapear os nomes dos meses para o formato numérico
mapa_meses = {
    'JANEIRO': 1,
    'FEVEREIRO': 2,
    'MARÇO': 3,
    'ABRIL': 4,
    'MAIO': 5,
    'JUNHO': 6,
    'JULHO': 7,
    'AGOSTO': 8,
    'SETEMBRO': 9,
    'OUTUBRO': 10,
    'NOVEMBRO': 11,
    'DEZEMBRO': 12
}

mapa_descricao_multas = {
  'Art. 209 Deixar de adentrar as áreas destinadas à pesagem de veículos': 1,
  'Realizar transporte permissionado de passageiros, sem a emissão de bilhete.': 2,
  'Transitar com o veículo com excesso de peso - PBT/PBTC':3,
  'Transitar com o veículo com excesso de peso - PBT/PBTC e Por Eixo':4,
  'Transitar com o veículo com excesso de peso - Por Eixo':5,
  'Transitar com o veículo excedendo a CMT acima de 1000 kg':6,
  'Transitar com o veículo excedendo a CMT em até 600 kg':7,
  'Transitar com o veículo excedendo a CMT entre 601 e 1000 kg':8
}

mapa_escopo_multas = {
                        
  'CMT - Capacidade Máxima de Tração': 1,
  'Evasão de Balança': 2,
  'Excesso de Peso':3
}

# Padronização dos valores da coluna 'mes' para maiúsculas e remoção de espaços extras
df_multas_transito['mes'] = df_multas_transito['mes'].astype(str).str.upper().str.strip()

# Aplicação do mapeamento à coluna 'mes' do DataFrame
df_multas_transito['mes'] = df_multas_transito['mes'].map(mapa_meses)

# Extrair o mês e o ano da coluna 'mes' e 'ano'
df_multas_transito['mes'] = pd.to_numeric(df_multas_transito['mes'], errors='coerce')
df_multas_transito['ano'] = pd.to_numeric(df_multas_transito['ano'], errors='coerce')

df_multas_transito['quantidade_autos'] = df_multas_transito['quantidade_autos'].astype(int)

# Agrupar os dados por mês/ano e calcular a quantidade total de multas de trânsito
df_multas_agregado = df_multas_transito.groupby(['mes', 'ano']).agg({'quantidade_autos': 'sum'}).reset_index()

# Renomear a coluna de quantidade de multas para o nome desejado na tabela fato
df_multas_agregado = df_multas_agregado.rename(columns={'quantidade_autos': 'quantidade_multas_emitidas'})

# Cria a coluna 'mes_ano' combinando as colunas 'mes' e 'ano'
df_multas_agregado['mes_ano'] = df_multas_agregado['mes'].astype(str) + df_multas_agregado['ano'].astype(str)

df_multas_agregado = df_multas_agregado.sort_values(['ano','mes']) # Ordenar o DataFrame final pelas colunas "mes e "ano"
df_multas_agregado ['comparativa_periodo_anterior'] = df_multas_agregado ['quantidade_multas_emitidas'] - df_multas_agregado ['quantidade_multas_emitidas'].shift(1) # Calcula a diferença de multas emitidas entre o mês atual e o mês anterior
df_multas_agregado ['comparativa_periodo_anterior'].fillna(df_multas_agregado ['quantidade_multas_emitidas'], inplace=True) # Substitui o valor nulo na primeira linha pelo valor do mês atual
df_multas_agregado['id_descricao_multa'] = df_multas_transito['descricao_infracao'].map(mapa_descricao_multas) #substituindo a descricao da infraçao pelo ID
df_multas_agregado['id_escopo_multa'] = df_multas_transito['escopo_autuacao'].map(mapa_escopo_multas) #substituindo o escopo da infraçao pelo ID

# Exibe o resultado
#print(df)

print(df_multas_agregado)

# Salvar o DataFrame como um arquivo CSV em um bucket do Google Cloud Storage
bucket_name = 'bucket_case-builders'
file_name = 'fato_multas_transito.csv'
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


save_dataframe_to_gcs(df_multas_agregado, bucket_name, file_name, project_id, credentials_path)

print(f"O DataFrame foi salvo como {file_name} no Google Cloud Storage.")