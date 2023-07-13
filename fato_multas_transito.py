from pymongo import MongoClient
import pandas as pd
from datetime import datetime
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

# Criar um dicionário de mapeamento entre descricoes de multas IDs
dim_descricao_multas = pd.read_csv('dim_descricao_multas.csv')
id_descricao_multa = dict(zip(dim_descricao_multas['descricao_multa'], dim_descricao_multas['id_descricao_multa']))

# Criar um dicionário de mapeamento entre escopos de multas IDs
dim_escopo_multas = pd.read_csv('dim_escopo_multas.csv')
id_escopo_multa = dict(zip(dim_escopo_multas['escopo_multa'], dim_escopo_multas['id_escopo_multa']))

# Criar um dicionário de mapeamento entre nomes de estados e IDs de estados
dim_estados_cidades = pd.read_csv('dim_estados_cidades.csv')
estado_id_map = dict(zip(dim_estados_cidades['uf'], dim_estados_cidades['id_estado']))

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

# ID
df_multas_agregado.reset_index(drop=True, inplace=True)
df_multas_agregado['id'] = df_multas_agregado.index + 1

# Substituir UF, escopo e descrição pelos IDs
df_multas_agregado['id_descricao_multa'] = df_multas_transito ['descricao_infracao'].map(id_descricao_multa)  #substituindo a descricao da infraçao pelo ID
df_multas_agregado['id_escopo_multa'] = df_multas_transito ['escopo_autuacao'].map(id_escopo_multa) #substituindo o escopo da infraçao pelo ID
df_multas_agregado['id_estado'] = df_multas_transito ['uf'].map(estado_id_map)


# Ranking de multas por UF
df_multas_estado = df_multas_agregado.groupby('id_estado')['quantidade_multas_emitidas'].sum().reset_index() # Calcular a quantidade total de multas por estado
df_multas_estado['ranking_estado'] = df_multas_estado['quantidade_multas_emitidas'].rank(ascending=False, method='min') # Calcular o ranking com base na quantidade de multas por estado
df_multas_agregado = df_multas_agregado.merge(df_multas_estado[['id_estado', 'ranking_estado']], on='id_estado') # Mesclar o ranking por estado de volta ao DataFrame principal

# Ranking de multas por Descrição de multa
df_descricao_multas = df_multas_agregado.groupby('id_descricao_multa')['quantidade_multas_emitidas'].sum().reset_index() # Calcular a quantidade total de multas por descricao
df_descricao_multas['ranking_descricao_multa'] = df_descricao_multas['quantidade_multas_emitidas'].rank(ascending=False, method='min') # Calcular o ranking com base na quantidade de multas por descricao
df_multas_agregado = df_multas_agregado.merge(df_descricao_multas[['id_descricao_multa', 'ranking_descricao_multa']], on='id_descricao_multa') # Mesclar o ranking por descricao de volta ao DataFrame principal

# Ranking de multas por escopo de multa
df_escopo_multas = df_multas_agregado.groupby('id_escopo_multa')['quantidade_multas_emitidas'].sum().reset_index() # Calcular a quantidade total de multas por escopo
df_escopo_multas['ranking_escopo_multa'] = df_escopo_multas['quantidade_multas_emitidas'].rank(ascending=False, method='min') # Calcular o ranking com base na quantidade de multas por escopo
df_multas_agregado = df_multas_agregado.merge(df_escopo_multas[['id_escopo_multa', 'ranking_escopo_multa']], on='id_escopo_multa') # Mesclar o ranking por escopo de volta ao DataFrame principal

# Ranking de multas de UF por descricao 
df_descricao_estado = df_multas_agregado.groupby('id_estado')['id_descricao_multa'].count().reset_index() # Calcular a quantidade total de multas por estado
df_descricao_estado['ranking_descricao_estado'] = df_descricao_estado['id_descricao_multa'].rank(ascending=False, method='min') # Calcular o ranking com base na quantidade de multas por estado
df_multas_agregado = df_multas_agregado.merge(df_descricao_estado[['id_estado', 'ranking_descricao_estado']], on='id_estado') # Mesclar o ranking por estado de volta ao DataFrame principal

print (df_descricao_estado)


# Comparativo com mês anterior
df_multas_agregado = df_multas_agregado.sort_values(['ano','mes']) # Ordenar o DataFrame final pelas colunas "mes e "ano"
df_multas_agregado['ano_mes'] = df_multas_agregado['ano'].astype(str) + df_multas_agregado['mes'].astype(str)
df_multas_agregado ['comparativa_periodo_anterior'] = df_multas_agregado ['quantidade_multas_emitidas'] - df_multas_agregado ['quantidade_multas_emitidas'].shift(1) # Calcula a diferença de multas emitidas entre o mês atual e o mês anterior
df_multas_agregado ['comparativa_periodo_anterior'].fillna(df_multas_agregado ['quantidade_multas_emitidas'], inplace=True) # Substitui o valor nulo na primeira linha pelo valor do mês atual
df_multas_agregado = df_multas_agregado.drop(['mes', 'ano'], axis=1)


def obter_nome_mes(data):
    meses = ['janeiro', 'fevereiro', 'março', 'abril', 'maio', 'junho', 'julho', 'agosto', 'setembro', 'outubro', 'novembro', 'dezembro']
    return meses[data.month - 1]

def ajustar_formato_ano_mes(ano_mes):
    mes = int('id_ano_mes'[:-2])
    ano = int('id_ano_mes'[:4])
    nome_mes = obter_nome_mes(datetime(ano, mes, 1))
    return nome_mes + ' de ' + str(ano)

df_multas_agregado['ano_mes'] = df_multas_agregado['ano_mes'].apply(ajustar_formato_ano_mes)

df_dim_tempo = pd.read_csv('dim_tempo.csv')

id_ano_mes = dict(zip(df_dim_tempo['ano_mes'], df_dim_tempo['id_ano_mes']))
df_multas_agregado['id_ano_mes'] = df_multas_agregado['ano_mes'].map(id_ano_mes)


# df_grupo_descricao_multa = df_multas_agregado.groupby('id_descricao_multa')
# estado_mais_multas_por_descricao = df_grupo_descricao_multa['quantidade_multas_emitidas'].idxmax()
# # Mapear o índice para o estado correspondente
# estado_mais_multas_por_descricao = df_multas_agregado.loc[estado_mais_multas_por_descricao, ['id_estado', 'id_descricao_multa']]



# Reorganizar as colunas do DataFrame
colunas_ordenadas = ['id','id_ano_mes', 'id_estado', 'id_descricao_multa', 'id_escopo_multa',
                     'quantidade_multas_emitidas', 'comparativa_periodo_anterior', 'ranking_estado','ranking_descricao_multa','ranking_escopo_multa','ranking_descricao_estado']

df_multas_agregado = df_multas_agregado.reindex(columns=colunas_ordenadas)

df_multas_agregado.to_csv('fato_multas.csv', index=False)


print(df_multas_agregado)

# Salvar o DataFrame como um arquivo CSV em um bucket do Google Cloud Storage
bucket_name = 'bucket_case-builders'
file_name = 'fato_multas_transito.csv'
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


save_dataframe_to_gcs(df_multas_agregado, bucket_name, file_name, project_id, credentials_path)

print(f"O DataFrame foi salvo como {file_name} no Google Cloud Storage.")