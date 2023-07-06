import pandas as pd
import mysql.connector
from google.cloud import storage
from google.oauth2 import service_account

# Estabelecer conexão
conn = mysql.connector.connect(
    host='34.95.170.227',
    port=3306,
    user='teste-dados-leitura',
    password='o7c4Cc8NDeXYbAMH',
    database='teste_dados',
    charset='utf8'  # Especificar a codificação correta
)

# Criar objeto de cursor
cursor = conn.cursor()

# Executar consulta
query = 'SELECT date,city_ibge_code,state, new_confirmed, new_deaths FROM DADOS_COVID WHERE city <> ""'
cursor.execute(query)

# Obter resultados
results = cursor.fetchall()

# Obter nomes das colunas
columns = [column[0] for column in cursor.description]

# Converter os resultados para UTF-8
results_utf8 = [[item.decode('utf-8') if isinstance(item, bytes) else item for item in row] for row in results]

# Criar DataFrame
df_covid = pd.DataFrame(results_utf8, columns=columns)

# Comparativo com dia anterior
df_covid = df_covid.sort_values('date') # Ordenar o DataFrame final pela coluna "date"
df_covid ['comparativa_periodo_anterior'] = df_covid ['new_deaths'] - df_covid ['new_deaths'].shift(1) # Calcula a diferença de mortes entre o dia atual e o dia anterior
df_covid ['comparativa_periodo_anterior'].fillna(df_covid ['new_deaths'], inplace=True) # Substitui o valor nulo na primeira linha pelo valor do mês atual

# Criar um dicionário de mapeamento entre nomes de estados e IDs de estados
dim_estados_cidades = pd.read_csv('dim_estados_cidades.csv')
estado_id_map = dict(zip(dim_estados_cidades['uf'], dim_estados_cidades['id_estado']))
df_covid['id_estado'] = df_covid['state'].map(estado_id_map)

df_covid.reset_index(drop=True, inplace=True)
df_covid['id'] = df_covid.index + 1


df_covid_final = df_covid.drop('state', axis=1)
df_covid_final = df_covid_final.rename(columns={'date':'data'})
df_covid_final = df_covid_final.rename(columns={'city_ibge_code': 'id_cidade'})
df_covid_final = df_covid_final.rename(columns={'new_confirmed': 'confirmados'})
df_covid_final = df_covid_final.rename(columns={'new_deaths': 'mortes'})


# Agrupar os dados por cidade e estado e calcular a quantidade total de mortes
df_agg_cidade = df_covid_final.groupby('id_cidade')['mortes'].sum().reset_index()
df_agg_estado = df_covid_final.groupby('id_estado')['mortes'].sum().reset_index()

# Ordenar os DataFrames pela quantidade total de mortes em ordem decrescente
df_agg_cidade = df_agg_cidade.sort_values('mortes', ascending=False)
df_agg_estado = df_agg_estado.sort_values('mortes', ascending=False)

# Criar colunas de ranking com base na ordem das mortes
df_agg_cidade['ranking_cidade'] = df_agg_cidade['mortes'].rank(ascending=False, method='min')
df_agg_estado['ranking_estado'] = df_agg_estado['mortes'].rank(ascending=False, method='min')

# Mesclar os DataFrames de agregação de mortes com o DataFrame original
df_covid_final = df_covid_final.merge(df_agg_cidade[['id_cidade', 'ranking_cidade']], on='id_cidade')
df_covid_final = df_covid_final.merge(df_agg_estado[['id_estado', 'ranking_estado']], on='id_estado')

colunas_ordenadas = ['id','data','id_cidade','id_estado','confirmados','mortes','comparativa_periodo_anterior','ranking_cidade','ranking_estado']
df_covid_final= df_covid_final.reindex(columns=colunas_ordenadas) # Reorganizar as colunas do DataFrame



print(df_covid_final) # Exibir DataFrame

# Fechar cursor e conexão
cursor.close()
conn.close()






# Salvar o DataFrame como um arquivo CSV em um bucket do Google Cloud Storage
bucket_name = 'bucket_case-builders'
file_name = 'fato_covid.csv'
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


save_dataframe_to_gcs(df_covid_final, bucket_name, file_name, project_id, credentials_path)

print(f"O DataFrame foi salvo como {file_name} no Google Cloud Storage.")