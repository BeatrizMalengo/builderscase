import pandas as pd
import mysql.connector
import ftfy
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
query = 'SELECT date,city,state, new_confirmed, new_deaths FROM DADOS_COVID'
cursor.execute(query)

# Obter resultados
results = cursor.fetchall()

# Obter nomes das colunas
columns = [column[0] for column in cursor.description]

# Converter os resultados para UTF-8
results_utf8 = [[item.decode('utf-8') if isinstance(item, bytes) else item for item in row] for row in results]

# Criar DataFrame
df = pd.DataFrame(results_utf8, columns=columns)
df['city'] = df['city'].apply(ftfy.fix_text)

# Eliminar as linhas com valores nulos na coluna 'cidade'
df_fato_covid = df.dropna(subset=['city'])
df_covid_sorted = df.sort_values(by='city')

# Exibir DataFrame
#print(df_sorted)

# Fechar cursor e conexão
cursor.close()
conn.close()

# Salvar o DataFrame como um arquivo CSV em um bucket do Google Cloud Storage
bucket_name = 'bucket_case-builders'
file_name = 'fato_covid.csv'
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


save_dataframe_to_gcs(df_covid_sorted, bucket_name, file_name, project_id, credentials_path)

print(f"O DataFrame foi salvo como {file_name} no Google Cloud Storage.")