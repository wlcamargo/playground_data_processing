import pandas as pd
from io import BytesIO
from minio import Minio
from configs import configs

# Inicializar o cliente MinIO
client = configs.credential_minio

# Nome do bucket e do objeto (arquivo)
bucket_name = 'landing-zone'
object_name = 'humanresources_employee.parquet'

# Baixar o objeto do bucket
response = client.get_object(bucket_name, object_name)

# Ler o conteúdo do objeto em um DataFrame do pandas
data = BytesIO(response.read())
df = pd.read_parquet(data)

# Fechar a resposta
response.close()
response.release_conn()

# Mostrar o DataFrame
print(df)
