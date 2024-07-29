import pandas as pd
import io
from minio import Minio

data = {
    'Nome': ['João', 'Maria', 'Pedro', 'Ana'],
    'Idade': [25, 30, 35, 28],
    'Cidade': ['São Paulo', 'Rio de Janeiro', 'Belo Horizonte', 'Salvador']
}
df = pd.DataFrame(data)

client = configs.credential_minio

csv_data = df.to_csv(index=False)

csv_bytes = io.BytesIO(csv_data.encode('utf-8'))

# Fazer upload do arquivo CSV para o MinIO
client.put_object('landing-zone', 'dados.csv', csv_bytes, len(csv_data))

print('dados.csv inserido com sucesso no Minio!')

