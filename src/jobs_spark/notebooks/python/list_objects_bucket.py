import pandas as pd
import io
from minio import Minio
from configs import configs

# Initialize MinIO client
client = configs.credential_minio

# Listar objetos no bucket
objects = client.list_objects('landing-zone', recursive=True)
for obj in objects:
    print(obj.object_name)
