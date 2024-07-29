import os
from minio import Minio
from configs import configs

# Initialize MinIO client
client = configs.credential_minio

# Local folder containing Parquet files
local_folder_path = configs.local_path

# List all files in the local folder
files = os.listdir(local_folder_path)

for file in files:
    if file.endswith('.parquet'):
        file_path = os.path.join(local_folder_path, file)
        
        # Upload Parquet file to MinIO
        with open(file_path, 'rb') as file_data:
            file_stat = os.stat(file_path)
            client.put_object('landing-zone', file, file_data, file_stat.st_size)

            print(f"Uploaded {file} to MinIO bucket 'landing-zone'")
