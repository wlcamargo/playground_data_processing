from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import random
import time

conf = SparkConf()
conf.setAppName("Generate Customer Real Time")
conf.set("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
conf.set("spark.hadoop.fs.s3a.access.key", "chapolin")
conf.set("spark.hadoop.fs.s3a.secret.key", "mudar@123")
conf.set("spark.hadoop.fs.s3a.path.style.access", True)
conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
conf.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')
conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") 
conf.set("hive.metastore.uris", "thrift://metastore:9083")

# Inicialização da sessão do Spark
spark = SparkSession.builder.config(conf=conf).enableHiveSupport().getOrCreate()


# Função para gerar dados aleatórios
def generate_random_data():
    id = random.randint(1, 1000)
    firstname = f"First_{id}"
    lastname = f"Last_{id}"
    country = random.choice(["Portugal", "USA", "Angola", "Brazil", "Argentina"])
    return (firstname, lastname, id, country)

# Esquema para o DataFrame
schema = StructType([
    StructField("firstname", StringType(), True),
    StructField("lastname", StringType(), True),
    StructField("id", IntegerType(), True),
    StructField("country", StringType(), True)
])

# Loop para criar arquivos de CSV com dados aleatórios a cada 10 segundos
while True:
    # Gera os dados aleatórios
    data = [generate_random_data() for _ in range(100)]  # Gera 100 linhas de dados
    
    # Cria DataFrame com os dados gerados
    df = spark.createDataFrame(data=data, schema=schema)
    
    # Salva o DataFrame como arquivo CSV no HDFS
    timestamp = int(time.time())
    filename = f"s3a://landing-zone/customer_real_time/customer_{timestamp}.csv"
    df.write.format("csv").mode("overwrite").option("header", "true").save(filename)
    
    # Aguarda 10 segundos antes de gerar o próximo conjunto de dados
    time.sleep(10)