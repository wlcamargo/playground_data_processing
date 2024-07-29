from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


# Configuração do Spark
conf = SparkConf()
conf.setAppName("Ingestion Real Time Customer")
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

# Esquema dos dados
schema = StructType([
    StructField("firstname", StringType(), True),
    StructField("lastname", StringType(), True),
    StructField("id", IntegerType(), True),
    StructField("country", StringType(), True)
])

# Diretório de entrada (CSV)
input_directory = "s3a://landing-zone/customer_real_time/*.csv"

# Diretório de checkpoint
checkpoint_directory = "s3a://bronze/customer_real_time/checkpoint/"

# Função para atualizar o Delta
def convert_parquet(df, batchId):
    df.write.format("delta") \
        .mode("append") \
        .save(f"s3a://bronze/customer_real_time/")

# Leitura em streaming dos dados CSV
df = spark.readStream.schema(schema).option("header", "true").csv(input_directory)

# Configuração do streaming para gravar em Delta
streaming_query = df.writeStream \
    .foreachBatch(convert_parquet) \
    .outputMode("append") \
    .trigger(processingTime="10 seconds") \
    .option("checkpointLocation", checkpoint_directory) \
    .start()

# Aguarda a conclusão do streaming
streaming_query.awaitTermination()

# Encerra a sessão Spark
spark.stop()
