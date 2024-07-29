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

# Diretório de entrada (CSV)
input_directory = "s3a://landing-zone/pacientes/*.json"

# Diretório de checkpoint
checkpoint_directory = "s3a://landing-zone/pacientes/checkpoint/"


if __name__ == "__main__":
    spark = SparkSession.builder.appName("Streaming to Postgres").getOrCreate()

    jsonschema = "idpaciente INT, nome STRING, situacao STRING" 

    df = spark.readStream.json(f'{input_directory}', schema=jsonschema)

    def update_postgres(df, batchId):
        try:
            df.write.format("jdbc") \
                .option("url", "jdbc:postgresql://172.21.121.140:5435/Adventureworks") \
                .option("dbtable", "tb_pacientes") \
                .option("user", "postgres") \
                .option("password", "postgres") \
                .option("driver", "org.postgresql.Driver") \
                .mode("append") \
                .save()
        except Exception as e:
            print(f"Error during batch write: {str(e)}")

    stcal = df.writeStream.foreachBatch(update_postgres) \
        .outputMode("append") \
        .trigger(processingTime="5 seconds") \
        .option("checkpointLocation", checkpoint_directory) \
        .start()

    stcal.awaitTermination()