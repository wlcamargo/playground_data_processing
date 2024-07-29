import pyspark
from pyspark.sql import SparkSession
import logging
from datetime import datetime  
from pyspark.sql.functions import lit 
from configs import configs
from functions import functions as F

spark = SparkSession.builder \
        .appName("ELT Full Landing to Bronze AdventureWorks") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "chapolin") \
        .config("spark.hadoop.fs.s3a.secret.key", "mudar@123") \
        .config("spark.hadoop.fs.s3a.path.style.access", True) \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("hive.metastore.uris", "thrift://metastore:9083") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


logging.info("Starting ingestions from Landing Zone to Bronze...")

table_input_name = configs.lake_path['landing_zone_adventure_works']  
output_prefix_layer_name = configs.prefix_layer_name['1']
storage_output = configs.lake_path['bronze']  

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


logging.info("Starting ingestions to bronze layer...")

for key, value in configs.tables_postgres_adventureworks.items():
    table = value
    table_name = F.convert_table_name(table)
    
    try:
        df_input_data = spark.read.format("parquet").load(f'{table_input_name}{table_name}') 
        df_with_update_date = df_input_data.withColumn("last_update", lit(datetime.now()))
        df_with_update_date.write.format("delta").mode("overwrite").partitionBy('month_key').save(f'{storage_output}{output_prefix_layer_name}{table_name}')        
        logging.info(f"Table {table_name} successfully processed and saved to HDFS: {storage_output}{output_prefix_layer_name}{table_name}")
        
    except Exception as e:
        logging.error(f"Error processing table {table}: {str(e)}")

logging.info("Ingestions to bronze layer completed!")
