import pyspark
from pyspark.sql import SparkSession
import logging
from datetime import datetime  

from pyspark.sql.functions import lit  

from configs import configs
from functions import functions as F


def process_table(spark, query_input, output_path):
    try:
        df_input_data = spark.sql(query_input)
        df_with_update_date = df_input_data.withColumn("last_update", lit(datetime.now()))
        df_with_update_date.write.format("delta").mode("overwrite").partitionBy('month_key').save(output_path)
        logging.info(f"Query '{query_input}' successfully processed and saved to {output_path}")
    except Exception as e:
        logging.error(f"Error processing query '{query_input}': {str(e)}")

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Process Full Bronze to Silver") \
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

    input_prefix_layer_name = configs.prefix_layer_name['1']  # silver layer
    input_path = configs.lake_path['bronze']

    output_prefix_layer_name = configs.prefix_layer_name['2']  # gold layer
    output_path = configs.lake_path['silver']

    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    logging.info("Starting Process to Silver...")

    try:
        for table_name, query_input in configs.tables_queries_silver.items():  
            table_name = F.convert_table_name(table_name)

            query_input = F.get_query(table_name, input_path, input_prefix_layer_name, configs.tables_queries_silver)
    
            storage_output = f"{output_path}{output_prefix_layer_name}{table_name}"
            
            process_table(spark, query_input, storage_output)
        
        logging.info("Process to Silver completed!")
    except Exception as e:
        logging.error(f"Error processing table: {str(e)}")
