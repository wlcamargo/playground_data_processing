import pyspark
from pyspark.sql import SparkSession
import logging
from datetime import datetime  

from pyspark.sql.functions import lit  

from configs import configs
from functions import functions as F


def process_table(spark, query_input, output_table_path):
    try:
        df_input_data = spark.sql(query_input)
        df_with_update_date = df_input_data.withColumn("last_update", lit(datetime.now()))
        df_with_update_date.write.format("delta").mode("overwrite").partitionBy('month_key').save(output_table_path)
        logging.info(f"Query '{query_input}' successfully processed and saved to {output_table_path}")
    except Exception as e:
        logging.error(f"Error processing query '{query_input}': {str(e)}")

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Refinement Full Silver to Gold") \
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

 
    input_prefix_layer_name = configs.prefix_layer_name['2']  # silver layer
    input_path = configs.lake_path['silver']
    output_prefix_layer_name = configs.prefix_layer_name['3']  # gold layer
    output_path = configs.lake_path['gold']

  
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


    logging.info("Starting refinement to Gold...")


    try:
        for table_name, query_input in configs.tables_gold.items(): 
            table_name = F.convert_table_name(table_name)

            query_input = F.get_query(table_name, input_path, input_prefix_layer_name, configs.tables_gold)

            storage_output= f"{output_path}{output_prefix_layer_name}{table_name}"

            process_table(spark, query_input, storage_output)  
        
        logging.info("Refinement to Gold completed!")
    except Exception as e:
        logging.error(f"Error processing table: {str(e)}")
